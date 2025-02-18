// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::TenancyConfig;
use odf::dataset::{DatasetIDStream, DatasetImpl, MetadataChainImpl};
use odf::storage::lfs::{NamedObjectRepositoryLocalFS, ObjectRepositoryLocalFSSha3};
use odf::storage::{
    MetadataBlockRepositoryCachingInMem,
    MetadataBlockRepositoryImpl,
    ReferenceRepositoryImpl,
};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetStorageUnitLocalFs {
    root: PathBuf,
    current_account_subject: Arc<CurrentAccountSubject>,
    tenancy_config: Arc<TenancyConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl DatasetStorageUnitLocalFs {
    pub fn new(
        root: PathBuf,
        current_account_subject: Arc<CurrentAccountSubject>,
        tenancy_config: Arc<TenancyConfig>,
    ) -> Self {
        Self {
            root,
            current_account_subject,
            tenancy_config,
        }
    }

    // TODO: Public only for testing
    pub fn get_dataset_layout(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<odf::dataset::DatasetLayout, odf::dataset::GetStoredDatasetError> {
        let dataset_path = self.get_dataset_path(dataset_id);
        if !dataset_path.exists() {
            return Err(odf::dataset::GetStoredDatasetError::NotFound(
                odf::dataset::DatasetNotFoundError {
                    dataset_ref: dataset_id.as_local_ref(),
                },
            ));
        }
        Ok(odf::dataset::DatasetLayout::new(dataset_path))
    }

    fn build_dataset(layout: odf::dataset::DatasetLayout) -> Arc<dyn odf::Dataset> {
        Arc::new(DatasetImpl::new(
            MetadataChainImpl::new(
                MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
                    ObjectRepositoryLocalFSSha3::new(layout.blocks_dir),
                )),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(layout.refs_dir)),
            ),
            ObjectRepositoryLocalFSSha3::new(layout.data_dir),
            ObjectRepositoryLocalFSSha3::new(layout.checkpoints_dir),
            NamedObjectRepositoryLocalFS::new(layout.info_dir),
            Url::from_directory_path(&layout.root_dir).unwrap(),
        ))
    }

    fn get_dataset_path(&self, dataset_id: &odf::DatasetID) -> PathBuf {
        self.root
            .join(dataset_id.as_multibase().to_stack_string().as_str())
    }

    fn canonical_dataset_alias(&self, raw_alias: &odf::DatasetAlias) -> odf::DatasetAlias {
        match self.tenancy_config.as_ref() {
            TenancyConfig::SingleTenant => raw_alias.clone(),
            TenancyConfig::MultiTenant => {
                let account_name = if raw_alias.is_multi_tenant() {
                    raw_alias.account_name.as_ref().unwrap()
                } else {
                    match self.current_account_subject.as_ref() {
                        CurrentAccountSubject::Anonymous(_) => {
                            panic!("Anonymous account misused, use multi-tenant alias");
                        }
                        CurrentAccountSubject::Logged(l) => &l.account_name,
                    }
                };

                odf::DatasetAlias::new(Some(account_name.clone()), raw_alias.dataset_name.clone())
            }
        }
    }

    async fn save_dataset_alias(
        &self,
        dataset: &dyn odf::Dataset,
        dataset_alias: &odf::DatasetAlias,
    ) -> Result<(), InternalError> {
        dataset
            .as_info_repo()
            .set("alias", dataset_alias.to_string().as_bytes())
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl odf::DatasetStorageUnit for DatasetStorageUnitLocalFs {
    async fn get_stored_dataset_by_id(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Arc<dyn odf::Dataset>, odf::dataset::GetStoredDatasetError> {
        let layout = self.get_dataset_layout(dataset_id)?;
        let dataset = Self::build_dataset(layout);
        Ok(dataset)
    }

    fn stored_dataset_ids(&self) -> DatasetIDStream<'_> {
        Box::pin(async_stream::try_stream! {
            // While creating a workspace, the directory has not yet been created
            if !self.root.exists() {
                return;
            }

            let read_dataset_dir = std::fs::read_dir(&self.root).int_err()?;

            for r_dataset_dir in read_dataset_dir {
                let dataset_dir_entry = r_dataset_dir.int_err()?;
                if let Some(s) = dataset_dir_entry.file_name().to_str() {
                    if s.starts_with('.') {
                        continue;
                    }
                }

                let dataset_id = odf::DatasetID::from_multibase_string(dataset_dir_entry.file_name().to_str().unwrap()).int_err()?;
                yield dataset_id;
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl odf::DatasetStorageUnitWriter for DatasetStorageUnitLocalFs {
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_alias, ?seed_block))]
    async fn create_dataset(
        &self,
        dataset_alias: &odf::DatasetAlias,
        seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
    ) -> Result<odf::CreateDatasetResult, odf::dataset::CreateDatasetError> {
        // Check if a dataset with the same ID can be resolved successfully
        use odf::DatasetStorageUnit;
        let maybe_existing_dataset = match self
            .get_stored_dataset_by_id(&seed_block.event.dataset_id)
            .await
        {
            Ok(existing_dataset) => Ok(Some(existing_dataset)),
            Err(odf::dataset::GetStoredDatasetError::NotFound(_)) => Ok(None),
            Err(odf::dataset::GetStoredDatasetError::Internal(e)) => {
                Err(odf::dataset::CreateDatasetError::Internal(e))
            }
        }?;

        // If so, there are 2 possibilities:
        // - Dataset was partially created before (no head yet) and was not GC'd - so we
        //   assume ownership
        // - Dataset existed before (has valid head) - we should error out with ref
        //   collision
        if let Some(existing_dataset) = maybe_existing_dataset {
            match existing_dataset
                .as_metadata_chain()
                .resolve_ref(&odf::BlockRef::Head)
                .await
            {
                // Existing head
                Ok(_) => {
                    return Err(odf::dataset::CreateDatasetError::RefCollision(
                        odf::dataset::RefCollisionError {
                            id: seed_block.event.dataset_id.clone(),
                        },
                    ));
                }

                // No head, so continue creating
                Err(odf::storage::GetRefError::NotFound(_)) => {}

                // Errors...
                Err(odf::storage::GetRefError::Access(e)) => {
                    return Err(odf::dataset::CreateDatasetError::Internal(e.int_err()))
                }
                Err(odf::storage::GetRefError::Internal(e)) => {
                    return Err(odf::dataset::CreateDatasetError::Internal(e))
                }
            }
        }

        // It's okay to create a new dataset by this point
        let dataset_handle = odf::DatasetHandle::new(
            seed_block.event.dataset_id.clone(),
            self.canonical_dataset_alias(dataset_alias),
        );

        // Create dataset
        let dataset_path = self.get_dataset_path(&dataset_handle.id);
        let layout = odf::dataset::DatasetLayout::create(&dataset_path).int_err()?;
        let dataset = Self::build_dataset(layout);

        // Set Head
        let head = match dataset
            .as_metadata_chain()
            .append(
                seed_block.into(),
                odf::dataset::AppendOpts {
                    // We are using head ref CAS to detect previous existence of a dataset
                    // as atomically as possible
                    check_ref_is: Some(None),
                    ..odf::dataset::AppendOpts::default()
                },
            )
            .await
        {
            Ok(head) => head,
            Err(err) => return Err(err.int_err().into()),
        };

        // Save alias
        // TODO: reconsuder if we need this
        self.save_dataset_alias(dataset.as_ref(), &dataset_handle.alias)
            .await?;

        tracing::info!(
            id = %dataset_handle.id,
            alias = %dataset_handle.alias,
            %head,
            "Created new dataset",
        );

        Ok(odf::CreateDatasetResult {
            dataset_handle,
            dataset,
            head,
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id, %new_name))]
    async fn rename_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        new_name: &odf::DatasetName,
    ) -> Result<(), odf::dataset::RenameDatasetError> {
        // Ensure dataset folder exists on disk
        let layout = self.get_dataset_layout(dataset_id).map_err(|e| match e {
            odf::dataset::GetStoredDatasetError::NotFound(e) => {
                odf::dataset::RenameDatasetError::NotFound(e)
            }
            odf::dataset::GetStoredDatasetError::Internal(e) => {
                odf::dataset::RenameDatasetError::Internal(e)
            }
        })?;

        // Build dataset pointing to disk location
        let dataset = DatasetStorageUnitLocalFs::build_dataset(layout);

        // Read current alias
        let current_alias = odf::dataset::read_dataset_alias(dataset.as_ref()).await?;

        // Try writing updated alias
        // TODO: reconsuder if we need this
        let new_alias =
            odf::DatasetAlias::new(current_alias.account_name.clone(), new_name.clone());
        self.save_dataset_alias(dataset.as_ref(), &new_alias)
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id))]
    async fn delete_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), odf::dataset::DeleteStoredDatasetError> {
        // Ensure dataset folder exists on disk
        let layout = self.get_dataset_layout(dataset_id)?;

        // Remove all stored files and the folder itself
        tokio::fs::remove_dir_all(layout.root_dir).await.int_err()?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
