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
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME_STR};
use kamu_core::TenancyConfig;
use odf::dataset::{DatasetImpl, DatasetLayout, MetadataChainImpl};
use odf::storage::lfs::{NamedObjectRepositoryLocalFS, ObjectRepositoryLocalFSSha3};
use odf::storage::{
    MetadataBlockRepositoryCachingInMem,
    MetadataBlockRepositoryImpl,
    ReferenceRepositoryImpl,
};
use odf::DatasetStorageUnit;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetStorageUnitLocalFs {
    root: PathBuf,
    tenancy_config: Arc<TenancyConfig>,
    current_account_subject: Arc<CurrentAccountSubject>,
    thrash_lock: tokio::sync::Mutex<()>,
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
            thrash_lock: tokio::sync::Mutex::new(()),
        }
    }

    // TODO: Used only for testing, but should be removed it in future to discourage
    // file-based access
    pub async fn get_dataset_layout(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<DatasetLayout, odf::dataset::GetDatasetError> {
        use odf::DatasetStorageUnit;
        let dataset_handle = self
            .resolve_stored_dataset_handle_by_ref(dataset_ref)
            .await?;
        Ok(DatasetLayout::new(
            self.get_dataset_path(&dataset_handle.id),
        ))
    }

    fn build_dataset(layout: DatasetLayout) -> Arc<dyn odf::Dataset> {
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

    // TODO: PERF, avoid this
    async fn resolve_dataset_by_alias(
        &self,
        alias: &odf::DatasetAlias,
    ) -> Result<odf::DatasetHandle, ResolveDatasetError> {
        let normalized_alias = self.normalize_alias(alias);

        use futures::StreamExt;
        let mut datasets = self.stored_dataset_handles();
        while let Some(hdl) = datasets.next().await {
            let hdl = hdl?;
            println!("hdl.alias={}, normalized = {}", hdl.alias, normalized_alias);
            if hdl.alias == normalized_alias {
                return Ok(hdl);
            }
        }
        Err(ResolveDatasetError::NotFound(
            odf::dataset::DatasetNotFoundError {
                dataset_ref: alias.as_local_ref(),
            },
        ))
    }

    async fn resolve_dataset_by_id(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<odf::DatasetHandle, ResolveDatasetError> {
        let dataset_path = self.get_dataset_path(dataset_id);
        if !dataset_path.exists() {
            return Err(ResolveDatasetError::NotFound(
                odf::dataset::DatasetNotFoundError {
                    dataset_ref: dataset_id.as_local_ref(),
                },
            ));
        }
        let dataset_layout = DatasetLayout::new(self.get_dataset_path(dataset_id));

        let dataset = Self::build_dataset(dataset_layout);

        let dataset_alias = match self.read_dataset_alias(dataset.as_ref()).await {
            Ok(alias) => Ok(alias),
            Err(err) => Err(err),
        }
        .int_err()?;

        Ok(odf::DatasetHandle::new(dataset_id.clone(), dataset_alias))
    }

    fn normalize_alias(&self, alias: &odf::DatasetAlias) -> odf::DatasetAlias {
        match self.tenancy_config.as_ref() {
            TenancyConfig::SingleTenant => {
                if alias.is_multi_tenant() {
                    assert!(
                        !alias.is_multi_tenant()
                            || alias.account_name.as_ref().unwrap() == DEFAULT_ACCOUNT_NAME_STR,
                        "Multi-tenant refs shouldn't have reached down to here with earlier \
                         validations"
                    );
                }
                alias.clone()
            }
            TenancyConfig::MultiTenant => {
                if alias.is_multi_tenant() {
                    alias.clone()
                } else {
                    match self.current_account_subject.as_ref() {
                        CurrentAccountSubject::Anonymous(_) => {
                            panic!("Anonymous account misused, use multi-tenant alias");
                        }
                        CurrentAccountSubject::Logged(l) => odf::DatasetAlias::new(
                            Some(l.account_name.clone()),
                            alias.dataset_name.clone(),
                        ),
                    }
                }
            }
        }
    }

    fn get_dataset_path(&self, dataset_id: &odf::DatasetID) -> PathBuf {
        self.root
            .join(dataset_id.as_multibase().to_stack_string().as_str())
    }

    async fn read_dataset_alias(
        &self,
        dataset: &dyn odf::Dataset,
    ) -> Result<odf::DatasetAlias, odf::storage::GetNamedError> {
        let bytes = dataset.as_info_repo().get("alias").await?;
        let dataset_alias_str = std::str::from_utf8(&bytes[..]).int_err()?.trim();
        let dataset_alias = odf::DatasetAlias::try_from(dataset_alias_str).int_err()?;
        Ok(dataset_alias)
    }

    fn stream_datasets_if<'s>(
        &'s self,
        alias_filter: impl Fn(&odf::DatasetAlias) -> bool + Send + 's,
    ) -> odf::dataset::DatasetHandleStream<'s> {
        Box::pin(async_stream::try_stream! {
            use futures::TryStreamExt;
            let mut datasets_stream = self.stored_dataset_handles();
            while let Some(hdl) = datasets_stream.try_next().await? {
                if alias_filter(&hdl.alias) {
                    yield hdl;
                }
            }
        })
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
    // TODO: PERF: Cache data and speed up lookups by ID
    //
    // TODO: CONCURRENCY: Since resolving ID to Name currently requires accessing
    // all summaries multiple threads calling this function or iterating all
    // datasets can result in significant thrashing. We use a lock here until we
    // have a better solution.
    //
    // Note that this lock does not prevent concurrent updates to summaries, only
    // reduces the chances of it.
    async fn resolve_stored_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<odf::DatasetHandle, odf::dataset::GetDatasetError> {
        // Anti-thrashing lock (see comment above)
        let _lock_guard = self.thrash_lock.lock().await;

        match dataset_ref {
            odf::DatasetRef::Handle(h) => Ok(h.clone()),
            odf::DatasetRef::Alias(alias) => {
                self.resolve_dataset_by_alias(alias)
                    .await
                    .map_err(|e| match e {
                        ResolveDatasetError::Internal(e) => {
                            odf::dataset::GetDatasetError::Internal(e)
                        }
                        ResolveDatasetError::NotFound(e) => {
                            odf::dataset::GetDatasetError::NotFound(e)
                        }
                    })
            }
            odf::DatasetRef::ID(id) => self.resolve_dataset_by_id(id).await.map_err(|e| match e {
                ResolveDatasetError::Internal(e) => odf::dataset::GetDatasetError::Internal(e),
                ResolveDatasetError::NotFound(e) => odf::dataset::GetDatasetError::NotFound(e),
            }),
        }
    }

    // TODO: PERF: Resolving handles currently involves reading alias files
    fn stored_dataset_handles(&self) -> odf::dataset::DatasetHandleStream<'_> {
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
                let dataset_layout = DatasetLayout::new(dataset_dir_entry.path());

                let dataset = Self::build_dataset(dataset_layout);

                let dataset_alias = match self.read_dataset_alias(dataset.as_ref()).await {
                    Ok(alias) => Ok(alias),
                    Err(odf::storage::GetNamedError::NotFound(_)) => {
                        continue;
                    }
                    Err(err) => Err(err),
                }
                .int_err()?;

                yield odf::DatasetHandle::new(dataset_id, dataset_alias);
            }
        })
    }

    fn stored_dataset_handles_by_owner(
        &self,
        account_name: &odf::AccountName,
    ) -> odf::dataset::DatasetHandleStream<'_> {
        if *self.tenancy_config == TenancyConfig::SingleTenant
            && *account_name != DEFAULT_ACCOUNT_NAME_STR
        {
            return Box::pin(futures::stream::empty());
        }

        let account_name = account_name.clone();
        self.stream_datasets_if(move |dataset_alias| {
            if let Some(dataset_account_name) = &dataset_alias.account_name {
                *dataset_account_name == account_name
            } else {
                true
            }
        })
    }

    fn get_stored_dataset_by_handle(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Arc<dyn odf::Dataset> {
        let layout = DatasetLayout::new(self.get_dataset_path(&dataset_handle.id));
        Self::build_dataset(layout)
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
        // Check if a dataset with the same alias can be resolved successfully
        use odf::DatasetStorageUnit;
        let maybe_existing_dataset_handle = match self
            .resolve_stored_dataset_handle_by_ref(&dataset_alias.as_local_ref())
            .await
        {
            Ok(existing_handle) => Ok(Some(existing_handle)),
            // ToDo temporary fix, remove it on favor of
            // https://github.com/kamu-data/kamu-cli/issues/342
            Err(odf::dataset::GetDatasetError::NotFound(_)) => match self
                .resolve_stored_dataset_handle_by_ref(&(seed_block.event.dataset_id.clone().into()))
                .await
            {
                Ok(existing_handle) => Ok(Some(existing_handle)),
                Err(odf::dataset::GetDatasetError::NotFound(_)) => Ok(None),
                Err(odf::dataset::GetDatasetError::Internal(e)) => Err(e),
            },
            Err(odf::dataset::GetDatasetError::Internal(e)) => Err(e),
        }?;

        // If so, there are 2 possibilities:
        // - Dataset was partially created before (no head yet) and was not GC'd - so we
        //   assume ownership
        // - Dataset existed before (has valid head) - we should error out with name
        //   collision
        if let Some(existing_dataset_handle) = maybe_existing_dataset_handle {
            let existing_dataset = self.get_stored_dataset_by_handle(&existing_dataset_handle);

            match existing_dataset
                .as_metadata_chain()
                .resolve_ref(&odf::BlockRef::Head)
                .await
            {
                // Existing head
                Ok(_) => {
                    return Err(odf::dataset::CreateDatasetError::NameCollision(
                        odf::dataset::NameCollisionError {
                            alias: dataset_alias.clone(),
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

        let dataset_path = self.get_dataset_path(&dataset_handle.id);
        let layout = DatasetLayout::create(&dataset_path).int_err()?;
        let dataset = Self::build_dataset(layout);

        // There are three possibilities at this point:
        // - Dataset did not exist before - continue normally
        // - Dataset was partially created before (no head yet) and was not GC'd - so we
        //   assume ownership
        // - Dataset existed before (has valid head) - we should error out with name
        //   collision
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
            Err(err) => {
                return Err(match err {
                    odf::dataset::AppendError::RefCASFailed(_) => {
                        odf::dataset::CreateDatasetError::RefCollision(
                            odf::dataset::RefCollisionError {
                                id: dataset_handle.id,
                            },
                        )
                    }
                    _ => err.int_err().into(),
                })
            }
        };

        self.save_dataset_alias(dataset.as_ref(), dataset_alias)
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

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_handle, %new_name))]
    async fn rename_dataset(
        &self,
        dataset_handle: &odf::DatasetHandle,
        new_name: &odf::DatasetName,
    ) -> Result<(), odf::dataset::RenameDatasetError> {
        let new_alias =
            odf::DatasetAlias::new(dataset_handle.alias.account_name.clone(), new_name.clone());

        // Note: should collision check be moved to use case level?
        match self.resolve_dataset_by_alias(&new_alias).await {
            Ok(_) => Err(odf::dataset::RenameDatasetError::NameCollision(
                odf::dataset::NameCollisionError {
                    alias: odf::DatasetAlias::new(
                        dataset_handle.alias.account_name.clone(),
                        new_name.clone(),
                    ),
                },
            )),
            Err(ResolveDatasetError::Internal(e)) => {
                Err(odf::dataset::RenameDatasetError::Internal(e))
            }
            Err(ResolveDatasetError::NotFound(_)) => Ok(()),
        }?;

        let dataset_path = self.get_dataset_path(&dataset_handle.id);
        let layout = DatasetLayout::new(dataset_path);
        let dataset = DatasetStorageUnitLocalFs::build_dataset(layout);

        let new_alias =
            odf::DatasetAlias::new(dataset_handle.alias.account_name.clone(), new_name.clone());
        self.save_dataset_alias(dataset.as_ref(), &new_alias)
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_handle))]
    async fn delete_dataset(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<(), odf::dataset::DeleteDatasetError> {
        // // Update repo info
        // let mut repo_info = self.read_repo_info().await?;
        // let index = repo_info
        //     .datasets
        //     .iter()
        //     .position(|d| d.name == dataset_handle.name)
        //     .ok_or("Inconsistent repository info")
        //     .int_err()?;
        // repo_info.datasets.remove(index);
        // self.write_repo_info(repo_info).await?;

        let dataset_dir = self.get_dataset_path(&dataset_handle.id);
        tokio::fs::remove_dir_all(dataset_dir).await.int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
enum ResolveDatasetError {
    #[error(transparent)]
    NotFound(
        #[from]
        #[backtrace]
        odf::dataset::DatasetNotFoundError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
