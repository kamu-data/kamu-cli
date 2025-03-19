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
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use odf_dataset::*;
use odf_metadata::*;
use odf_storage::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetStorageUnitLocalFs {
    root: PathBuf,
    dataset_lfs_builder: Arc<dyn DatasetLfsBuilder>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl DatasetStorageUnitLocalFs {
    pub fn new(root: PathBuf, dataset_lfs_builder: Arc<dyn DatasetLfsBuilder>) -> Self {
        Self {
            root,
            dataset_lfs_builder,
        }
    }

    // TODO: Public only for testing
    pub fn get_dataset_layout(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetLayout, GetStoredDatasetError> {
        let dataset_path = self.get_dataset_path(dataset_id);
        if !dataset_path.exists() {
            return Err(GetStoredDatasetError::UnresolvedId(
                DatasetUnresolvedIdError {
                    dataset_id: dataset_id.clone(),
                },
            ));
        }
        Ok(DatasetLayout::new(dataset_path))
    }

    fn get_dataset_path(&self, dataset_id: &DatasetID) -> PathBuf {
        self.root
            .join(dataset_id.as_multibase().to_stack_string().as_str())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait]
impl DatasetStorageUnit for DatasetStorageUnitLocalFs {
    #[tracing::instrument(level = "debug", name = DatasetStorageUnitLocalFs_get_stored_dataset_by_id, skip_all, fields(%dataset_id))]
    async fn get_stored_dataset_by_id(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Arc<dyn Dataset>, GetStoredDatasetError> {
        let layout = self.get_dataset_layout(dataset_id)?;
        let dataset = self
            .dataset_lfs_builder
            .build_lfs_dataset(dataset_id, layout);
        Ok(dataset)
    }

    #[tracing::instrument(level = "debug", name = DatasetStorageUnitLocalFs_stored_dataset_ids, skip_all)]
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

                let dataset_id = DatasetID::from_multibase_string(dataset_dir_entry.file_name().to_str().unwrap()).int_err()?;

                // Head must exist and be properly set
                let layout = DatasetLayout::create(dataset_dir_entry.path()).int_err()?;
                let dataset = self.dataset_lfs_builder.build_lfs_dataset(&dataset_id, layout);
                let head_res = dataset.as_metadata_chain().as_uncached_ref_repo().get(BlockRef::Head.as_str()).await;
                match head_res {
                    // Got head => good dataset
                    Ok(_) => { yield dataset_id; Ok(()) }

                    // No head => garbage
                    Err(GetRefError::NotFound(_)) => { /* skip, garbage */ Ok(())}

                    // Other cases are propagated errors
                    Err(GetRefError::Access(e)) => Err(e.int_err()),
                    Err(GetRefError::Internal(e)) => Err(e)
                }?;

            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait]
impl DatasetStorageUnitWriter for DatasetStorageUnitLocalFs {
    #[tracing::instrument(level = "debug", name = DatasetStorageUnitLocalFs_store_dataset, skip_all, fields(?seed_block))]
    async fn store_dataset(
        &self,
        seed_block: MetadataBlockTyped<Seed>,
        opts: StoreDatasetOpts,
    ) -> Result<StoreDatasetResult, StoreDatasetError> {
        // Check if a dataset with the same ID can be resolved successfully
        use DatasetStorageUnit;
        let maybe_existing_dataset = match self
            .get_stored_dataset_by_id(&seed_block.event.dataset_id)
            .await
        {
            Ok(existing_dataset) => Ok(Some(existing_dataset)),
            Err(GetStoredDatasetError::UnresolvedId(_)) => Ok(None),
            Err(GetStoredDatasetError::Internal(e)) => Err(StoreDatasetError::Internal(e)),
        }?;

        // If so, there are 2 possibilities:
        // - Dataset was partially created before (no head yet) and was not GC'd - so we
        //   assume ownership
        // - Dataset existed before (has valid head) - we should error out with ref
        //   collision
        if let Some(existing_dataset) = maybe_existing_dataset {
            match existing_dataset
                .as_metadata_chain()
                .as_uncached_ref_repo()
                .get(BlockRef::Head.as_str())
                .await
            {
                // Existing head
                Ok(_) => {
                    return Err(StoreDatasetError::RefCollision(RefCollisionError {
                        id: seed_block.event.dataset_id.clone(),
                    }));
                }

                // No head, so continue creating
                Err(GetRefError::NotFound(_)) => {}

                // Errors...
                Err(GetRefError::Access(e)) => {
                    return Err(StoreDatasetError::Internal(e.int_err()))
                }
                Err(GetRefError::Internal(e)) => return Err(StoreDatasetError::Internal(e)),
            }
        }

        // It's okay to create a new dataset by this point
        let dataset_id = seed_block.event.dataset_id.clone();

        // Create dataset
        let dataset_path = self.get_dataset_path(&dataset_id);
        let layout = DatasetLayout::create(&dataset_path).int_err()?;
        let dataset = self
            .dataset_lfs_builder
            .build_lfs_dataset(&dataset_id, layout);

        // Write seed block
        // Only set reference, if specified in the options
        let seed = match dataset
            .as_metadata_chain()
            .append(
                seed_block.into(),
                AppendOpts {
                    // We are using head ref CAS to detect previous existence of a dataset
                    // as atomically as possible
                    check_ref_is: Some(None),
                    update_ref: if opts.set_head {
                        Some(&BlockRef::Head)
                    } else {
                        None
                    },
                    ..AppendOpts::default()
                },
            )
            .await
        {
            Ok(seed) => seed,
            Err(err) => return Err(err.int_err().into()),
        };

        tracing::info!(
            id = %dataset_id,
            %seed,
            "Created new dataset",
        );

        Ok(StoreDatasetResult {
            dataset_id,
            dataset,
            seed,
        })
    }

    #[tracing::instrument(level = "debug", name = DatasetStorageUnitLocalFs_delete_dataset, skip_all, fields(%dataset_id))]
    async fn delete_dataset(&self, dataset_id: &DatasetID) -> Result<(), DeleteStoredDatasetError> {
        // Ensure dataset folder exists on disk
        let layout = self.get_dataset_layout(dataset_id)?;

        // Remove HEAD file first, it will simplify potential concurrency issues
        tokio::fs::remove_file(layout.refs_dir.join(BlockRef::Head.as_str()))
            .await
            .int_err()?;

        // Remove all stored files and the folder itself
        tokio::fs::remove_dir_all(layout.root_dir).await.int_err()?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
