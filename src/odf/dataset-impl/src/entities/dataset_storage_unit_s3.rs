// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dill::*;
use internal_error::{ErrorIntoInternal, InternalError};
use odf_dataset::*;
use odf_metadata::*;
use odf_storage::*;
use s3_utils::S3Context;
use time_source::SystemTimeSource;
use tokio::sync::Mutex;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetStorageUnitS3 {
    s3_context: S3Context,
    registry_cache: Option<Arc<S3RegistryCache>>,
    system_time_source: Arc<dyn SystemTimeSource>,
    dataset_s3_builder: Arc<dyn DatasetS3Builder>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl DatasetStorageUnitS3 {
    /// # Arguments
    ///
    /// * `registry_cache` - when present in the catalog enables in-memory cache
    ///   of the dataset IDs present in the repository, allowing to avoid
    ///   expensive bucket scanning
    pub fn new(
        s3_context: S3Context,
        registry_cache: Option<Arc<S3RegistryCache>>,
        system_time_source: Arc<dyn SystemTimeSource>,
        dataset_s3_builder: Arc<dyn DatasetS3Builder>,
    ) -> Self {
        Self {
            s3_context,
            registry_cache,
            system_time_source,
            dataset_s3_builder,
        }
    }

    fn get_dataset_impl(&self, dataset_id: &DatasetID) -> Arc<dyn Dataset> {
        let s3_context = self
            .s3_context
            .sub_context(&format!("{}/", &dataset_id.as_multibase()));

        self.dataset_s3_builder
            .build_s3_dataset(dataset_id, s3_context)
    }

    async fn delete_dataset_s3_objects(&self, dataset_id: &DatasetID) -> Result<(), InternalError> {
        let dataset_key_prefix = self
            .s3_context
            .get_key(&dataset_id.as_multibase().to_stack_string());
        self.s3_context.recursive_delete(dataset_key_prefix).await
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn list_dataset_ids_in_s3(&self) -> Result<HashSet<DatasetID>, InternalError> {
        let mut res = HashSet::new();

        let folders_common_prefixes = self.s3_context.bucket_list_folders().await?;

        for prefix in folders_common_prefixes {
            let mut prefix = prefix.prefix.unwrap();
            while prefix.ends_with('/') {
                prefix.pop();
            }

            if let Ok(id) = DatasetID::from_multibase_string(&prefix) {
                res.insert(id);
            }
        }

        Ok(res)
    }

    async fn list_dataset_ids_maybe_cached(&self) -> Result<HashSet<DatasetID>, InternalError> {
        if let Some(cache) = &self.registry_cache {
            let mut cache = cache.state.lock().await;

            // Init cache
            if cache.last_updated == DateTime::UNIX_EPOCH {
                tracing::debug!("Initializing dataset registry cache");
                cache.dataset_ids = self.list_dataset_ids_in_s3().await?;
                cache.last_updated = self.system_time_source.now();
            }

            Ok(cache.dataset_ids.clone())
        } else {
            self.list_dataset_ids_in_s3().await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait]
impl DatasetStorageUnit for DatasetStorageUnitS3 {
    #[tracing::instrument(level = "debug", name = DatasetStorageUnitS3_get_stored_dataset_by_id, skip_all, fields(%dataset_id))]
    async fn get_stored_dataset_by_id(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Arc<dyn Dataset>, GetStoredDatasetError> {
        if self
            .s3_context
            .bucket_path_exists(dataset_id.as_multibase().to_stack_string().as_str())
            .await?
        {
            let dataset = self.get_dataset_impl(dataset_id);
            Ok(dataset)
        } else {
            Err(GetStoredDatasetError::UnresolvedId(
                DatasetUnresolvedIdError {
                    dataset_id: dataset_id.clone(),
                },
            ))
        }
    }

    #[tracing::instrument(level = "debug", name = DatasetStorageUnitS3_stored_dataset_ids, skip_all)]
    fn stored_dataset_ids(&self) -> DatasetIDStream<'_> {
        Box::pin(async_stream::try_stream! {
            for dataset_id in self.list_dataset_ids_maybe_cached().await? {
                // Head must exist, otherwise it's a garbage
                let dataset = self.get_dataset_impl(&dataset_id);
                let head_res = dataset.as_metadata_chain().as_raw_ref_repo().get(BlockRef::Head.as_str()).await;
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

#[common_macros::method_names_consts]
#[async_trait]
impl DatasetStorageUnitWriter for DatasetStorageUnitS3 {
    #[tracing::instrument(level = "debug", name = DatasetStorageUnitS3_store_dataset, skip_all, fields(?seed_block))]
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
                .as_raw_ref_repo()
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
        let dataset = self.get_dataset_impl(&dataset_id);

        // Write seed block.
        // Set HEAD only if specified in the options
        let seed: Multihash = match dataset
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

        // Update cache if enabled
        if let Some(cache) = &self.registry_cache {
            let mut cache = cache.state.lock().await;
            cache.dataset_ids.insert(dataset_id.clone());
        }

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

    #[tracing::instrument(level = "debug", name = DatasetStorageUnitS3_delete_dataset, skip_all, fields(%dataset_id))]
    async fn delete_dataset(&self, dataset_id: &DatasetID) -> Result<(), DeleteStoredDatasetError> {
        // Ensure key exists in S3
        let _ = self.get_stored_dataset_by_id(dataset_id).await?;

        // Remove HEAD object first, it will simplify potential concurrency issues
        let head_key = self.s3_context.get_key(
            format!(
                "{}/refs/{}",
                &dataset_id.as_multibase().to_stack_string(),
                BlockRef::Head.as_str()
            )
            .as_str(),
        );
        self.s3_context
            .delete_object(head_key)
            .await
            .map_err(|e| e.into_service_error().int_err())?;

        // Remove all objects under the key
        self.delete_dataset_s3_objects(dataset_id).await?;

        // Update cache if enabled
        if let Some(cache) = &self.registry_cache {
            let mut cache = cache.state.lock().await;
            cache.dataset_ids.remove(dataset_id);
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct S3RegistryCache {
    state: Arc<Mutex<State>>,
}

struct State {
    dataset_ids: HashSet<DatasetID>,
    last_updated: DateTime<Utc>,
}

#[component(pub)]
#[dill::scope(Singleton)]
impl S3RegistryCache {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                dataset_ids: HashSet::new(),
                last_updated: DateTime::UNIX_EPOCH,
            })),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
