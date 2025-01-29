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
use chrono::{DateTime, Utc};
use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME_STR};
use kamu_core::{DatasetStorageUnitWriter, DidGenerator, TenancyConfig};
use odf::dataset::{DatasetImpl, MetadataChainImpl};
use odf::storage::lfs::ObjectRepositoryCachingLocalFs;
use odf::storage::s3::{NamedObjectRepositoryS3, ObjectRepositoryS3Sha3};
use odf::storage::{
    MetadataBlockRepositoryCachingInMem,
    MetadataBlockRepositoryImpl,
    ReferenceRepositoryImpl,
};
use s3_utils::S3Context;
use time_source::SystemTimeSource;
use tokio::sync::Mutex;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetStorageUnitS3 {
    s3_context: S3Context,
    current_account_subject: Arc<CurrentAccountSubject>,
    tenancy_config: Arc<TenancyConfig>,
    registry_cache: Option<Arc<S3RegistryCache>>,
    metadata_cache_local_fs_path: Option<Arc<PathBuf>>,
    system_time_source: Arc<dyn SystemTimeSource>,
    did_generator: Arc<dyn DidGenerator>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl DatasetStorageUnitS3 {
    /// # Arguments
    ///
    /// * `registry_cache` - when present in the catalog enables in-memory cache
    ///   of the dataset IDs and aliases present in the repository, allowing to
    ///   avoid expensive bucket scanning
    ///
    /// * `metadata_cache_local_fs_path` - when specified enables the local FS
    ///   cache of metadata blocks, allowing to dramatically reduce the number
    ///   of requests to S3
    pub fn new(
        s3_context: S3Context,
        current_account_subject: Arc<CurrentAccountSubject>,
        tenancy_config: Arc<TenancyConfig>,
        registry_cache: Option<Arc<S3RegistryCache>>,
        metadata_cache_local_fs_path: Option<Arc<PathBuf>>,
        system_time_source: Arc<dyn SystemTimeSource>,
        did_generator: Arc<dyn DidGenerator>,
    ) -> Self {
        Self {
            s3_context,
            current_account_subject,
            tenancy_config,
            registry_cache,
            metadata_cache_local_fs_path,
            system_time_source,
            did_generator,
        }
    }

    fn get_dataset_impl(&self, dataset_id: &odf::DatasetID) -> Arc<dyn odf::Dataset> {
        let s3_context = self
            .s3_context
            .sub_context(&format!("{}/", &dataset_id.as_multibase()));

        let s3_context_url = s3_context.make_url();

        // TODO: Consider switching DatasetImpl to dynamic dispatch to simplify
        // configurability
        if let Some(metadata_cache_local_fs_path) = &self.metadata_cache_local_fs_path {
            Arc::new(DatasetImpl::new(
                MetadataChainImpl::new(
                    MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
                        ObjectRepositoryCachingLocalFs::new(
                            ObjectRepositoryS3Sha3::new(s3_context.sub_context("blocks/")),
                            metadata_cache_local_fs_path.clone(),
                        ),
                    )),
                    ReferenceRepositoryImpl::new(NamedObjectRepositoryS3::new(
                        s3_context.sub_context("refs/"),
                    )),
                ),
                ObjectRepositoryS3Sha3::new(s3_context.sub_context("data/")),
                ObjectRepositoryS3Sha3::new(s3_context.sub_context("checkpoints/")),
                NamedObjectRepositoryS3::new(s3_context.into_sub_context("info/")),
                s3_context_url,
            ))
        } else {
            Arc::new(DatasetImpl::new(
                MetadataChainImpl::new(
                    MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
                        ObjectRepositoryS3Sha3::new(s3_context.sub_context("blocks/")),
                    )),
                    ReferenceRepositoryImpl::new(NamedObjectRepositoryS3::new(
                        s3_context.sub_context("refs/"),
                    )),
                ),
                ObjectRepositoryS3Sha3::new(s3_context.sub_context("data/")),
                ObjectRepositoryS3Sha3::new(s3_context.sub_context("checkpoints/")),
                NamedObjectRepositoryS3::new(s3_context.into_sub_context("info/")),
                s3_context_url,
            ))
        }
    }

    async fn delete_dataset_s3_objects(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        let dataset_key_prefix = self
            .s3_context
            .get_key(&dataset_id.as_multibase().to_stack_string());
        self.s3_context.recursive_delete(dataset_key_prefix).await
    }

    async fn resolve_dataset_alias(
        &self,
        dataset: &dyn odf::Dataset,
    ) -> Result<odf::DatasetAlias, odf::storage::GetNamedError> {
        let bytes = dataset.as_info_repo().get("alias").await?;
        let dataset_alias_str = std::str::from_utf8(&bytes[..]).int_err()?.trim();
        let dataset_alias = odf::DatasetAlias::try_from(dataset_alias_str).int_err()?;
        Ok(dataset_alias)
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

    #[tracing::instrument(level = "info", skip_all)]
    async fn list_datasets_in_s3(&self) -> Result<Vec<odf::DatasetHandle>, InternalError> {
        let mut res = Vec::new();

        let folders_common_prefixes = self.s3_context.bucket_list_folders().await?;

        for prefix in folders_common_prefixes {
            let mut prefix = prefix.prefix.unwrap();
            while prefix.ends_with('/') {
                prefix.pop();
            }

            if let Ok(id) = odf::DatasetID::from_multibase_string(&prefix) {
                let dataset = self.get_dataset_impl(&id);
                let dataset_alias = match self.resolve_dataset_alias(dataset.as_ref()).await {
                    Ok(alias) => Ok(alias),
                    Err(odf::storage::GetNamedError::NotFound(_)) => {
                        tracing::warn!(
                            s3_prefix = prefix,
                            "Found dataset entry without a valid alias - this is likely a result \
                             of interrupted creation or a push - ignoring this entry and leaving \
                             it to be cleaned up by GC."
                        );
                        continue;
                    }
                    Err(err) => Err(err),
                }
                .int_err()?;

                res.push(odf::DatasetHandle::new(id, dataset_alias));
            }
        }

        Ok(res)
    }

    async fn list_datasets_maybe_cached(&self) -> Result<Vec<odf::DatasetHandle>, InternalError> {
        if let Some(cache) = &self.registry_cache {
            let mut cache = cache.state.lock().await;

            // Init cache
            if cache.last_updated == DateTime::UNIX_EPOCH {
                tracing::debug!("Initializing dataset registry cache");
                cache.datasets = self.list_datasets_in_s3().await?;
                cache.last_updated = self.system_time_source.now();
            }

            Ok(cache.datasets.clone())
        } else {
            self.list_datasets_in_s3().await
        }
    }

    fn stream_datasets_if<'s>(
        &'s self,
        alias_filter: impl Fn(&odf::DatasetAlias) -> bool + Send + 's,
    ) -> odf::dataset::DatasetHandleStream<'s> {
        Box::pin(async_stream::try_stream! {
            for hdl in self.list_datasets_maybe_cached().await? {
                if alias_filter(&hdl.alias) {
                    yield hdl;
                }
            }
        })
    }

    fn normalize_alias(&self, alias: &odf::DatasetAlias) -> odf::DatasetAlias {
        if alias.is_multi_tenant() {
            alias.clone()
        } else if *self.tenancy_config == TenancyConfig::MultiTenant {
            match self.current_account_subject.as_ref() {
                CurrentAccountSubject::Anonymous(_) => {
                    panic!("Anonymous account misused, use multi-tenant alias");
                }
                CurrentAccountSubject::Logged(l) => {
                    odf::DatasetAlias::new(Some(l.account_name.clone()), alias.dataset_name.clone())
                }
            }
        } else {
            odf::DatasetAlias::new(None, alias.dataset_name.clone())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl odf::DatasetStorageUnit for DatasetStorageUnitS3 {
    async fn resolve_stored_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<odf::DatasetHandle, odf::dataset::GetDatasetError> {
        match dataset_ref {
            odf::DatasetRef::Handle(h) => Ok(h.clone()),
            odf::DatasetRef::Alias(alias) => {
                // TODO: this is really really slow and expensive!
                let normalized_alias = self.normalize_alias(alias);
                use futures::StreamExt;
                let mut datasets = self.stored_dataset_handles();
                while let Some(hdl) = datasets.next().await {
                    let hdl = hdl?;
                    if hdl.alias == normalized_alias {
                        return Ok(hdl);
                    }
                }
                Err(odf::dataset::GetDatasetError::NotFound(
                    odf::dataset::DatasetNotFoundError {
                        dataset_ref: dataset_ref.clone(),
                    },
                ))
            }
            odf::DatasetRef::ID(id) => {
                if self
                    .s3_context
                    .bucket_path_exists(id.as_multibase().to_stack_string().as_str())
                    .await?
                {
                    let dataset = self.get_dataset_impl(id);
                    let dataset_alias = self
                        .resolve_dataset_alias(dataset.as_ref())
                        .await
                        .int_err()?;
                    Ok(odf::DatasetHandle::new(id.clone(), dataset_alias))
                } else {
                    Err(odf::dataset::GetDatasetError::NotFound(
                        odf::dataset::DatasetNotFoundError {
                            dataset_ref: dataset_ref.clone(),
                        },
                    ))
                }
            }
        }
    }

    fn stored_dataset_handles(&self) -> odf::dataset::DatasetHandleStream<'_> {
        self.stream_datasets_if(|_| true)
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
        self.get_dataset_impl(&dataset_handle.id)
    }
}

#[async_trait]
impl DatasetStorageUnitWriter for DatasetStorageUnitS3 {
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_alias, ?seed_block))]
    async fn create_dataset(
        &self,
        dataset_alias: &odf::DatasetAlias,
        seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
    ) -> Result<odf::CreateDatasetResult, odf::dataset::CreateDatasetError> {
        use odf::DatasetStorageUnit;

        // TODO: AUTH: Introduce AccountActionAuthorizer to check whether the current
        // subject has permissions to create dataset under the account specified in the
        // dataset_alias.
        let dataset_alias = self.normalize_alias(dataset_alias);

        // Check if a dataset with the same alias can be resolved successfully
        let maybe_existing_dataset_handle = match self
            .resolve_stored_dataset_handle_by_ref(&dataset_alias.as_local_ref())
            .await
        {
            Ok(existing_handle) => Ok(Some(existing_handle)),
            Err(odf::dataset::GetDatasetError::NotFound(_)) => Ok(None),
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

        let dataset_handle =
            odf::DatasetHandle::new(seed_block.event.dataset_id.clone(), dataset_alias.clone());
        let dataset = self.get_dataset_impl(&dataset_handle.id);

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

        self.save_dataset_alias(dataset.as_ref(), &dataset_alias)
            .await?;

        // Update cache if enabled
        if let Some(cache) = &self.registry_cache {
            let mut cache = cache.state.lock().await;
            cache.datasets.push(dataset_handle.clone());
        }

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

    #[tracing::instrument(level = "debug", skip_all, fields(?snapshot))]
    async fn create_dataset_from_snapshot(
        &self,
        snapshot: odf::DatasetSnapshot,
    ) -> Result<odf::CreateDatasetFromSnapshotResult, odf::dataset::CreateDatasetFromSnapshotError>
    {
        create_dataset_from_snapshot_impl(
            self,
            snapshot,
            self.system_time_source.now(),
            self.did_generator.generate_dataset_id(),
        )
        .await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_handle, %new_name))]
    async fn rename_dataset(
        &self,
        dataset_handle: &odf::DatasetHandle,
        new_name: &odf::DatasetName,
    ) -> Result<(), odf::dataset::RenameDatasetError> {
        let dataset = self.get_dataset_impl(&dataset_handle.id);

        let new_alias =
            odf::DatasetAlias::new(dataset_handle.alias.account_name.clone(), new_name.clone());

        // Note: should collision check be moved to use case level?
        use odf::DatasetStorageUnit;
        match self
            .resolve_stored_dataset_handle_by_ref(&new_alias.as_local_ref())
            .await
        {
            Ok(_) => Err(odf::dataset::RenameDatasetError::NameCollision(
                odf::dataset::NameCollisionError {
                    alias: odf::DatasetAlias::new(
                        dataset_handle.alias.account_name.clone(),
                        new_name.clone(),
                    ),
                },
            )),
            Err(odf::dataset::GetDatasetError::Internal(e)) => {
                Err(odf::dataset::RenameDatasetError::Internal(e))
            }
            Err(odf::dataset::GetDatasetError::NotFound(_)) => Ok(()),
        }?;

        // It's safe to rename dataset
        self.save_dataset_alias(dataset.as_ref(), &new_alias)
            .await?;

        // Update cache if enabled
        if let Some(cache) = &self.registry_cache {
            let mut cache = cache.state.lock().await;
            cache.datasets.retain(|h| h.id != dataset_handle.id);
            cache.datasets.push(odf::DatasetHandle::new(
                dataset_handle.id.clone(),
                new_alias,
            ));
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_handle))]
    async fn delete_dataset(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<(), odf::dataset::DeleteDatasetError> {
        self.delete_dataset_s3_objects(&dataset_handle.id)
            .await
            .map_err(odf::dataset::DeleteDatasetError::Internal)?;

        // Update cache if enabled
        if let Some(cache) = &self.registry_cache {
            let mut cache = cache.state.lock().await;
            cache.datasets.retain(|h| h.id != dataset_handle.id);
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct S3RegistryCache {
    state: Arc<Mutex<State>>,
}

struct State {
    datasets: Vec<odf::DatasetHandle>,
    last_updated: DateTime<Utc>,
}

#[component(pub)]
#[dill::scope(Singleton)]
impl S3RegistryCache {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                datasets: Vec::new(),
                last_updated: DateTime::UNIX_EPOCH,
            })),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
