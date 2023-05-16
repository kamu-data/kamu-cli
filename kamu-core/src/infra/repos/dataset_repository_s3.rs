// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use dill::*;
use futures::TryStreamExt;
use opendatafabric::*;
use thiserror::Error;
use url::Url;

use super::{get_downstream_dependencies_impl, DatasetFactoryImpl};
use crate::domain::*;
use crate::infra::utils::s3_context::S3Context;

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct DatasetRepositoryS3 {
    s3_context: S3Context,
    endpoint: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetRepositoryS3 {
    pub fn new(s3_context: S3Context, endpoint: String) -> Self {
        Self {
            s3_context,
            endpoint,
        }
    }

    fn get_s3_bucket_path(&self, dataset_alias: &DatasetAlias) -> Url {
        assert!(
            !dataset_alias.is_multitenant(),
            "Multitenancy is not yet supported by S3 repo"
        );

        let dataset_url_string = format!(
            "s3+{}/{}/{}/",
            self.endpoint,
            self.s3_context.bucket,
            self.s3_context.get_key(&dataset_alias.dataset_name)
        );
        Url::parse(dataset_url_string.as_str()).unwrap()
    }

    async fn get_dataset_impl(
        &self,
        dataset_alias: &DatasetAlias,
    ) -> Result<impl Dataset, InternalError> {
        assert!(
            !dataset_alias.is_multitenant(),
            "Multitenancy is not yet supported by S3 repo"
        );
        let dataset_url = self.get_s3_bucket_path(dataset_alias);
        DatasetFactoryImpl::get_s3(dataset_url).await
    }

    async fn delete_dataset_s3_objects(
        &self,
        dataset_alias: &DatasetAlias,
    ) -> Result<(), InternalError> {
        assert!(
            !dataset_alias.is_multitenant(),
            "Multitenancy is not yet supported by S3 repo"
        );
        let dataset_key_prefix = self.s3_context.get_key(&dataset_alias.dataset_name);
        self.s3_context.recursive_delete(dataset_key_prefix).await
    }

    async fn move_bucket_items_on_dataset_rename(
        &self,
        old_alias: &DatasetAlias,
        new_alias: &DatasetAlias,
    ) -> Result<(), MoveBucketItemsOnRenameError> {
        assert!(
            !new_alias.is_multitenant(),
            "Multitenancy is not yet supported by S3 repo"
        );

        let new_key_prefix = self.s3_context.get_key(&new_alias.dataset_name);
        if self
            .s3_context
            .bucket_path_exists(new_key_prefix.as_str())
            .await?
        {
            return Err(MoveBucketItemsOnRenameError::NameCollision(
                NameCollisionError {
                    alias: new_alias.clone(),
                },
            ));
        }

        let old_key_prefix = self.s3_context.get_key(&old_alias.dataset_name);
        self.s3_context
            .recursive_move(old_key_prefix, new_key_prefix)
            .await?;

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Clone for DatasetRepositoryS3 {
    fn clone(&self) -> Self {
        Self {
            s3_context: self.s3_context.clone(),
            endpoint: self.endpoint.clone(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl DatasetRegistry for DatasetRepositoryS3 {
    async fn get_dataset_url(&self, _dataset_ref: &DatasetRef) -> Result<Url, GetDatasetUrlError> {
        unimplemented!("get_dataset_url not supported by S3 repository")
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl DatasetRepository for DatasetRepositoryS3 {
    async fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, GetDatasetError> {
        match dataset_ref {
            DatasetRef::Handle(h) => Ok(h.clone()),
            DatasetRef::Alias(alias) => {
                assert!(
                    !alias.is_multitenant(),
                    "Multitenancy is not yet supported by S3 repo"
                );

                if self
                    .s3_context
                    .bucket_path_exists(&alias.dataset_name)
                    .await?
                {
                    let dataset = self.get_dataset_impl(alias).await?;
                    let summary = dataset
                        .get_summary(GetSummaryOpts::default())
                        .await
                        .map_err(|e| {
                            if let GetSummaryError::EmptyDataset = e {
                                GetDatasetError::NotFound(DatasetNotFoundError {
                                    dataset_ref: dataset_ref.clone(),
                                })
                            } else {
                                GetDatasetError::Internal(e.int_err())
                            }
                        })?;

                    Ok(DatasetHandle::new(summary.id, alias.clone()))
                } else {
                    Err(GetDatasetError::NotFound(DatasetNotFoundError {
                        dataset_ref: dataset_ref.clone(),
                    }))
                }
            }
            DatasetRef::ID(_) => {
                unimplemented!("Querying S3 bucket not supported yet");
            }
        }
    }

    fn get_all_datasets<'s>(&'s self) -> DatasetHandleStream<'s> {
        Box::pin(async_stream::try_stream! {
            let folders_common_prefixes = self.s3_context.bucket_list_folders().await?;
            for prefix in folders_common_prefixes {
                let mut prefix = prefix.prefix.unwrap();
                while prefix.ends_with('/') {
                    prefix.pop();
                }

                let name = DatasetAlias::new(None, DatasetName::try_from(prefix).int_err()?);
                let hdl = self.resolve_dataset_ref(&name.into()).await.int_err()?;
                yield hdl;
            }
        })
    }

    async fn get_dataset(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Arc<dyn Dataset>, GetDatasetError> {
        let handle = self.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self.get_dataset_impl(&handle.alias).await?;
        Ok(Arc::new(dataset))
    }

    async fn create_dataset(
        &self,
        dataset_alias: &DatasetAlias,
        seed_block: MetadataBlockTyped<Seed>,
    ) -> Result<CreateDatasetResult, CreateDatasetError> {
        let dataset_id = seed_block.event.dataset_id.clone();
        let dataset_url = self.get_s3_bucket_path(dataset_alias);
        let dataset = DatasetFactoryImpl::get_s3(dataset_url).await?;

        // There are three possiblities at this point:
        // - Dataset did not exist before - continue normally
        // - Dataset was partially created before (no head yet) and was not GC'd - so we
        //   assume ownership
        // - Dataset existed before (has valid head) - we should error out with name
        //   collision
        let head = match dataset
            .as_metadata_chain()
            .append(seed_block.into(), AppendOpts::default())
            .await
        {
            Ok(hash) => Ok(hash),
            Err(AppendError::RefCASFailed(_)) => {
                // We are using head ref CAS to detect previous existence of a dataset
                // as atomically as possible
                Err(CreateDatasetError::NameCollision(NameCollisionError {
                    alias: dataset_alias.clone(),
                }))
            }
            Err(err) => Err(err.int_err().into()),
        }?;

        Ok(CreateDatasetResult {
            dataset_handle: DatasetHandle::new(dataset_id, dataset_alias.clone()),
            dataset: Arc::new(dataset),
            head,
        })
    }

    async fn rename_dataset(
        &self,
        dataset_ref: &DatasetRef,
        new_alias: &DatasetAlias,
    ) -> Result<(), RenameDatasetError> {
        let old_alias = self.resolve_dataset_ref(dataset_ref).await?.alias;

        match self
            .move_bucket_items_on_dataset_rename(&old_alias, new_alias)
            .await
        {
            Ok(_) => Ok(()),
            Err(MoveBucketItemsOnRenameError::NameCollision(e)) => {
                Err(RenameDatasetError::NameCollision(e))
            }
            Err(MoveBucketItemsOnRenameError::Internal(e)) => Err(RenameDatasetError::Internal(e)),
        }
    }

    async fn delete_dataset(&self, dataset_ref: &DatasetRef) -> Result<(), DeleteDatasetError> {
        let dataset_handle = match self.resolve_dataset_ref(dataset_ref).await {
            Ok(dataset_handle) => dataset_handle,
            Err(GetDatasetError::NotFound(e)) => return Err(DeleteDatasetError::NotFound(e)),
            Err(GetDatasetError::Internal(e)) => return Err(DeleteDatasetError::Internal(e)),
        };

        let children: Vec<_> = get_downstream_dependencies_impl(self, dataset_ref)
            .try_collect()
            .await?;

        if !children.is_empty() {
            return Err(DanglingReferenceError {
                dataset_handle,
                children,
            }
            .into());
        }

        match self.delete_dataset_s3_objects(&dataset_handle.alias).await {
            Ok(_) => Ok(()),
            Err(e) => Err(DeleteDatasetError::Internal(e)),
        }
    }

    fn get_downstream_dependencies<'s>(
        &'s self,
        dataset_ref: &'s DatasetRef,
    ) -> DatasetHandleStream<'s> {
        Box::pin(get_downstream_dependencies_impl(self, dataset_ref))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// MoveBucketItemsOnRenameError
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
enum MoveBucketItemsOnRenameError {
    #[error(transparent)]
    NameCollision(#[from] NameCollisionError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}
