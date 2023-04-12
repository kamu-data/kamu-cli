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
use opendatafabric::{DatasetAlias, DatasetHandle, DatasetName, DatasetRef};
use thiserror::Error;
use url::Url;

use crate::{domain::*, infra::utils::s3_context::S3Context};

use super::{get_downstream_dependencies_impl, get_staging_name, DatasetFactoryImpl};

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

    async fn finish_create_dataset(
        &self,
        dataset: &dyn Dataset,
        staging_alias: &DatasetAlias,
        dataset_alias: &DatasetAlias,
    ) -> Result<DatasetHandle, CreateDatasetError> {
        let summary = match dataset.get_summary(GetSummaryOpts::default()).await {
            Ok(s) => Ok(s),
            Err(GetSummaryError::EmptyDataset) => unreachable!(),
            Err(GetSummaryError::Access(e)) => Err(e.int_err().into()),
            Err(GetSummaryError::Internal(e)) => Err(CreateDatasetError::Internal(e)),
        }?;

        let handle = DatasetHandle::new(summary.id, dataset_alias.clone());

        match self
            .move_bucket_items_on_dataset_rename(staging_alias, dataset_alias)
            .await
        {
            Ok(_) => Ok(handle),
            Err(MoveBucketItemsOnRenameError::NameCollision(e)) => {
                Err(CreateDatasetError::NameCollision(e))
            }
            Err(MoveBucketItemsOnRenameError::Internal(e)) => Err(CreateDatasetError::Internal(e)),
        }
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
                        .int_err()?;

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
    ) -> Result<Box<dyn DatasetBuilder>, BeginCreateDatasetError> {
        let staging_alias =
            DatasetAlias::new(None, DatasetName::new_unchecked(&get_staging_name()));
        let dataset_url = self.get_s3_bucket_path(&staging_alias);
        let dataset_result = DatasetFactoryImpl::get_s3(dataset_url).await;
        match dataset_result {
            Ok(dataset) => Ok(Box::new(DatasetBuilderS3::new(
                self.clone(),
                Arc::new(dataset),
                staging_alias,
                dataset_alias.clone(),
            ))),
            Err(e) => Err(BeginCreateDatasetError::Internal(e)),
        }
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
// DatasetS3BuilderImpl
/////////////////////////////////////////////////////////////////////////////////////////

struct DatasetBuilderS3 {
    repo: DatasetRepositoryS3,
    dataset: Arc<dyn Dataset>,
    staging_alias: DatasetAlias,
    dataset_alias: DatasetAlias,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetBuilderS3 {
    fn new(
        repo: DatasetRepositoryS3,
        dataset: Arc<dyn Dataset>,
        staging_alias: DatasetAlias,
        dataset_alias: DatasetAlias,
    ) -> Self {
        Self {
            repo,
            dataset,
            staging_alias,
            dataset_alias,
        }
    }
}

#[async_trait]
impl DatasetBuilder for DatasetBuilderS3 {
    fn as_dataset(&self) -> &dyn Dataset {
        self.dataset.as_ref()
    }

    async fn finish(&self) -> Result<DatasetHandle, CreateDatasetError> {
        match self
            .dataset
            .as_metadata_chain()
            .get_ref(&BlockRef::Head)
            .await
        {
            Ok(_) => Ok(()),
            Err(GetRefError::NotFound(_)) => {
                self.discard().await?;
                Err(CreateDatasetError::EmptyDataset)
            }
            Err(GetRefError::Access(e)) => Err(e.int_err().into()),
            Err(GetRefError::Internal(e)) => Err(CreateDatasetError::Internal(e)),
        }?;

        self.repo
            .finish_create_dataset(
                self.dataset.as_ref(),
                &self.staging_alias,
                &self.dataset_alias,
            )
            .await
    }

    async fn discard(&self) -> Result<(), InternalError> {
        self.repo
            .delete_dataset_s3_objects(&self.dataset_alias)
            .await
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
