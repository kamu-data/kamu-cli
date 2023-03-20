// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use dill::*;
use futures::TryStreamExt;
use opendatafabric::{DatasetHandle, DatasetName, DatasetRefLocal};
use rusoto_s3::*;
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

    fn get_s3_bucket_path(&self, dataset_name: &str) -> Url {
        let dataset_url_string = format!(
            "s3+{}/{}/{}/",
            self.endpoint,
            self.s3_context.bucket,
            self.s3_context.get_key(dataset_name)
        );
        Url::parse(dataset_url_string.as_str()).unwrap()
    }

    fn get_dataset_impl(&self, dataset_name: &DatasetName) -> Result<impl Dataset, InternalError> {
        let dataset_url = self.get_s3_bucket_path(dataset_name.as_str());
        DatasetFactoryImpl::get_s3(dataset_url)
    }

    async fn delete_s3_objects_by_prefix(
        &self,
        dataset_name: &DatasetName,
    ) -> Result<(), InternalError> {
        let dataset_key_prefix = self.s3_context.get_key(dataset_name.as_str());
        let list_response = self
            .s3_context
            .client
            .list_objects_v2(ListObjectsV2Request {
                bucket: self.s3_context.bucket.clone(),
                prefix: Some(dataset_key_prefix),
                ..ListObjectsV2Request::default()
            })
            .await
            .int_err()?;

        if let Some(contents) = list_response.contents {
            self.s3_context
                .client
                .delete_objects(DeleteObjectsRequest {
                    bucket: self.s3_context.bucket.clone(),
                    delete: Delete {
                        objects: contents
                            .iter()
                            .map(|obj| ObjectIdentifier {
                                key: obj.key.clone().unwrap(),
                                version_id: None,
                            })
                            .collect(),
                        quiet: Some(true),
                    },
                    ..DeleteObjectsRequest::default()
                })
                .await
                .int_err()?;
        }

        Ok(())
    }

    async fn finish_create_dataset(
        &self,
        dataset: &dyn Dataset,
        staging_name: &str,
        dataset_name: &DatasetName,
    ) -> Result<DatasetHandle, CreateDatasetError> {
        let summary = match dataset.get_summary(GetSummaryOpts::default()).await {
            Ok(s) => Ok(s),
            Err(GetSummaryError::EmptyDataset) => unreachable!(),
            Err(GetSummaryError::Access(e)) => Err(e.int_err().into()),
            Err(GetSummaryError::Internal(e)) => Err(CreateDatasetError::Internal(e)),
        }?;

        let handle = DatasetHandle::new(summary.id, dataset_name.clone());

        match self
            .move_bucket_items_on_dataset_rename(staging_name, dataset_name.as_str())
            .await
        {
            Ok(_) => Ok(handle),
            Err(MoveBucketItemsOnRenameError::NameCollision(e)) => {
                Err(CreateDatasetError::NameCollision(e))
            }
            Err(MoveBucketItemsOnRenameError::Internal(e)) => Err(CreateDatasetError::Internal(e)),
        }
    }

    async fn bucket_path_exists(&self, key_prefix: &str) -> Result<bool, InternalError> {
        let listing = self
            .s3_context
            .client
            .list_objects_v2(ListObjectsV2Request {
                bucket: self.s3_context.bucket.clone(),
                prefix: Some(self.s3_context.get_key(key_prefix)),
                max_keys: Some(1),
                ..ListObjectsV2Request::default()
            })
            .await;

        match listing {
            Ok(resp) => Ok(resp.contents.is_some()),
            Err(e) => Err(e.int_err().into()),
        }
    }

    async fn move_bucket_items_on_dataset_rename(
        &self,
        old_dataset_name: &str,
        new_dataset_name: &str,
    ) -> Result<(), MoveBucketItemsOnRenameError> {
        let target_key_prefix = self.s3_context.get_key(new_dataset_name);
        match self.bucket_path_exists(target_key_prefix.as_str()).await {
            Ok(resp) => {
                if resp {
                    return Err(MoveBucketItemsOnRenameError::NameCollision(
                        NameCollisionError {
                            name: DatasetName::from_str(new_dataset_name).unwrap(),
                        },
                    ));
                }
            }
            Err(e) => return Err(MoveBucketItemsOnRenameError::Internal(e)),
        }

        let old_key_prefix = self.s3_context.get_key(old_dataset_name);
        let list_response = self
            .s3_context
            .client
            .list_objects_v2(ListObjectsV2Request {
                bucket: self.s3_context.bucket.clone(),
                prefix: Some(old_key_prefix.clone()),
                ..ListObjectsV2Request::default()
            })
            .await
            .int_err()?;

        // TODO: concurrency safety.
        // It is important not to allow parallel writes of Head reference file in the same bucket.
        // Consider optimistic locking (comparing old head with expected before final commit).

        if let Some(contents) = list_response.contents {
            for obj in &contents {
                let copy_source = format!(
                    "{}/{}",
                    self.s3_context.bucket.clone(),
                    obj.key.clone().unwrap()
                );
                let new_key = obj
                    .key
                    .clone()
                    .unwrap()
                    .replace(old_key_prefix.as_str(), target_key_prefix.as_str());
                self.s3_context
                    .client
                    .copy_object(CopyObjectRequest {
                        bucket: self.s3_context.bucket.clone(),
                        copy_source,
                        key: new_key,
                        ..CopyObjectRequest::default()
                    })
                    .await
                    .int_err()?;
            }

            self.s3_context
                .client
                .delete_objects(DeleteObjectsRequest {
                    bucket: self.s3_context.bucket.clone(),
                    delete: Delete {
                        objects: contents
                            .iter()
                            .map(|obj| ObjectIdentifier {
                                key: obj.key.clone().unwrap(),
                                version_id: None,
                            })
                            .collect(),
                        quiet: Some(true),
                    },
                    ..DeleteObjectsRequest::default()
                })
                .await
                .int_err()?;
        }

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
    async fn get_dataset_url(
        &self,
        _dataset_ref: &DatasetRefLocal,
    ) -> Result<Url, GetDatasetUrlError> {
        unimplemented!("get_dataset_url not supported by S3 repository")
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl DatasetRepository for DatasetRepositoryS3 {
    async fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<DatasetHandle, GetDatasetError> {
        match dataset_ref {
            DatasetRefLocal::Handle(h) => Ok(h.clone()),
            DatasetRefLocal::Name(name) => {
                match self.bucket_path_exists(name.as_str()).await {
                    Ok(resp) => {
                        if resp {
                            Ok(())
                        } else {
                            Err(GetDatasetError::NotFound(DatasetNotFoundError {
                                dataset_ref: dataset_ref.clone(),
                            }))
                        }
                    }
                    Err(e) => Err(e.int_err().into()),
                }?;

                let dataset = self.get_dataset_impl(name)?;
                let summary = dataset
                    .get_summary(GetSummaryOpts::default())
                    .await
                    .int_err()?;

                Ok(DatasetHandle::new(summary.id, name.clone()))
            }
            DatasetRefLocal::ID(_idd) => {
                unimplemented!("Querying S3 bucket not supported yet");
            }
        }
    }

    fn get_all_datasets<'s>(&'s self) -> DatasetHandleStream<'s> {
        Box::pin(async_stream::try_stream! {
            let list_objects_resp = self
                .s3_context
                .client
                .list_objects_v2(ListObjectsV2Request {
                    bucket: self.s3_context.bucket.clone(),
                    delimiter: Some("/".to_owned()),
                    ..ListObjectsV2Request::default()
                })
                .await
                .int_err()?;

            for prefix in list_objects_resp.common_prefixes.unwrap_or_default() {
                let mut prefix = prefix.prefix.unwrap();
                while prefix.ends_with('/') {
                    prefix.pop();
                }

                let name = DatasetName::try_from(prefix).int_err()?;
                let hdl = self.resolve_dataset_ref(&name.into()).await.int_err()?;
                yield hdl;
            }
        })
    }

    async fn get_dataset(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Arc<dyn Dataset>, GetDatasetError> {
        let handle = self.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self.get_dataset_impl(&handle.name)?;
        Ok(Arc::new(dataset))
    }

    async fn create_dataset(
        &self,
        dataset_name: &DatasetName,
    ) -> Result<Box<dyn DatasetBuilder>, BeginCreateDatasetError> {
        let staging_name = get_staging_name();
        let dataset_url = self.get_s3_bucket_path(staging_name.as_str());
        let dataset_result = DatasetFactoryImpl::get_s3(dataset_url);
        match dataset_result {
            Ok(dataset) => Ok(Box::new(DatasetBuildS3::new(
                self.clone(),
                Arc::new(dataset),
                staging_name,
                dataset_name.clone(),
            ))),
            Err(e) => Err(BeginCreateDatasetError::Internal(e)),
        }
    }

    async fn rename_dataset(
        &self,
        dataset_ref: &DatasetRefLocal,
        new_name: &DatasetName,
    ) -> Result<(), RenameDatasetError> {
        let old_name = self.resolve_dataset_ref(dataset_ref).await?.name;
        match self
            .move_bucket_items_on_dataset_rename(old_name.as_str(), new_name.as_str())
            .await
        {
            Ok(_) => Ok(()),
            Err(MoveBucketItemsOnRenameError::NameCollision(e)) => {
                Err(RenameDatasetError::NameCollision(e))
            }
            Err(MoveBucketItemsOnRenameError::Internal(e)) => Err(RenameDatasetError::Internal(e)),
        }
    }

    async fn delete_dataset(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<(), DeleteDatasetError> {
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

        match self.delete_s3_objects_by_prefix(&dataset_handle.name).await {
            Ok(_) => Ok(()),
            Err(e) => Err(DeleteDatasetError::Internal(e)),
        }
    }

    fn get_downstream_dependencies<'s>(
        &'s self,
        dataset_ref: &'s DatasetRefLocal,
    ) -> DatasetHandleStream<'s> {
        Box::pin(get_downstream_dependencies_impl(self, dataset_ref))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// DatasetS3BuilderImpl
/////////////////////////////////////////////////////////////////////////////////////////

struct DatasetBuildS3 {
    repo: DatasetRepositoryS3,
    dataset: Arc<dyn Dataset>,
    staging_name: String,
    dataset_name: DatasetName,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetBuildS3 {
    fn new(
        repo: DatasetRepositoryS3,
        dataset: Arc<dyn Dataset>,
        staging_name: String,
        dataset_name: DatasetName,
    ) -> Self {
        Self {
            repo,
            dataset,
            staging_name,
            dataset_name,
        }
    }
}

#[async_trait]
impl DatasetBuilder for DatasetBuildS3 {
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
                self.staging_name.as_str(),
                &self.dataset_name,
            )
            .await
    }

    async fn discard(&self) -> Result<(), InternalError> {
        self.repo
            .delete_s3_objects_by_prefix(&self.dataset_name)
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
