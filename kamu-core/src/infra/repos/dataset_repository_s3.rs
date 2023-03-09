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
use opendatafabric::{DatasetHandle, DatasetName, DatasetRefLocal, DatasetSnapshot};
use rusoto_s3::*;
use url::Url;

use crate::{domain::*, infra::utils::s3_context::S3Context};

use super::DatasetFactoryImpl;

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

    async fn discard_create_dataset(
        &self,
        dataset_name: &DatasetName,
    ) -> Result<(), InternalError> {
        let dataset_key_prefix = self.s3_context.get_key(dataset_name.as_str());
        let list_response = self
            .s3_context
            .client
            .list_objects(ListObjectsRequest {
                bucket: self.s3_context.bucket.clone(),
                prefix: Some(dataset_key_prefix),
                ..ListObjectsRequest::default()
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
        dataset_name: &DatasetName,
    ) -> Result<DatasetHandle, CreateDatasetError> {
        let summary = match dataset.get_summary(GetSummaryOpts::default()).await {
            Ok(s) => Ok(s),
            Err(GetSummaryError::EmptyDataset) => unreachable!(),
            Err(GetSummaryError::Access(e)) => Err(e.int_err().into()),
            Err(GetSummaryError::Internal(e)) => Err(CreateDatasetError::Internal(e)),
        }?;

        let handle = DatasetHandle::new(summary.id, dataset_name.clone());

        // Check for late name collision - this seems to have sense with staging functin only
        /*if let Some(existing_dataset) = self
            .try_resolve_dataset_ref(&dataset_name.as_local_ref())
            .await?
        {
            return Err(NameCollisionError {
                name: existing_dataset.name,
            }
            .into());
        }*/

        Ok(handle)
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
                match self
                    .s3_context
                    .client
                    .list_objects(ListObjectsRequest {
                        bucket: self.s3_context.bucket.clone(),
                        prefix: Some(self.s3_context.get_key(name.as_str())),
                        max_keys: Some(1),
                        ..ListObjectsRequest::default()
                    })
                    .await
                {
                    Ok(resp) => {
                        if resp.contents.is_some() {
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
        unimplemented!("get_all_datasets not supported by S3 repository")
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
        let dataset_url = self.get_s3_bucket_path(dataset_name.as_str());
        let dataset_result = DatasetFactoryImpl::get_s3(dataset_url);
        match dataset_result {
            Ok(dataset) => Ok(Box::new(DatasetS3BuilderImpl::new(
                self.clone(),
                Arc::new(dataset),
                dataset_name.clone(),
            ))),
            Err(e) => Err(BeginCreateDatasetError::Internal(e)),
        }
    }

    async fn create_dataset_from_snapshot(
        &self,
        _snapshot: DatasetSnapshot,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError> {
        unimplemented!("create_dataset_from_snapshot not supported by S3 repository")
    }

    async fn create_datasets_from_snapshots(
        &self,
        _snapshots: Vec<DatasetSnapshot>,
    ) -> Vec<(
        DatasetName,
        Result<CreateDatasetResult, CreateDatasetFromSnapshotError>,
    )> {
        unimplemented!("create_datasets_from_snapshots not supported by S3 repository")
    }

    async fn rename_dataset(
        &self,
        _dataset_ref: &DatasetRefLocal,
        _new_name: &DatasetName,
    ) -> Result<(), RenameDatasetError> {
        unimplemented!("rename_dataset not supported by S3 repository")
    }

    async fn delete_dataset(
        &self,
        _dataset_ref: &DatasetRefLocal,
    ) -> Result<(), DeleteDatasetError> {
        unimplemented!("delete_dataset not supported by S3 repository")
    }

    fn get_downstream_dependencies<'s>(
        &'s self,
        _dataset_ref: &'s DatasetRefLocal,
    ) -> DatasetHandleStream<'s> {
        unimplemented!("get_downstream_dependencies not supported by S3 repository")
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// DatasetS3BuilderImpl
/////////////////////////////////////////////////////////////////////////////////////////

struct DatasetS3BuilderImpl {
    repo: DatasetRepositoryS3,
    dataset: Arc<dyn Dataset>,
    dataset_name: DatasetName,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetS3BuilderImpl {
    fn new(
        repo: DatasetRepositoryS3,
        dataset: Arc<dyn Dataset>,
        dataset_name: DatasetName,
    ) -> Self {
        Self {
            repo,
            dataset,
            dataset_name,
        }
    }
}

#[async_trait]
impl DatasetBuilder for DatasetS3BuilderImpl {
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
            .finish_create_dataset(self.dataset.as_ref(), &self.dataset_name)
            .await
    }

    async fn discard(&self) -> Result<(), InternalError> {
        self.repo.discard_create_dataset(&self.dataset_name).await
    }
}
