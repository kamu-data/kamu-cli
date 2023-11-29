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
use event_bus::EventBus;
use futures::TryStreamExt;
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer, DEFAULT_ACCOUNT_NAME};
use kamu_core::*;
use opendatafabric::*;
use url::Url;

use super::{get_downstream_dependencies_impl, DatasetFactoryImpl};
use crate::utils::s3_context::S3Context;

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct DatasetRepositoryS3 {
    s3_context: S3Context,
    current_account_subject: Arc<CurrentAccountSubject>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    event_bus: Arc<EventBus>,
    multi_tenant: bool,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetRepositoryS3 {
    pub fn new(
        s3_context: S3Context,
        current_account_subject: Arc<CurrentAccountSubject>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        event_bus: Arc<EventBus>,
        multi_tenant: bool,
    ) -> Self {
        Self {
            s3_context,
            current_account_subject,
            dataset_action_authorizer,
            event_bus,
            multi_tenant,
        }
    }

    async fn get_dataset_impl(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<impl Dataset, InternalError> {
        let s3_context = self
            .s3_context
            .sub_context(&format!("{}/", &dataset_id.cid.to_string()));

        DatasetFactoryImpl::get_s3_from_context(s3_context).await
    }

    async fn delete_dataset_s3_objects(&self, dataset_id: &DatasetID) -> Result<(), InternalError> {
        let dataset_key_prefix = self.s3_context.get_key(&dataset_id.cid.to_string());
        self.s3_context.recursive_delete(dataset_key_prefix).await
    }

    async fn resolve_dataset_alias(
        &self,
        dataset: &dyn Dataset,
    ) -> Result<DatasetAlias, InternalError> {
        let bytes = dataset.as_info_repo().get("alias").await.int_err()?;
        let dataset_alias_str = std::str::from_utf8(&bytes[..]).int_err()?.trim();
        let dataset_alias = DatasetAlias::try_from(dataset_alias_str).int_err()?;
        Ok(dataset_alias)
    }

    async fn save_dataset_alias(
        &self,
        dataset: &dyn Dataset,
        dataset_alias: DatasetAlias,
    ) -> Result<(), InternalError> {
        dataset
            .as_info_repo()
            .set("alias", dataset_alias.to_string().as_bytes())
            .await
            .int_err()?;

        Ok(())
    }

    fn stream_datasets_if<'s>(
        &'s self,
        alias_filter: impl Fn(&DatasetAlias) -> bool + Send + 's,
    ) -> DatasetHandleStream<'s> {
        Box::pin(async_stream::try_stream! {
            let folders_common_prefixes = self.s3_context.bucket_list_folders().await?;
            for prefix in folders_common_prefixes {
                let mut prefix = prefix.prefix.unwrap();
                while prefix.ends_with('/') {
                    prefix.pop();
                }

                if let Ok(id) = DatasetID::try_from(format!("did:odf:{}", prefix)) {
                    let dataset = self.get_dataset_impl(&id).await?;
                    let dataset_alias = self.resolve_dataset_alias(&dataset).await?;
                    if alias_filter(&dataset_alias) {
                        let hdl = DatasetHandle::new(id, dataset_alias);
                        yield hdl;
                    }

                }
            }
        })
    }

    fn normalize_alias(&self, alias: &DatasetAlias) -> DatasetAlias {
        if alias.is_multi_tenant() {
            alias.clone()
        } else if self.is_multi_tenant() {
            match self.current_account_subject.as_ref() {
                CurrentAccountSubject::Anonymous(_) => {
                    panic!("Anonymous account misused, use multi-tenant alias");
                }
                CurrentAccountSubject::Logged(l) => {
                    DatasetAlias::new(Some(l.account_name.clone()), alias.dataset_name.clone())
                }
            }
        } else {
            DatasetAlias::new(None, alias.dataset_name.clone())
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
    fn is_multi_tenant(&self) -> bool {
        self.multi_tenant
    }

    async fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, GetDatasetError> {
        match dataset_ref {
            DatasetRef::Handle(h) => Ok(h.clone()),
            DatasetRef::Alias(alias) => {
                // TODO: this is really really slow and expensive!
                let normalized_alias = self.normalize_alias(alias);
                use futures::StreamExt;
                let mut datasets = self.get_all_datasets();
                while let Some(hdl) = datasets.next().await {
                    let hdl = hdl?;
                    if hdl.alias == normalized_alias {
                        return Ok(hdl);
                    }
                }
                Err(GetDatasetError::NotFound(DatasetNotFoundError {
                    dataset_ref: dataset_ref.clone(),
                }))
            }
            DatasetRef::ID(id) => {
                if self
                    .s3_context
                    .bucket_path_exists(&id.cid.to_string())
                    .await?
                {
                    let dataset = self.get_dataset_impl(id).await?;
                    let dataset_alias = self.resolve_dataset_alias(&dataset).await?;
                    Ok(DatasetHandle::new(id.clone(), dataset_alias))
                } else {
                    Err(GetDatasetError::NotFound(DatasetNotFoundError {
                        dataset_ref: dataset_ref.clone(),
                    }))
                }
            }
        }
    }

    fn get_all_datasets<'s>(&'s self) -> DatasetHandleStream<'s> {
        self.stream_datasets_if(|_| true)
    }

    fn get_datasets_by_owner<'s>(&'s self, account_name: AccountName) -> DatasetHandleStream<'s> {
        if !self.is_multi_tenant() && account_name != DEFAULT_ACCOUNT_NAME {
            return Box::pin(futures::stream::empty());
        }

        self.stream_datasets_if(move |dataset_alias| {
            if let Some(dataset_account_name) = &dataset_alias.account_name {
                dataset_account_name == &account_name
            } else {
                true
            }
        })
    }

    async fn get_dataset(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Arc<dyn Dataset>, GetDatasetError> {
        let dataset_handle = self.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self.get_dataset_impl(&dataset_handle.id).await?;
        Ok(Arc::new(dataset))
    }

    async fn create_dataset(
        &self,
        dataset_alias: &DatasetAlias,
        seed_block: MetadataBlockTyped<Seed>,
    ) -> Result<CreateDatasetResult, CreateDatasetError> {
        // Check if a dataset with the same alias can be resolved succesfully
        let maybe_existing_dataset_handle = match self
            .resolve_dataset_ref(&dataset_alias.as_local_ref())
            .await
        {
            Ok(existing_handle) => Ok(Some(existing_handle)),
            Err(GetDatasetError::NotFound(_)) => Ok(None),
            Err(GetDatasetError::Internal(e)) => Err(e),
        }?;

        // If so, there are 2 possibilities:
        // - Dataset was partially created before (no head yet) and was not GC'd - so we
        //   assume ownership
        // - Dataset existed before (has valid head) - we should error out with name
        //   collision
        if let Some(existing_dataset_handle) = maybe_existing_dataset_handle {
            let existing_dataset = self
                .get_dataset(&existing_dataset_handle.as_local_ref())
                .await
                .int_err()?;

            match existing_dataset
                .as_metadata_chain()
                .get_ref(&BlockRef::Head)
                .await
            {
                // Existing head
                Ok(_) => {
                    return Err(CreateDatasetError::NameCollision(NameCollisionError {
                        alias: dataset_alias.clone(),
                    }));
                }

                // No head, so continue creating
                Err(GetRefError::NotFound(_)) => {}

                // Errors...
                Err(GetRefError::Access(e)) => {
                    return Err(CreateDatasetError::Internal(e.int_err()))
                }
                Err(GetRefError::Internal(e)) => return Err(CreateDatasetError::Internal(e)),
            }
        }

        // It's okay to create a new dataset by this point

        let dataset_id = seed_block.event.dataset_id.clone();
        let dataset = self.get_dataset_impl(&dataset_id).await?;

        // There are three possiblities at this point:
        // - Dataset did not exist before - continue normally
        // - Dataset was partially created before (no head yet) and was not GC'd - so we
        //   assume ownership
        // - Dataset existed before (has valid head) - we should error out with name
        //   collision
        let head = match dataset
            .as_metadata_chain()
            .append(
                seed_block.into(),
                AppendOpts {
                    // We are using head ref CAS to detect previous existence of a dataset
                    // as atomically as possible
                    check_ref_is: Some(None),
                    ..AppendOpts::default()
                },
            )
            .await
        {
            Ok(head) => head,
            Err(err) => return Err(err.int_err().into()),
        };

        let normalized_alias = self.normalize_alias(&dataset_alias);
        self.save_dataset_alias(&dataset, normalized_alias).await?;

        let dataset_handle = DatasetHandle::new(dataset_id, dataset_alias.clone());

        tracing::info!(
            id = %dataset_handle.id,
            alias = %dataset_handle.alias,
            %head,
            "Created new dataset",
        );

        Ok(CreateDatasetResult {
            dataset_handle,
            dataset: Arc::new(dataset),
            head,
        })
    }

    async fn rename_dataset(
        &self,
        dataset_ref: &DatasetRef,
        new_name: &DatasetName,
    ) -> Result<(), RenameDatasetError> {
        let dataset_handle = self.resolve_dataset_ref(dataset_ref).await?;

        let dataset = self.get_dataset_impl(&dataset_handle.id).await?;

        let new_alias =
            DatasetAlias::new(dataset_handle.alias.account_name.clone(), new_name.clone());

        // Check against possible name collisions
        match self.resolve_dataset_ref(&new_alias.as_local_ref()).await {
            Ok(_) => Err(RenameDatasetError::NameCollision(NameCollisionError {
                alias: DatasetAlias::new(
                    dataset_handle.alias.account_name.clone(),
                    new_name.clone(),
                ),
            })),
            Err(GetDatasetError::Internal(e)) => Err(RenameDatasetError::Internal(e)),
            Err(GetDatasetError::NotFound(_)) => Ok(()),
        }?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, DatasetAction::Write)
            .await?;

        // It's safe to rename dataset
        self.save_dataset_alias(&dataset, new_alias).await?;

        Ok(())
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

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, DatasetAction::Write)
            .await?;

        self.delete_dataset_s3_objects(&dataset_handle.id)
            .await
            .map_err(|e| DeleteDatasetError::Internal(e))?;

        self.event_bus
            .dispatch_event(events::DatasetEventDeleted {
                dataset_id: dataset_handle.id,
            })
            .await?;

        Ok(())
    }

    fn get_downstream_dependencies<'s>(
        &'s self,
        dataset_ref: &'s DatasetRef,
    ) -> DatasetHandleStream<'s> {
        Box::pin(get_downstream_dependencies_impl(self, dataset_ref))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
