// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::component;
use internal_error::ErrorIntoInternal;
use kamu_core::services::DatasetNotFoundError;
use kamu_core::{
    BlockRef,
    Dataset,
    DatasetFactory,
    DatasetFactoryFn,
    DatasetRegistry,
    DatasetRegistryExt,
    GetDatasetError,
    GetRefError,
    RemoteRepositoryRegistry,
    SyncError,
    SyncRef,
    SyncRequest,
    SyncRequestDestination,
    SyncRequestSource,
};
use opendatafabric::{DatasetHandleRemote, DatasetRefAny, DatasetRefRemote};
use url::Url;

use crate::{DatasetRepositoryWriter, UrlExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct SyncRequestBuilder {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_factory: Arc<dyn DatasetFactory>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    remote_repo_registry: Arc<dyn RemoteRepositoryRegistry>,
}

impl SyncRequestBuilder {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_factory: Arc<dyn DatasetFactory>,
        dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
        remote_repo_registry: Arc<dyn RemoteRepositoryRegistry>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_factory,
            dataset_repo_writer,
            remote_repo_registry,
        }
    }

    pub async fn build_sync_request(
        &self,
        src_ref: DatasetRefAny,
        dst_ref: DatasetRefAny,
        create_dst_if_not_exists: bool,
    ) -> Result<SyncRequest, SyncError> {
        let src_sync_ref = self.resolve_sync_ref(&src_ref)?;
        let src_dataset = self.get_dataset_reader(&src_sync_ref).await?;

        let dst_sync_ref = self.resolve_sync_ref(&dst_ref)?;
        let (maybe_dst_dataset, maybe_dst_dataset_factory) = self
            .get_dataset_writer(&dst_sync_ref, create_dst_if_not_exists)
            .await?;

        let sync_request = SyncRequest {
            src: SyncRequestSource {
                src_ref,
                sync_ref: src_sync_ref,
                dataset: src_dataset,
            },
            dst: SyncRequestDestination {
                sync_ref: dst_sync_ref,
                dst_ref,
                maybe_dataset: maybe_dst_dataset,
                maybe_dataset_factory: maybe_dst_dataset_factory,
            },
        };

        Ok(sync_request)
    }

    fn resolve_sync_ref(&self, any_ref: &DatasetRefAny) -> Result<SyncRef, SyncError> {
        match any_ref.as_local_ref(|repo| self.remote_repo_registry.get_repository(repo).is_ok()) {
            Ok(local_ref) => Ok(SyncRef::Local(local_ref)),
            Err(remote_ref) => Ok(SyncRef::Remote(Arc::new(resolve_remote_dataset_url(
                self.remote_repo_registry.as_ref(),
                &remote_ref,
            )?))),
        }
    }

    async fn get_dataset_reader(
        &self,
        dataset_ref: &SyncRef,
    ) -> Result<Arc<dyn Dataset>, SyncError> {
        let dataset = match dataset_ref {
            SyncRef::Local(local_ref) => {
                self.dataset_registry.get_dataset_by_ref(local_ref).await?
            }
            SyncRef::Remote(url) => {
                self.dataset_factory
                    .get_dataset(url.as_ref(), false)
                    .await?
            }
        };

        match dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
        {
            Ok(_) => Ok(dataset),
            Err(GetRefError::NotFound(_)) => Err(DatasetNotFoundError {
                dataset_ref: dataset_ref.as_any_ref(),
            }
            .into()),
            Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }
    }

    async fn get_dataset_writer(
        &self,
        dataset_ref: &SyncRef,
        create_if_not_exists: bool,
    ) -> Result<(Option<Arc<dyn Dataset>>, Option<DatasetFactoryFn>), SyncError> {
        match dataset_ref {
            SyncRef::Local(local_ref) => {
                match self.dataset_registry.get_dataset_by_ref(local_ref).await {
                    Ok(dataset) => Ok((Some(dataset), None)),
                    Err(GetDatasetError::NotFound(_)) if create_if_not_exists => {
                        let alias = local_ref.alias().unwrap().clone();
                        let repo_writer = self.dataset_repo_writer.clone();

                        Ok((
                            None,
                            Some(Box::new(move |seed_block| {
                                Box::pin(async move {
                                    // After retrieving the dataset externally, we default to
                                    // private visibility.
                                    /*let create_options = CreateDatasetUseCaseOptions {
                                        dataset_visibility: DatasetVisibility::Private,
                                    };*/

                                    repo_writer.create_dataset(&alias, seed_block).await
                                })
                            })),
                        ))
                    }
                    Err(err) => Err(err.into()),
                }
            }
            SyncRef::Remote(url) => {
                // TODO: implement authorization checks somehow
                let dataset = self
                    .dataset_factory
                    .get_dataset(url.as_ref(), create_if_not_exists)
                    .await?;

                if !create_if_not_exists {
                    match dataset
                        .as_metadata_chain()
                        .resolve_ref(&BlockRef::Head)
                        .await
                    {
                        Ok(_) => Ok(()),
                        Err(GetRefError::NotFound(_)) => Err(DatasetNotFoundError {
                            dataset_ref: dataset_ref.as_any_ref(),
                        }
                        .into()),
                        Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
                        Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
                    }?;
                }

                Ok((Some(dataset), None))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resolve_remote_dataset_url(
    remote_repo_registry: &dyn RemoteRepositoryRegistry,
    remote_ref: &DatasetRefRemote,
) -> Result<Url, SyncError> {
    // TODO: REMOTE ID
    match remote_ref {
        DatasetRefRemote::ID(_, _) => Err(SyncError::Internal(
            "Syncing remote dataset by ID is not yet supported".int_err(),
        )),
        DatasetRefRemote::Alias(alias)
        | DatasetRefRemote::Handle(DatasetHandleRemote { alias, .. }) => {
            let mut repo = remote_repo_registry.get_repository(&alias.repo_name)?;

            repo.url.ensure_trailing_slash();
            Ok(repo.url.join(&format!("{}/", alias.local_alias())).unwrap())
        }
        DatasetRefRemote::Url(url) => {
            let mut dataset_url = url.as_ref().clone();
            dataset_url.ensure_trailing_slash();
            Ok(dataset_url)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
