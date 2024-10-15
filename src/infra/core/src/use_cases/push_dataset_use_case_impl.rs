// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::auth::{
    ClassifyByAllowanceResponse,
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetActionUnauthorizedError,
};
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
    PushDatasetUseCase,
    PushError,
    PushMultiOptions,
    PushResponse,
    RemoteAliasKind,
    RemoteAliasResolver,
    RemoteAliasesRegistry,
    RemoteRepositoryRegistry,
    RemoteTarget,
    SyncError,
    SyncMultiListener,
    SyncRef,
    SyncRequestDestination,
    SyncRequestNew,
    SyncRequestSource,
    SyncResult,
    SyncService,
};
use opendatafabric::{
    DatasetHandle,
    DatasetHandleRemote,
    DatasetPushTarget,
    DatasetRefAny,
    DatasetRefRemote,
};
use url::Url;

use crate::{DatasetRepositoryWriter, UrlExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn PushDatasetUseCase)]
pub struct PushDatasetUseCaseImpl {
    //push_service: Arc<dyn PushService>,
    sync_service: Arc<dyn SyncService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_factory: Arc<dyn DatasetFactory>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    remote_alias_resolver: Arc<dyn RemoteAliasResolver>,
    remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
    remote_repo_registry: Arc<dyn RemoteRepositoryRegistry>,
}

impl PushDatasetUseCaseImpl {
    pub fn new(
        //push_service: Arc<dyn PushService>,
        sync_service: Arc<dyn SyncService>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_factory: Arc<dyn DatasetFactory>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
        remote_alias_resolver: Arc<dyn RemoteAliasResolver>,
        remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
        remote_repo_registry: Arc<dyn RemoteRepositoryRegistry>,
    ) -> Self {
        Self {
            // push_service,
            sync_service,
            dataset_registry,
            dataset_factory,
            dataset_action_authorizer,
            dataset_repo_writer,
            remote_alias_resolver,
            remote_alias_registry,
            remote_repo_registry,
        }
    }

    async fn make_authorization_checks(
        &self,
        dataset_handles: Vec<DatasetHandle>,
        push_target: Option<&DatasetPushTarget>,
    ) -> Result<(Vec<DatasetHandle>, Vec<PushResponse>), InternalError> {
        let ClassifyByAllowanceResponse {
            authorized_handles,
            unauthorized_handles_with_errors,
        } = self
            .dataset_action_authorizer
            .classify_datasets_by_allowance(dataset_handles, DatasetAction::Read)
            .await?;

        let unauthorized_responses = unauthorized_handles_with_errors
            .into_iter()
            .map(|(hdl, error)| PushResponse {
                local_handle: Some(hdl),
                target: push_target.cloned(),
                result: Err(PushError::SyncError(match error {
                    DatasetActionUnauthorizedError::Access(e) => SyncError::Access(e),
                    DatasetActionUnauthorizedError::Internal(e) => SyncError::Internal(e),
                })),
            })
            .collect();

        Ok((authorized_handles, unauthorized_responses))
    }

    async fn collect_push_plan(
        &self,
        dataset_handles: &[DatasetHandle],
        push_target: Option<&DatasetPushTarget>,
    ) -> (Vec<PushPlanItem>, Vec<PushResponse>) {
        let mut plan = Vec::new();
        let mut errors = Vec::new();

        for hdl in dataset_handles {
            match self.collect_push_plan_item(hdl.clone(), push_target).await {
                Ok(item) => plan.push(item),
                Err(err) => errors.push(err),
            }
        }

        (plan, errors)
    }

    async fn collect_push_plan_item(
        &self,
        local_handle: DatasetHandle,
        push_target: Option<&DatasetPushTarget>,
    ) -> Result<PushPlanItem, PushResponse> {
        let local_target = self
            .dataset_registry
            .get_resolved_dataset_by_handle(&local_handle);

        match self
            .remote_alias_resolver
            .resolve_push_target(local_target, push_target.cloned())
            .await
        {
            Ok(remote_target) => Ok(PushPlanItem {
                local_handle,
                remote_target,
                push_target: push_target.cloned(),
            }),
            Err(e) => Err(PushResponse {
                local_handle: Some(local_handle),
                target: push_target.cloned(),
                result: Err(e.into()),
            }),
        }
    }

    fn resolve_sync_ref(&self, any_ref: &DatasetRefAny) -> Result<SyncRef, SyncError> {
        match any_ref.as_local_ref(|repo| self.remote_repo_registry.get_repository(repo).is_ok()) {
            Ok(local_ref) => Ok(SyncRef::Local(local_ref)),
            Err(remote_ref) => Ok(SyncRef::Remote(Arc::new(
                self.resolve_remote_dataset_url(&remote_ref)?,
            ))),
        }
    }

    fn resolve_remote_dataset_url(&self, remote_ref: &DatasetRefRemote) -> Result<Url, SyncError> {
        // TODO: REMOTE ID
        match remote_ref {
            DatasetRefRemote::ID(_, _) => Err(SyncError::Internal(
                "Syncing remote dataset by ID is not yet supported".int_err(),
            )),
            DatasetRefRemote::Alias(alias)
            | DatasetRefRemote::Handle(DatasetHandleRemote { alias, .. }) => {
                let mut repo = self.remote_repo_registry.get_repository(&alias.repo_name)?;

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

    async fn get_dataset_reader(
        &self,
        dataset_ref: &SyncRef,
    ) -> Result<Arc<dyn Dataset>, SyncError> {
        let dataset = match dataset_ref {
            SyncRef::Local(local_ref) => {
                self.dataset_registry.get_dataset_by_ref(local_ref).await?
            }
            SyncRef::Remote(url) => {
                // TODO: implement authorization checks somehow
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

#[async_trait::async_trait]
impl PushDatasetUseCase for PushDatasetUseCaseImpl {
    async fn execute_multi(
        &self,
        dataset_handles: Vec<DatasetHandle>,
        options: PushMultiOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PushResponse>, InternalError> {
        // Check for unsupported options first
        if options.recursive {
            unimplemented!("Recursive push is not yet supported")
        }
        if options.all {
            unimplemented!("Pushing all datasets is not yet supported")
        }

        // Authorization checks upon all datasets come first
        let (authorized_handles, unauthorized_responses) = self
            .make_authorization_checks(dataset_handles, options.remote_target.as_ref())
            .await?;
        if !unauthorized_responses.is_empty() {
            return Ok(unauthorized_responses);
        }

        // Prepare a push plan
        let (plan, errors) = self
            .collect_push_plan(&authorized_handles, options.remote_target.as_ref())
            .await;
        if !errors.is_empty() {
            return Ok(errors);
        }

        let mut sync_requests = Vec::new();
        for pi in &plan {
            let src_ref = pi.local_handle.as_any_ref();
            let src_sync_ref = self.resolve_sync_ref(&src_ref).int_err()?;
            let src_dataset = self.get_dataset_reader(&src_sync_ref).await.int_err()?;

            let dst_ref = (&pi.remote_target.url).into();
            let dst_sync_ref = self.resolve_sync_ref(&dst_ref).int_err()?;
            let (maybe_dst_dataset, maybe_dst_dataset_factory) = self
                .get_dataset_writer(&dst_sync_ref, options.sync_options.create_if_not_exists)
                .await
                .int_err()?;

            let sync_request = SyncRequestNew {
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
            sync_requests.push(sync_request);
        }

        let sync_results = self
            .sync_service
            .sync_multi_new(sync_requests, options.sync_options, sync_listener)
            .await;

        assert_eq!(plan.len(), sync_results.len());

        let results: Vec<_> = std::iter::zip(&plan, sync_results)
            .map(|(pi, res)| {
                let remote_ref: DatasetRefAny = (&pi.remote_target.url).into();
                assert_eq!(pi.local_handle.as_any_ref(), res.src);
                assert_eq!(remote_ref, res.dst);
                pi.as_response(res.result)
            })
            .collect();

        // If no errors - add aliases to initial items
        if options.add_aliases && results.iter().all(|r| r.result.is_ok()) {
            for push_item in &plan {
                // TODO: Improve error handling
                let dataset = self
                    .dataset_registry
                    .get_dataset_by_handle(&push_item.local_handle);
                self.remote_alias_registry
                    .get_remote_aliases(dataset)
                    .await
                    .unwrap()
                    .add(
                        &((&push_item.remote_target.url).into()),
                        RemoteAliasKind::Push,
                    )
                    .await
                    .unwrap();
            }
        }

        Ok(results)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct PushPlanItem {
    local_handle: DatasetHandle,
    remote_target: RemoteTarget,
    push_target: Option<DatasetPushTarget>,
}

impl PushPlanItem {
    fn as_response(&self, result: Result<SyncResult, SyncError>) -> PushResponse {
        PushResponse {
            local_handle: Some(self.local_handle.clone()),
            target: self.push_target.clone(),
            result: result.map_err(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
