// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu_core::*;
use opendatafabric::*;

pub struct PushServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    remote_alias_resolver: Arc<dyn RemoteAliasResolver>,
    sync_svc: Arc<dyn SyncService>,
}

#[component(pub)]
#[interface(dyn PushService)]
impl PushServiceImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        remote_alias_resolver: Arc<dyn RemoteAliasResolver>,
        sync_svc: Arc<dyn SyncService>,
    ) -> Self {
        Self {
            dataset_repo,
            remote_alias_reg,
            remote_alias_resolver,
            sync_svc,
        }
    }

    async fn collect_plan(&self, items: &Vec<PushRequest>) -> (Vec<PushItem>, Vec<PushResponse>) {
        let mut plan = Vec::new();
        let mut errors = Vec::new();

        for request in items {
            match self.collect_plan_item(request.clone()).await {
                Ok(item) => plan.push(item),
                Err(err) => errors.push(err),
            }
        }

        (plan, errors)
    }

    async fn collect_plan_item(&self, request: PushRequest) -> Result<PushItem, PushResponse> {
        // Resolve local dataset if we have a local reference
        let local_handle = if let Some(local_ref) = &request.local_ref {
            match self.dataset_repo.resolve_dataset_ref(local_ref).await {
                Ok(h) => h,
                Err(e) => {
                    return Err(PushResponse {
                        local_handle: None,
                        remote_ref: request.remote_ref.clone(),
                        result: Err(e.into()),
                        original_request: request,
                    })
                }
            }
        } else {
            // We are sure that here we will have remote ref
            let transfer_ref = request.remote_ref.as_ref().unwrap();
            match self
                .remote_alias_resolver
                .inverse_lookup_dataset_by_alias(transfer_ref, RemoteAliasKind::Push)
                .await
            {
                Ok(local_handle) => local_handle,
                Err(e) => {
                    return Err(PushResponse {
                        local_handle: None,
                        remote_ref: Some(transfer_ref.clone()),
                        result: Err(e.into()),
                        original_request: request,
                    })
                }
            }
        };

        match &request {
            PushRequest {
                local_ref: None,
                remote_ref: None,
            } => panic!("Push request must contain either local or remote reference"),
            PushRequest {
                local_ref: Some(_),
                remote_ref: Some(_transfer_ref @ TransferDatasetRef::RemoteRef(remote_ref)),
            } => Ok(PushItem {
                local_handle,
                remote_ref: remote_ref.clone(),
                original_request: request,
            }),
            push_request => match self
                .remote_alias_resolver
                .resolve_remote_alias(
                    &local_handle,
                    push_request.remote_ref.clone(),
                    RemoteAliasKind::Push,
                )
                .await
            {
                Ok(remote_ref) => Ok(PushItem {
                    local_handle,
                    remote_ref,
                    original_request: request,
                }),
                Err(e) => Err(PushResponse {
                    local_handle: Some(local_handle),
                    remote_ref: request.remote_ref.clone(),
                    result: Err(e.into()),
                    original_request: request,
                }),
            },
        }
    }
}

#[async_trait::async_trait]
impl PushService for PushServiceImpl {
    async fn push_multi(
        &self,
        dataset_refs: Vec<DatasetRefAny>,
        options: PushMultiOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<PushResponse> {
        let requests = dataset_refs
            .into_iter()
            .map(
                |r| match r.as_local_ref(|_| !self.dataset_repo.is_multi_tenant()) {
                    Ok(local_ref) => PushRequest {
                        local_ref: Some(local_ref),
                        remote_ref: None,
                    },
                    Err(remote_ref) => PushRequest {
                        local_ref: None,
                        remote_ref: Some(remote_ref.into()),
                    },
                },
            )
            .collect();

        self.push_multi_ext(requests, options, sync_listener).await
    }

    async fn push_multi_ext(
        &self,
        initial_requests: Vec<PushRequest>,
        options: PushMultiOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<PushResponse> {
        if options.recursive {
            unimplemented!("Recursive push is not yet supported")
        }
        if options.all {
            unimplemented!("Pushing all datasets is not yet supported")
        }

        let (plan, errors) = self.collect_plan(&initial_requests).await;
        if !errors.is_empty() {
            return errors;
        }

        let sync_results = self
            .sync_svc
            .sync_multi(
                plan.iter()
                    .map(|pi| SyncRequest {
                        src: pi.local_handle.as_any_ref(),
                        dst: pi.remote_ref.as_any_ref(),
                    })
                    .collect(),
                options.sync_options,
                sync_listener,
            )
            .await;

        assert_eq!(plan.len(), sync_results.len());

        let results: Vec<_> = std::iter::zip(plan, sync_results)
            .map(|(pi, res)| {
                assert_eq!(pi.local_handle.as_any_ref(), res.src);
                assert_eq!(pi.remote_ref.as_any_ref(), res.dst);
                pi.into_response(res.result)
            })
            .collect();

        // If no errors - add aliases to initial items
        if options.add_aliases && results.iter().all(|r| r.result.is_ok()) {
            for request in &initial_requests {
                if let PushRequest {
                    local_ref: Some(local_ref),
                    remote_ref: Some(_transfer_ref @ TransferDatasetRef::RemoteRef(remote_ref)),
                } = request
                {
                    // TODO: Improve error handling
                    self.remote_alias_reg
                        .get_remote_aliases(local_ref)
                        .await
                        .unwrap()
                        .add(remote_ref, RemoteAliasKind::Push)
                        .await
                        .unwrap();
                }
            }
        }

        results
    }
}

#[derive(Debug)]
struct PushItem {
    original_request: PushRequest,
    local_handle: DatasetHandle,
    remote_ref: DatasetRefRemote,
}

impl PushItem {
    fn into_response(self, result: Result<SyncResult, SyncError>) -> PushResponse {
        PushResponse {
            original_request: self.original_request,
            local_handle: Some(self.local_handle),
            remote_ref: Some(self.remote_ref.into()),
            result: result.map_err(Into::into),
        }
    }
}
