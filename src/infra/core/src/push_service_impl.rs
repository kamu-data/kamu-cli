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
    local_repo: Arc<dyn DatasetRepository>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    sync_svc: Arc<dyn SyncService>,
}

#[component(pub)]
impl PushServiceImpl {
    pub fn new(
        local_repo: Arc<dyn DatasetRepository>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        sync_svc: Arc<dyn SyncService>,
    ) -> Self {
        Self {
            local_repo,
            remote_alias_reg,
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
            match self.local_repo.resolve_dataset_ref(local_ref).await {
                Ok(h) => Some(h),
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
            None
        };

        match &request {
            PushRequest {
                local_ref: None,
                remote_ref: None,
            } => panic!("Push request must contain either local or remote reference"),
            PushRequest {
                local_ref: Some(_),
                remote_ref: None,
            } => match self
                .resolve_push_alias(local_handle.as_ref().unwrap())
                .await
            {
                Ok(remote_ref) => Ok(PushItem {
                    local_handle: local_handle.unwrap(),
                    remote_ref,
                    original_request: request,
                }),
                Err(e) => Err(PushResponse {
                    local_handle,
                    remote_ref: request.remote_ref.clone(),
                    result: Err(e),
                    original_request: request,
                }),
            },
            PushRequest {
                local_ref: None,
                remote_ref: Some(remote_ref),
            } => match self.inverse_lookup_dataset_by_push_alias(remote_ref).await {
                Ok(local_handle) => Ok(PushItem {
                    local_handle,
                    remote_ref: remote_ref.clone(),
                    original_request: request,
                }),
                Err(e) => Err(PushResponse {
                    local_handle: None,
                    remote_ref: Some(remote_ref.clone()),
                    result: Err(e),
                    original_request: request,
                }),
            },
            PushRequest {
                local_ref: Some(_),
                remote_ref: Some(remote_ref),
            } => Ok(PushItem {
                local_handle: local_handle.unwrap(),
                remote_ref: remote_ref.clone(),
                original_request: request,
            }),
        }
    }

    async fn resolve_push_alias(
        &self,
        local_handle: &DatasetHandle,
    ) -> Result<DatasetRefRemote, PushError> {
        let remote_aliases = self
            .remote_alias_reg
            .get_remote_aliases(&local_handle.as_local_ref())
            .await
            .int_err()?;

        let mut push_aliases: Vec<_> = remote_aliases.get_by_kind(RemoteAliasKind::Push).collect();

        match push_aliases.len() {
            0 => Err(PushError::NoTarget),
            1 => Ok(push_aliases.remove(0).clone()),
            _ => Err(PushError::AmbiguousTarget),
        }
    }

    // TODO: avoid traversing all datasets for every alias
    async fn inverse_lookup_dataset_by_push_alias(
        &self,
        remote_ref: &DatasetRefRemote,
    ) -> Result<DatasetHandle, PushError> {
        // Do a quick check when remote and local names match
        if let Some(remote_name) = remote_ref.dataset_name() {
            if let Some(local_handle) = self
                .local_repo
                .try_resolve_dataset_ref(
                    &DatasetAlias::new(None, remote_name.clone()).as_local_ref(),
                )
                .await?
            {
                if self
                    .remote_alias_reg
                    .get_remote_aliases(&local_handle.as_local_ref())
                    .await
                    .int_err()?
                    .contains(&remote_ref, RemoteAliasKind::Push)
                {
                    return Ok(local_handle);
                }
            }
        }

        // No luck - now have to search through aliases
        use tokio_stream::StreamExt;
        let mut datasets = self.local_repo.get_all_datasets();
        while let Some(dataset_handle) = datasets.next().await {
            let dataset_handle = dataset_handle?;

            if self
                .remote_alias_reg
                .get_remote_aliases(&dataset_handle.as_local_ref())
                .await
                .int_err()?
                .contains(&remote_ref, RemoteAliasKind::Push)
            {
                return Ok(dataset_handle);
            }
        }
        Err(PushError::NoTarget)
    }
}

#[async_trait::async_trait(?Send)]
impl PushService for PushServiceImpl {
    async fn push_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefAny>,
        options: PushOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<PushResponse> {
        let mut requests = dataset_refs.map(|r| {
            // TODO: Support local multi-tenancy
            match r.as_local_single_tenant_ref() {
                Ok(local_ref) => PushRequest {
                    local_ref: Some(local_ref),
                    remote_ref: None,
                },
                Err(remote_ref) => PushRequest {
                    local_ref: None,
                    remote_ref: Some(remote_ref),
                },
            }
        });

        self.push_multi_ext(&mut requests, options, sync_listener)
            .await
    }

    async fn push_multi_ext(
        &self,
        requests: &mut dyn Iterator<Item = PushRequest>,
        options: PushOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<PushResponse> {
        if options.recursive {
            unimplemented!("Recursive push is not yet supported")
        }
        if options.all {
            unimplemented!("Pushing all datasets is not yet supported")
        }

        let initial_requests: Vec<_> = requests.collect();

        let (plan, errors) = self.collect_plan(&initial_requests).await;
        if !errors.is_empty() {
            return errors;
        }

        let sync_results = self
            .sync_svc
            .sync_multi(
                &mut plan
                    .iter()
                    .map(|pi| (pi.local_handle.as_any_ref(), pi.remote_ref.as_any_ref())),
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

        // If no errors - add alliases to initial items
        if options.add_aliases && results.iter().all(|r| r.result.is_ok()) {
            for request in &initial_requests {
                match request {
                    PushRequest {
                        local_ref: Some(local_ref),
                        remote_ref: Some(remote_ref),
                    } => {
                        // TODO: Improve error handling
                        self.remote_alias_reg
                            .get_remote_aliases(local_ref)
                            .await
                            .unwrap()
                            .add(remote_ref, RemoteAliasKind::Push)
                            .unwrap();
                    }
                    _ => {}
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
            remote_ref: Some(self.remote_ref),
            result: result.map_err(|e| e.into()),
        }
    }
}
