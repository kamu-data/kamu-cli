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

    async fn collect_plan(
        &self,
        items: &Vec<DatasetRef>,
        push_target: &Option<DatasetPushTarget>,
    ) -> (Vec<PushItem>, Vec<PushResponse>) {
        let mut plan = Vec::new();
        let mut errors = Vec::new();

        for dataset_ref in items {
            match self.collect_plan_item(dataset_ref, push_target).await {
                Ok(item) => plan.push(item),
                Err(err) => errors.push(err),
            }
        }

        (plan, errors)
    }

    async fn collect_plan_item(
        &self,
        dataset_ref: &DatasetRef,
        push_target: &Option<DatasetPushTarget>,
    ) -> Result<PushItem, PushResponse> {
        // Resolve local dataset if we have a local reference
        let local_handle = match self.dataset_repo.resolve_dataset_ref(dataset_ref).await {
            Ok(h) => h,
            Err(e) => {
                return Err(PushResponse {
                    local_handle: None,
                    target: push_target.clone(),
                    result: Err(e.into()),
                })
            }
        };

        match self
            .remote_alias_resolver
            .resolve_push_target(&local_handle, push_target.clone())
            .await
        {
            Ok(remote_target) => Ok(PushItem {
                local_handle,
                remote_target,
                push_target: push_target.clone(),
            }),
            Err(e) => Err(PushResponse {
                local_handle: Some(local_handle),
                target: push_target.clone(),
                result: Err(e.into()),
            }),
        }
    }
}

#[async_trait::async_trait]
impl PushService for PushServiceImpl {
    async fn push_multi(
        &self,
        dataset_refs: Vec<DatasetRef>,
        options: PushMultiOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<PushResponse> {
        if options.recursive {
            unimplemented!("Recursive push is not yet supported")
        }
        if options.all {
            unimplemented!("Pushing all datasets is not yet supported")
        }

        let (plan, errors) = self
            .collect_plan(&dataset_refs, &options.remote_target)
            .await;
        if !errors.is_empty() {
            return errors;
        }

        let sync_results = self
            .sync_svc
            .sync_multi(
                plan.iter()
                    .map(|pi| SyncRequest {
                        src: pi.local_handle.as_any_ref(),
                        dst: (&pi.remote_target.url).into(),
                    })
                    .collect(),
                options.sync_options,
                sync_listener,
            )
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
                self.remote_alias_reg
                    .get_remote_aliases(&(push_item.local_handle.as_local_ref()))
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

        results
    }
}

#[derive(Debug)]
struct PushItem {
    local_handle: DatasetHandle,
    remote_target: RemoteTarget,
    push_target: Option<DatasetPushTarget>,
}

impl PushItem {
    fn as_response(&self, result: Result<SyncResult, SyncError>) -> PushResponse {
        PushResponse {
            local_handle: Some(self.local_handle.clone()),
            target: self.push_target.clone(),
            result: result.map_err(Into::into),
        }
    }
}
