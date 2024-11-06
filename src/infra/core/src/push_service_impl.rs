// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::*;

use crate::dataset_pushes::{DatasetPush, DatasetPushes};

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

// TODO: move to a separate entity
async fn read_pushes_info(dataset: Arc<dyn Dataset>) -> Result<DatasetPushes, InternalError> {
    match dataset.as_info_repo().get("pushes").await {
        Ok(bytes) => {
            let manifest: Manifest<DatasetPushes> = serde_yaml::from_slice(&bytes[..]).int_err()?;
            assert_eq!(manifest.kind, "DatasetPushes");
            Ok(manifest.content)
        }
        Err(GetNamedError::Internal(e)) => Err(e),
        Err(GetNamedError::Access(e)) => Err(e.int_err()),
        Err(GetNamedError::NotFound(_)) => Ok(DatasetPushes::default()),
    }
}

// TODO: move to a separate entity
async fn update_push_info(
    dataset: Arc<dyn Dataset>,
    push: &DatasetPush,
) -> Result<(), InternalError> {
    let prev = read_pushes_info(dataset.clone()).await?;
    let mut prev_pushes = prev.pushes.clone();
    prev_pushes.insert(push.target.clone(), push.clone());
    let updated = DatasetPushes {
        pushes: prev_pushes,
    };

    let manifest = Manifest {
        kind: "DatasetPushes".to_owned(),
        version: 1,
        content: updated,
    };
    let manifest_yaml = serde_yaml::to_string(&manifest).int_err()?;
    dataset
        .as_info_repo()
        .set("pushes", manifest_yaml.as_bytes())
        .await
        .int_err()?;
    Ok(())
}
// TODO: add remove record function (on repo/alias deletion)

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
            let pushed_at: DateTime<Utc> = Utc::now();
            for push_item in &plan {
                // TODO: Improve error handling
                let remote_target = (&push_item.remote_target.url).into();
                let dataset_ref = push_item.local_handle.as_local_ref();
                self.remote_alias_reg
                    .get_remote_aliases(&dataset_ref)
                    .await
                    .unwrap()
                    .add(&remote_target, RemoteAliasKind::Push)
                    .await
                    .unwrap();

                // Store last block hash of pushed data
                if let Ok(dataset) = self.dataset_repo.find_dataset_by_ref(&dataset_ref).await {
                    if let Ok(summary) = dataset.get_summary(GetSummaryOpts::default()).await {
                        let push = DatasetPush {
                            target: remote_target,
                            pushed_at,
                            head: summary.last_block_hash,
                        };
                        if let Err(err) = update_push_info(dataset, &push).await {
                            tracing::error!(error = ?err, error_msg = %err, "Failed to record push operation");
                        };
                    }
                };
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
