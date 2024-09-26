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
use internal_error::ResultIntoInternal;
use kamu_core::*;
use opendatafabric::*;
use serde_json::json;
use url::Url;

use crate::UrlExt;

pub struct PushServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    sync_svc: Arc<dyn SyncService>,
}

#[component(pub)]
#[interface(dyn PushService)]
impl PushServiceImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        sync_svc: Arc<dyn SyncService>,
    ) -> Self {
        Self {
            dataset_repo,
            remote_alias_reg,
            sync_svc,
        }
    }

    async fn collect_plan(
        &self,
        items: &Vec<PushRequest>,
        options: &PushMultiOptions,
    ) -> (Vec<PushItem>, Vec<PushResponse>) {
        let mut plan = Vec::new();
        let mut errors = Vec::new();

        for request in items {
            match self.collect_plan_item(request.clone(), options).await {
                Ok(item) => plan.push(item),
                Err(err) => errors.push(err),
            }
        }

        (plan, errors)
    }

    async fn collect_plan_item(
        &self,
        request: PushRequest,
        options: &PushMultiOptions,
    ) -> Result<PushItem, PushResponse> {
        // Resolve local dataset if we have a local reference
        let local_handle = if let Some(local_ref) = &request.local_ref {
            match self.dataset_repo.resolve_dataset_ref(local_ref).await {
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
                .resolve_push_alias(local_handle.as_ref().unwrap(), options)
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
        options: &PushMultiOptions,
    ) -> Result<DatasetRefRemote, PushError> {
        let remote_aliases = self
            .remote_alias_reg
            .get_remote_aliases(&local_handle.as_local_ref())
            .await
            .int_err()?;

        let mut push_aliases: Vec<_> = remote_aliases.get_by_kind(RemoteAliasKind::Push).collect();

        match push_aliases.len() {
            0 => {
                if let Some(remote_repo_opts) = &options.remote_repo_opts {
                    let push_dataset_name = if let Some(remote_dataset_name) = self
                        .resolve_remote_dataset_name(
                            &remote_repo_opts.remote_repo_url,
                            &local_handle.id,
                        )
                        .await?
                    {
                        remote_dataset_name
                    } else {
                        local_handle.alias.dataset_name.clone()
                    };
                    let mut res_url = remote_repo_opts
                        .remote_repo_url
                        .clone()
                        .as_odf_protoocol()
                        .unwrap();
                    if let Some(account_name) = &remote_repo_opts.remote_account_name {
                        res_url.path_segments_mut().unwrap().push(account_name);
                    }
                    res_url
                        .path_segments_mut()
                        .unwrap()
                        .push(&push_dataset_name);
                    return Ok(res_url.into());
                }
                Err(PushError::NoTarget)
            }
            1 => Ok(push_aliases.remove(0).clone()),
            _ => Err(PushError::AmbiguousTarget),
        }
    }

    async fn resolve_remote_dataset_name(
        &self,
        remote_server_url: &Url,
        dataset_id: &DatasetID,
    ) -> Result<Option<DatasetName>, PushError> {
        let client = reqwest::Client::new();
        let mut server_url = remote_server_url.clone(); // Clone the original URL to modify it
        server_url.path_segments_mut().unwrap().push("graphql");

        let gql_query = r#"
            query Datasets {
                datasets {
                    byId(datasetId: "{dataset_id}") {
                        name
                    }
                }
            }
            "#
        .replace("{dataset_id}", &dataset_id.to_string());

        let response = client
            .post(server_url)
            .json(&json!({"query": gql_query}))
            .send()
            .await
            .int_err()?
            .error_for_status()
            .int_err()?;

        let gql_response: serde_json::Value = response.json().await.int_err()?;

        if let Some(gql_dataset_name) = gql_response["data"]["datasets"]["byId"]["name"].as_str() {
            let dataset_name = DatasetName::try_from(gql_dataset_name).int_err()?;
            return Ok(Some(dataset_name));
        }
        Ok(None)
    }

    // TODO: avoid traversing all datasets for every alias
    async fn inverse_lookup_dataset_by_push_alias(
        &self,
        remote_ref: &DatasetRefRemote,
    ) -> Result<DatasetHandle, PushError> {
        // Do a quick check when remote and local names match
        if let Some(remote_name) = remote_ref.dataset_name() {
            if let Some(local_handle) = self
                .dataset_repo
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
                    .contains(remote_ref, RemoteAliasKind::Push)
                {
                    return Ok(local_handle);
                }
            }
        }

        // No luck - now have to search through aliases
        use tokio_stream::StreamExt;
        let mut datasets = self.dataset_repo.get_all_datasets();
        while let Some(dataset_handle) = datasets.next().await {
            let dataset_handle = dataset_handle?;

            if self
                .remote_alias_reg
                .get_remote_aliases(&dataset_handle.as_local_ref())
                .await
                .int_err()?
                .contains(remote_ref, RemoteAliasKind::Push)
            {
                return Ok(dataset_handle);
            }
        }
        Err(PushError::NoTarget)
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
                        remote_ref: Some(remote_ref),
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

        let (plan, errors) = self.collect_plan(&initial_requests, &options).await;
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
                    remote_ref: Some(remote_ref),
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
            remote_ref: Some(self.remote_ref),
            result: result.map_err(Into::into),
        }
    }
}
