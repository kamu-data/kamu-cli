// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::*;

use chrono::prelude::*;
use dill::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, info_span};
use url::Url;

pub struct PullServiceImpl {
    local_repo: Arc<dyn DatasetRepository>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    ingest_svc: Arc<dyn IngestService>,
    transform_svc: Arc<dyn TransformService>,
    sync_svc: Arc<dyn SyncService>,
}

#[component(pub)]
impl PullServiceImpl {
    pub fn new(
        local_repo: Arc<dyn DatasetRepository>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        ingest_svc: Arc<dyn IngestService>,
        transform_svc: Arc<dyn TransformService>,
        sync_svc: Arc<dyn SyncService>,
    ) -> Self {
        Self {
            local_repo,
            remote_alias_reg,
            ingest_svc,
            transform_svc,
            sync_svc,
        }
    }

    // This function descends down the dependency tree of datasets (starting with provided references)
    // assigning depth index to every dataset in the graph(s).
    // Datasets that share the same depth level are independent and can be pulled in parallel.
    async fn collect_pull_graph(
        &self,
        requests: &Vec<PullRequest>,
        options: &PullOptions,
    ) -> (Vec<PullItem>, Vec<PullResponse>) {
        let mut visited = HashMap::new();
        let mut errors = Vec::new();

        for pr in requests {
            match self
                .collect_pull_graph_depth_first(pr, true, options, &mut visited)
                .await
            {
                Ok(_) => {}
                Err(e) => errors.push(PullResponse {
                    original_request: Some(pr.clone()),
                    local_ref: None,
                    remote_ref: None,
                    result: Err(e),
                }),
            }
        }

        let mut ordered = Vec::with_capacity(visited.len());
        ordered.extend(visited.into_values());
        ordered.sort();
        (ordered, errors)
    }

    #[async_recursion::async_recursion]
    async fn collect_pull_graph_depth_first(
        &self,
        request: &PullRequest,
        referenced_explicitly: bool,
        options: &PullOptions,
        visited: &mut HashMap<DatasetAlias, PullItem>,
    ) -> Result<i32, PullError> {
        debug!(?request, "Entering node");

        // Resolve local dataset if it exists
        let local_handle = if let Some(local_ref) = &request.local_ref {
            let local_handle = self.local_repo.try_resolve_dataset_ref(local_ref).await?;
            if local_handle.is_none() && request.remote_ref.is_none() {
                // Dataset does not exist locally nor remote ref was provided
                return Err(PullError::NotFound(DatasetNotFoundError {
                    dataset_ref: local_ref.clone(),
                }));
            }
            local_handle
        } else if let Some(remote_ref) = &request.remote_ref {
            self.try_inverse_lookup_dataset_by_pull_alias(remote_ref)
                .await?
        } else {
            panic!("Pull request must contain either local or remote reference")
        };

        // Resolve the name of a local dataset if it exists
        // or a name to create dataset with if syncing from remote and creation is allowed
        let local_alias = if let Some(hdl) = &local_handle {
            // Target exists
            hdl.alias.clone()
        } else if let Some(local_ref) = &request.local_ref {
            // Target does not exist but was provided
            if let Some(alias) = local_ref.alias() {
                alias.clone()
            } else {
                return Err(PullError::NotFound(DatasetNotFoundError {
                    dataset_ref: local_ref.clone(),
                }));
            }
        } else {
            // Infer target name from remote reference
            // TODO: Inferred name can already exist, should we care?
            match &request.remote_ref {
                Some(DatasetRefRemote::ID(_, _)) => {
                    unimplemented!("Pulling from remote by ID is not supported")
                }
                Some(
                    DatasetRefRemote::Alias(alias)
                    | DatasetRefRemote::Handle(DatasetHandleRemote { alias, .. }),
                ) => DatasetAlias::new(alias.account_name.clone(), alias.dataset_name.clone()),
                Some(DatasetRefRemote::Url(url)) => {
                    DatasetAlias::new(None, self.infer_local_name_from_url(url)?)
                }
                None => unreachable!(),
            }
        };

        if local_handle.is_none() && !options.sync_options.create_if_not_exists {
            return Err(PullError::InvalidOperation(
                "Dataset does not exist and auto-create is switched off".to_owned(),
            ));
        }

        // Already visited?
        if let Some(pi) = visited.get_mut(&local_alias) {
            debug!("Already visited - continuing");
            if referenced_explicitly {
                pi.original_request = Some(request.clone())
            }
            return Ok(pi.depth);
        }

        // Resolve remote alias, if any
        let remote_ref = if let Some(remote_ref) = &request.remote_ref {
            Ok(Some(remote_ref.clone()))
        } else if let Some(hdl) = &local_handle {
            self.resolve_pull_alias(&hdl.as_local_ref()).await
        } else {
            Ok(None)
        }?;

        let mut pull_item = if remote_ref.is_some() {
            // Datasets synced from remotes are depth 0
            PullItem {
                original_request: None,
                depth: 0,
                local_ref: local_handle
                    .map(|h| h.into())
                    .unwrap_or(local_alias.clone().into()),
                remote_ref,
            }
        } else {
            // Pulling an existing local root or derivative dataset
            let local_handle = local_handle.unwrap();

            let summary = self
                .local_repo
                .get_dataset(&local_handle.as_local_ref())
                .await
                .int_err()?
                .get_summary(GetSummaryOpts::default())
                .await
                .int_err()?;

            if summary.kind != DatasetKind::Root && request.ingest_from.is_some() {
                return Err(PullError::InvalidOperation(
                    "Cannot ingest data into a non-root dataset".to_owned(),
                ));
            }

            // TODO: EVO: Should be accounting for historical dependencies, not only current ones?
            let mut max_dep_depth = -1;

            for dep in summary.dependencies {
                let id = dep.id.unwrap();
                debug!(%id, name = %dep.name, "Descending into dependency");

                let depth = self
                    .collect_pull_graph_depth_first(
                        &PullRequest {
                            local_ref: Some(id.as_local_ref()),
                            remote_ref: None,
                            ingest_from: None,
                        },
                        false,
                        options,
                        visited,
                    )
                    .await?;
                max_dep_depth = std::cmp::max(max_dep_depth, depth);
            }

            PullItem {
                original_request: None,
                depth: max_dep_depth + 1,
                local_ref: local_handle.into(),
                remote_ref: None,
            }
        };

        if referenced_explicitly {
            pull_item.original_request = Some(request.clone());
        }

        debug!(?pull_item, "Resolved node");

        let depth = pull_item.depth;
        visited.insert(local_alias.clone(), pull_item);
        Ok(depth)
    }

    // TODO: avoid traversing all datasets for every alias
    async fn try_inverse_lookup_dataset_by_pull_alias(
        &self,
        remote_ref: &DatasetRefRemote,
    ) -> Result<Option<DatasetHandle>, InternalError> {
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
                    .contains(&remote_ref, RemoteAliasKind::Pull)
                {
                    return Ok(Some(local_handle));
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
                .contains(&remote_ref, RemoteAliasKind::Pull)
            {
                return Ok(Some(dataset_handle));
            }
        }
        Ok(None)
    }

    async fn resolve_pull_alias(
        &self,
        local_ref: &DatasetRef,
    ) -> Result<Option<DatasetRefRemote>, PullError> {
        let remote_aliases = match self.remote_alias_reg.get_remote_aliases(local_ref).await {
            Ok(v) => Ok(v),
            Err(GetAliasesError::DatasetNotFound(e)) => Err(PullError::NotFound(e)),
            Err(e) => Err(e.int_err().into()),
        }?;

        let mut pull_aliases: Vec<_> = remote_aliases.get_by_kind(RemoteAliasKind::Pull).collect();

        match pull_aliases.len() {
            0 => Ok(None),
            1 => Ok(Some(pull_aliases.remove(0).clone())),
            _ => Err(PullError::AmbiguousSource),
        }
    }

    fn infer_local_name_from_url(&self, url: &Url) -> Result<DatasetName, PullError> {
        if let Some(last_segment) = url.path_segments().and_then(|s| s.rev().next()) {
            if let Ok(name) = DatasetName::try_from(last_segment) {
                return Ok(name);
            }
        }
        if let Some(url::Host::Domain(host)) = url.host() {
            if let Ok(name) = DatasetName::try_from(host) {
                return Ok(name);
            }
        }
        Err(PullError::InvalidOperation(
            "Unable to infer local name from remote URL, please specify the destination explicitly"
                .to_owned(),
        ))
    }

    fn slice<'a>(&self, to_slice: &'a [PullItem]) -> (i32, bool, &'a [PullItem], &'a [PullItem]) {
        let first = &to_slice[0];
        let count = to_slice
            .iter()
            .take_while(|pi| {
                pi.depth == first.depth && pi.remote_ref.is_some() == first.remote_ref.is_some()
            })
            .count();
        (
            first.depth,
            first.remote_ref.is_some(),
            &to_slice[..count],
            &to_slice[count..],
        )
    }

    async fn ingest_multi(
        &self,
        batch: &[PullItem], // TODO: Move to avoid cloning
        options: &PullOptions,
        listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let ingest_results = self
            .ingest_svc
            .ingest_multi_ext(
                &mut batch.iter().map(|pi| IngestRequest {
                    dataset_ref: pi.local_ref.clone(),
                    fetch_override: pi
                        .original_request
                        .as_ref()
                        .and_then(|r| r.ingest_from.clone()),
                }),
                options.ingest_options.clone(),
                listener,
            )
            .await;

        assert_eq!(batch.len(), ingest_results.len());

        Ok(std::iter::zip(batch, ingest_results)
            .map(|(pi, res)| {
                assert_eq!(pi.local_ref, res.0);
                pi.clone().into_response_ingest(res)
            })
            .collect())
    }

    async fn sync_multi(
        &self,
        batch: &[PullItem], // TODO: Move to avoid cloning
        options: &PullOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let sync_results = self
            .sync_svc
            .sync_multi(
                &mut batch.iter().map(|pi| {
                    (
                        pi.remote_ref.as_ref().unwrap().into(),
                        pi.local_ref.as_any_ref(),
                    )
                }),
                options.sync_options.clone(),
                listener,
            )
            .await;

        assert_eq!(batch.len(), sync_results.len());

        let results: Vec<_> = std::iter::zip(batch, sync_results)
            .map(|(pi, res)| {
                assert_eq!(pi.local_ref.as_any_ref(), res.dst);
                pi.clone().into_response_sync(res)
            })
            .collect();

        // Associate newly-synced datasets with remotes
        if options.add_aliases {
            for res in &results {
                if let Ok(PullResult::Updated { old_head: None, .. }) = res.result {
                    if let Some(remote_ref) = &res.remote_ref {
                        self.remote_alias_reg
                            .get_remote_aliases(res.local_ref.as_ref().unwrap())
                            .await
                            .int_err()?
                            .add(&remote_ref, RemoteAliasKind::Pull)
                            .int_err()?;
                    }
                }
            }
        }

        Ok(results)
    }

    async fn transform_multi(
        &self,
        batch: &[PullItem], // TODO: Move to avoid cloning
        _options: &PullOptions,
        listener: Option<Arc<dyn TransformMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let transform_results = self
            .transform_svc
            .transform_multi(&mut batch.iter().map(|pi| pi.local_ref.clone()), listener)
            .await;

        assert_eq!(batch.len(), transform_results.len());

        Ok(std::iter::zip(batch, transform_results)
            .map(|(pi, res)| {
                assert_eq!(pi.local_ref, res.0);
                pi.clone().into_response_transform(res)
            })
            .collect())
    }
}

#[async_trait::async_trait(?Send)]
impl PullService for PullServiceImpl {
    async fn pull_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefAny>,
        options: PullOptions,
        ingest_listener: Option<Arc<dyn IngestMultiListener>>,
        transform_listener: Option<Arc<dyn TransformMultiListener>>,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let mut requests = dataset_refs.map(|r| {
            // TODO: Support local multi-tenancy
            match r.as_local_single_tenant_ref() {
                Ok(local_ref) => PullRequest {
                    local_ref: Some(local_ref),
                    remote_ref: None,
                    ingest_from: None,
                },
                Err(remote_ref) => PullRequest {
                    local_ref: None,
                    remote_ref: Some(remote_ref),
                    ingest_from: None,
                },
            }
        });

        self.pull_multi_ext(
            &mut requests,
            options,
            ingest_listener,
            transform_listener,
            sync_listener,
        )
        .await
    }

    async fn pull_multi_ext(
        &self,
        requests: &mut dyn Iterator<Item = PullRequest>,
        options: PullOptions,
        ingest_listener: Option<Arc<dyn IngestMultiListener>>,
        transform_listener: Option<Arc<dyn TransformMultiListener>>,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let requests: Vec<_> = if !options.all {
            requests.collect()
        } else {
            use futures::TryStreamExt;
            self.local_repo
                .get_all_datasets()
                .map_ok(|hdl| PullRequest {
                    local_ref: Some(hdl.into()),
                    remote_ref: None,
                    ingest_from: None,
                })
                .try_collect()
                .await?
        };

        let span = info_span!("Pull");
        let _span_guard = span.enter();
        info!(?requests, ?options, "Performing pull");

        let (mut plan, errors) = self.collect_pull_graph(&requests, &options).await;
        info!(
            num_items = plan.len(),
            num_errors = errors.len(),
            ?plan,
            "Resolved pull plan"
        );
        if !errors.is_empty() {
            return Ok(errors);
        }

        if !(options.recursive || options.all) {
            // Leave only datasets explicitly mentioned, preserving the depth order
            plan.retain(|pi| pi.original_request.is_some());
        }

        info!(num_items = plan.len(), "Retained pull plan");

        let mut results = Vec::with_capacity(plan.len());

        let mut rest = &plan[..];
        while !rest.is_empty() {
            let (depth, is_remote, batch, tail) = self.slice(rest);
            rest = tail;

            let results_level: Vec<_> = if depth == 0 && !is_remote {
                info!(%depth, ?batch, "Running ingest batch");
                self.ingest_multi(batch, &options, ingest_listener.clone())
                    .await?
            } else if depth == 0 && is_remote {
                info!(%depth, ?batch, "Running sync batch");
                self.sync_multi(batch, &options, sync_listener.clone())
                    .await?
            } else {
                info!(%depth, ?batch, "Running transform batch");
                self.transform_multi(batch, &options, transform_listener.clone())
                    .await?
            };

            let errors = results_level.iter().any(|r| r.result.is_err());
            results.extend(results_level);
            if errors {
                break;
            }
        }

        Ok(results)
    }

    async fn set_watermark(
        &self,
        dataset_ref: &DatasetRef,
        watermark: DateTime<Utc>,
    ) -> Result<PullResult, SetWatermarkError> {
        let aliases = match self.remote_alias_reg.get_remote_aliases(dataset_ref).await {
            Ok(v) => Ok(v),
            Err(GetAliasesError::DatasetNotFound(e)) => Err(SetWatermarkError::NotFound(e)),
            Err(GetAliasesError::Internal(e)) => Err(SetWatermarkError::Internal(e)),
        }?;

        if !aliases.is_empty(RemoteAliasKind::Pull) {
            return Err(SetWatermarkError::IsRemote);
        }

        let dataset = self.local_repo.get_dataset(dataset_ref).await?;
        let chain = dataset.as_metadata_chain();

        if let Some(last_watermark) = chain
            .iter_blocks()
            .filter_data_stream_blocks()
            .filter_map_ok(|(_, b)| b.event.output_watermark)
            .try_first()
            .await
            .int_err()?
        {
            if last_watermark >= watermark {
                return Ok(PullResult::UpToDate);
            }
        }

        let commit_result = dataset
            .commit_event(
                MetadataEvent::SetWatermark(SetWatermark {
                    output_watermark: watermark,
                }),
                CommitOpts::default(),
            )
            .await
            .int_err()?;

        Ok(PullResult::Updated {
            old_head: commit_result.old_head,
            new_head: commit_result.new_head,
            num_blocks: 1,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
struct PullItem {
    depth: i32,
    local_ref: DatasetRef,
    remote_ref: Option<DatasetRefRemote>,
    original_request: Option<PullRequest>,
}

impl PullItem {
    fn into_response_ingest(
        self,
        r: (DatasetRef, Result<IngestResult, IngestError>),
    ) -> PullResponse {
        PullResponse {
            original_request: self.original_request,
            local_ref: Some(r.0),
            remote_ref: None,
            result: match r.1 {
                Ok(r) => Ok(r.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn into_response_sync(self, r: SyncResultMulti) -> PullResponse {
        // TODO: Support local multi-tenancy
        PullResponse {
            original_request: self.original_request,
            local_ref: r.dst.as_local_ref(|_| true).ok(),
            remote_ref: r.src.as_remote_ref(|_| true).ok(),
            result: match r.result {
                Ok(r) => Ok(r.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn into_response_transform(
        self,
        r: (DatasetRef, Result<TransformResult, TransformError>),
    ) -> PullResponse {
        PullResponse {
            original_request: self.original_request,
            local_ref: Some(r.0),
            remote_ref: None,
            result: match r.1 {
                Ok(r) => Ok(r.into()),
                Err(e) => Err(e.into()),
            },
        }
    }
}

impl PartialOrd for PullItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PullItem {
    fn cmp(&self, other: &Self) -> Ordering {
        let depth_ord = self.depth.cmp(&other.depth);
        if depth_ord != Ordering::Equal {
            return depth_ord;
        }

        if self.remote_ref.is_some() != other.remote_ref.is_some() {
            return if self.remote_ref.is_some() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        match (self.local_ref.alias(), other.local_ref.alias()) {
            (Some(lhs), Some(rhs)) => lhs.cmp(rhs),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            _ => Ordering::Equal,
        }
    }
}
