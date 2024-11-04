// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use opendatafabric::*;
use url::Url;

use crate::SyncRequestBuilder;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PullRequestPlannerImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
    transform_request_planner: Arc<dyn TransformRequestPlanner>,
    sync_request_builder: Arc<SyncRequestBuilder>,
    current_account_subject: Arc<CurrentAccountSubject>,
}

#[component(pub)]
#[interface(dyn PullRequestPlanner)]
impl PullRequestPlannerImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
        transform_request_planner: Arc<dyn TransformRequestPlanner>,
        sync_request_builder: Arc<SyncRequestBuilder>,
        current_account_subject: Arc<CurrentAccountSubject>,
    ) -> Self {
        Self {
            dataset_registry,
            remote_alias_registry,
            transform_request_planner,
            sync_request_builder,
            current_account_subject,
        }
    }

    // This function descends down the dependency tree of datasets (starting with
    // provided references) assigning depth index to every dataset in the
    // graph(s). Datasets that share the same depth level are independent and
    // can be pulled in parallel.
    async fn collect_pull_graph(
        &self,
        requests: &[PullRequest],
        options: &PullOptions,
        in_multi_tenant_mode: bool,
    ) -> (Vec<PullItem>, Vec<PullResponse>) {
        let mut errors = Vec::new();

        let mut depth_first_traversal = PullGraphDepthFirstTraversal::new(
            self.dataset_registry.clone(),
            self.remote_alias_registry.clone(),
            self.current_account_subject.clone(),
            options,
            in_multi_tenant_mode,
        );

        for pr in requests {
            match depth_first_traversal
                .traverse_pull_graph(
                    pr,   /* pull request */
                    true, /* referenced_explicitly */
                    true, /* scan dependent */
                )
                .await
            {
                Ok(_) => {}
                Err(e) => errors.push(PullResponse {
                    maybe_original_request: Some(pr.clone()),
                    maybe_local_ref: None,
                    maybe_remote_ref: None,
                    result: Err(e),
                }),
            }
        }

        let visited = depth_first_traversal.visited;
        let mut ordered = Vec::with_capacity(visited.len());
        ordered.extend(visited.into_values());
        ordered.sort();
        (ordered, errors)
    }

    async fn build_single_node_pull_graph(
        &self,
        request: &PullRequest,
        options: &PullOptions,
        in_multi_tenant_mode: bool,
    ) -> (Vec<PullItem>, Vec<PullResponse>) {
        let mut depth_first_traversal = PullGraphDepthFirstTraversal::new(
            self.dataset_registry.clone(),
            self.remote_alias_registry.clone(),
            self.current_account_subject.clone(),
            options,
            in_multi_tenant_mode,
        );

        if let Err(e) = depth_first_traversal
            .traverse_pull_graph(
                request, /* pull request */
                true,    /* referenced_explicitly */
                false,   /* scan dependent */
            )
            .await
        {
            let error_response = PullResponse {
                maybe_original_request: Some(request.clone()),
                maybe_local_ref: None,
                maybe_remote_ref: None,
                result: Err(e),
            };
            return (vec![], vec![error_response]);
        }

        let visited = depth_first_traversal.visited;
        assert_eq!(visited.len(), 1);
        let the_item = visited.into_values().next().unwrap();
        (vec![the_item], vec![])
    }

    fn slice(&self, mut plan: Vec<PullItem>) -> (i32, bool, Vec<PullItem>, Vec<PullItem>) {
        let first_depth = plan[0].depth;
        let first_is_remote = plan[0].maybe_remote_ref.is_some();

        let count = plan
            .iter()
            .take_while(|pi| {
                pi.depth == first_depth && pi.maybe_remote_ref.is_some() == first_is_remote
            })
            .count();

        let rest = plan.split_off(count);
        (first_depth, first_is_remote, plan, rest)
    }

    fn build_ingest_items(&self, pull_items: Vec<PullItem>) -> Vec<PullIngestItem> {
        pull_items
            .into_iter()
            .map(|pi| {
                assert!(pi.maybe_remote_ref.is_none());

                let hdl = match pi.local_target {
                    PullLocalTarget::Existing(local_handle) => local_handle,
                    PullLocalTarget::ToCreate(_) => {
                        unreachable!("Ingest flows expect to work with existing local targets")
                    }
                };

                PullIngestItem {
                    depth: pi.depth,
                    target: self.dataset_registry.get_resolved_dataset_by_handle(&hdl),
                    maybe_original_request: pi.maybe_original_request,
                }
            })
            .collect()
    }

    async fn build_transform_items(
        &self,
        pull_items: Vec<PullItem>,
        transform_options: &TransformOptions,
    ) -> Result<Vec<PullTransformItem>, Vec<PullResponse>> {
        let mut pull_transform_items = Vec::new();
        let mut errors = Vec::new();

        for pi in pull_items {
            assert!(pi.maybe_remote_ref.is_none());

            let hdl = match pi.local_target {
                PullLocalTarget::Existing(local_handle) => local_handle,
                PullLocalTarget::ToCreate(_) => {
                    unreachable!("Transform flows expect to work with existing local targets")
                }
            };

            let target = self.dataset_registry.get_resolved_dataset_by_handle(&hdl);

            match self
                .transform_request_planner
                .build_transform_plan(target.clone(), transform_options)
                .await
            {
                Ok(plan) => {
                    pull_transform_items.push(PullTransformItem {
                        depth: pi.depth,
                        target,
                        maybe_original_request: pi.maybe_original_request,
                        plan,
                    });
                }
                Err(e) => {
                    errors.push(PullResponse {
                        maybe_original_request: pi.maybe_original_request,
                        maybe_local_ref: Some(hdl.as_local_ref()),
                        maybe_remote_ref: None,
                        result: Err(PullError::TransformPlanError(e)),
                    });
                }
            }
        }

        if !errors.is_empty() {
            Err(errors)
        } else {
            Ok(pull_transform_items)
        }
    }

    async fn build_sync_items(
        &self,
        pull_items: Vec<PullItem>,
        sync_options: &SyncOptions,
    ) -> Result<(Vec<PullSyncItem>, Vec<SyncRequest>), Vec<PullResponse>> {
        let mut pull_sync_items = Vec::new();
        let mut sync_requests = Vec::new();
        let mut errors = Vec::new();

        for pi in pull_items {
            assert!(pi.maybe_remote_ref.is_some());

            let remote_ref = pi.maybe_remote_ref.unwrap();

            match self
                .sync_request_builder
                .build_sync_request(
                    remote_ref.as_any_ref(),
                    pi.local_target.as_any_ref(),
                    sync_options.create_if_not_exists,
                )
                .await
            {
                Ok(sync_request) => {
                    let psi = PullSyncItem {
                        depth: pi.depth,
                        local_target: pi.local_target,
                        remote_ref,
                        maybe_original_request: pi.maybe_original_request,
                    };
                    pull_sync_items.push(psi);
                    sync_requests.push(sync_request);
                }
                Err(e) => {
                    errors.push(PullResponse {
                        maybe_original_request: pi.maybe_original_request,
                        maybe_local_ref: Some(pi.local_target.as_local_ref()),
                        maybe_remote_ref: Some(remote_ref),
                        result: Err(PullError::SyncError(e)),
                    });
                }
            };
        }

        if !errors.is_empty() {
            Err(errors)
        } else {
            Ok((pull_sync_items, sync_requests))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PullRequestPlanner for PullRequestPlannerImpl {
    async fn build_pull_plan(
        &self,
        request: PullRequest,
        options: &PullOptions,
        in_multi_tenant_mode: bool,
    ) -> Result<PullPlanIterationJob, PullResponse> {
        let (mut plan, mut errors) = self
            .build_pull_multi_plan(&[request], options, in_multi_tenant_mode)
            .await;
        assert!(plan.len() == 1 && errors.is_empty() || plan.is_empty() && errors.len() == 1);
        if plan.is_empty() {
            let the_error = errors.remove(0);
            Err(the_error)
        } else {
            let the_job = plan.remove(0).job;
            Ok(the_job)
        }
    }

    async fn build_pull_multi_plan(
        &self,
        requests: &[PullRequest],
        options: &PullOptions,
        in_multi_tenant_mode: bool,
    ) -> (Vec<PullPlanIteration>, Vec<PullResponse>) {
        // If there is just 1 dataset, and no recursion set, do a simplified procedure.
        // Otherwise, do a hierarchical scan trying to find relations
        let (mut plan, errors) = if requests.len() == 1 && !options.recursive {
            self.build_single_node_pull_graph(&requests[0], options, in_multi_tenant_mode)
                .await
        } else {
            self.collect_pull_graph(requests, options, in_multi_tenant_mode)
                .await
        };

        tracing::info!(
            num_items = plan.len(),
            num_errors = errors.len(),
            ?plan,
            "Resolved pull graph"
        );
        if !errors.is_empty() {
            return (vec![], errors);
        }

        if !options.recursive {
            // Leave only datasets explicitly mentioned, preserving the depth order
            plan.retain(|pi| pi.maybe_original_request.is_some());
        }

        tracing::info!(num_items = plan.len(), ?plan, "Retained pull graph");

        let mut iterations = Vec::new();
        let mut errors = Vec::new();

        let mut rest = plan;
        while !rest.is_empty() {
            let (depth, is_remote, batch, tail) = self.slice(rest);
            rest = tail;

            if depth == 0 && !is_remote {
                iterations.push(PullPlanIteration {
                    depth,
                    job: PullPlanIterationJob::Ingest(self.build_ingest_items(batch)),
                });
            } else if depth == 0 && is_remote {
                match self.build_sync_items(batch, &options.sync_options).await {
                    Ok(items) => {
                        iterations.push(PullPlanIteration {
                            depth,
                            job: PullPlanIterationJob::Sync(items),
                        });
                    }
                    Err(sync_errors) => {
                        errors.extend(sync_errors);
                    }
                }
            } else {
                match self
                    .build_transform_items(batch, &options.transform_options)
                    .await
                {
                    Ok(items) => {
                        iterations.push(PullPlanIteration {
                            depth,
                            job: PullPlanIterationJob::Transform(items),
                        });
                    }
                    Err(transform_errors) => {
                        errors.extend(transform_errors);
                    }
                }
            };
        }

        (iterations, errors)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PullGraphDepthFirstTraversal<'a> {
    dataset_registry: Arc<dyn DatasetRegistry>,
    remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
    current_account_subject: Arc<CurrentAccountSubject>,
    options: &'a PullOptions,
    in_multi_tenant_mode: bool,
    visited: HashMap<DatasetAlias, PullItem>,
}

impl<'a> PullGraphDepthFirstTraversal<'a> {
    fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
        current_account_subject: Arc<CurrentAccountSubject>,
        options: &'a PullOptions,
        in_multi_tenant_mode: bool,
    ) -> Self {
        Self {
            dataset_registry,
            remote_alias_registry,
            current_account_subject,
            options,
            in_multi_tenant_mode,
            visited: HashMap::new(),
        }
    }

    #[async_recursion::async_recursion]
    async fn traverse_pull_graph(
        &mut self,
        request: &PullRequest,
        referenced_explicitly: bool,
        traverse_dependencies: bool,
    ) -> Result<i32, PullError> {
        tracing::debug!(?request, "Entering node");

        // Resolve local dataset handle, if dataset exists
        let maybe_local_handle = self.try_resolve_local_handle(request).await?;

        // If dataset is not found, and auto-create is disabled, it's en error
        if maybe_local_handle.is_none() && !self.options.sync_options.create_if_not_exists {
            return Err(PullError::InvalidOperation(
                "Dataset does not exist and auto-create is switched off".to_owned(),
            ));
        }

        // Resolve the name of a local dataset if it exists
        // or a name to create dataset with if syncing from remote and creation is
        // allowed
        let local_alias = self.form_local_alias(maybe_local_handle.as_ref(), request)?;

        // Already visited?
        if let Some(pi) = self.visited.get_mut(&local_alias) {
            tracing::debug!("Already visited - continuing");
            if referenced_explicitly {
                pi.maybe_original_request = Some(request.clone());
            }
            return Ok(pi.depth);
        }

        // Resolve remote alias, if any
        let maybe_remote_ref = self
            .resolve_remote_ref(request, maybe_local_handle.as_ref())
            .await?;

        let mut pull_item = if maybe_remote_ref.is_some() {
            // Datasets synced from remotes are depth 0
            let local_target = if let Some(local_handle) = maybe_local_handle {
                PullLocalTarget::existing(local_handle)
            } else {
                PullLocalTarget::to_create(local_alias.clone())
            };
            PullItem {
                maybe_original_request: None, // May be set below
                depth: 0,
                local_target,
                maybe_remote_ref,
            }
        } else {
            // Pulling an existing local root or derivative dataset
            let local_handle = maybe_local_handle.unwrap();

            // Read summary
            let summary = self
                .dataset_registry
                .get_dataset_by_handle(&local_handle)
                .get_summary(GetSummaryOpts::default())
                .await
                .int_err()?;

            // Plan up-stream dependencies first
            let max_dep_depth = if traverse_dependencies {
                self.traverse_upstream_datasets(summary).await?
            } else {
                // Without scanning upstreams, decide on depth based on Root/Derived kind.
                // The exact depth is not important, as long as we keep `depth=>0` for derived datasets.
                match summary.kind {
                    DatasetKind::Root => -1,
                    DatasetKind::Derivative => 0,
                }
            };

            // Plan the current dataset as last
            PullItem {
                maybe_original_request: None, // May be set below
                depth: max_dep_depth + 1,
                local_target: PullLocalTarget::existing(local_handle),
                maybe_remote_ref: None,
            }
        };

        if referenced_explicitly {
            pull_item.maybe_original_request = Some(request.clone());
        }

        tracing::debug!(?pull_item, "Resolved node");

        let depth = pull_item.depth;
        self.visited.insert(local_alias.clone(), pull_item);
        Ok(depth)
    }

    async fn try_resolve_local_handle(
        &self,
        request: &PullRequest,
    ) -> Result<Option<DatasetHandle>, PullError> {
        let maybe_local_handle = match request {
            PullRequest::Local(local_ref) => {
                self.dataset_registry
                    .try_resolve_dataset_handle_by_ref(local_ref)
                    .await?
            }
            PullRequest::Remote(remote) => {
                let maybe_local_handle = if let Some(local_alias) = &remote.maybe_local_alias {
                    self.dataset_registry
                        .try_resolve_dataset_handle_by_ref(&local_alias.as_local_ref())
                        .await?
                } else {
                    None
                };
                if maybe_local_handle.is_none() {
                    self.try_inverse_lookup_dataset_by_pull_alias(&remote.remote_ref)
                        .await?
                } else {
                    maybe_local_handle
                }
            }
        };

        Ok(maybe_local_handle)
    }

    // TODO: avoid traversing all datasets for every alias
    async fn try_inverse_lookup_dataset_by_pull_alias(
        &self,
        remote_ref: &DatasetRefRemote,
    ) -> Result<Option<DatasetHandle>, InternalError> {
        // Do a quick check when remote and local names match
        if let Some(remote_name) = remote_ref.dataset_name() {
            if let Some(local_handle) = self
                .dataset_registry
                .try_resolve_dataset_handle_by_ref(
                    &DatasetAlias::new(None, remote_name.clone()).as_local_ref(),
                )
                .await?
            {
                let dataset = self.dataset_registry.get_dataset_by_handle(&local_handle);
                if self
                    .remote_alias_registry
                    .get_remote_aliases(dataset)
                    .await
                    .int_err()?
                    .contains(remote_ref, RemoteAliasKind::Pull)
                {
                    return Ok(Some(local_handle));
                }
            }
        }

        // No luck - now have to search through aliases (of current user)
        if let CurrentAccountSubject::Logged(l) = self.current_account_subject.as_ref() {
            use tokio_stream::StreamExt;
            let mut datasets = self
                .dataset_registry
                .all_dataset_handles_by_owner(&l.account_name);
            while let Some(dataset_handle) = datasets.next().await {
                let dataset_handle = dataset_handle?;
                let dataset = self.dataset_registry.get_dataset_by_handle(&dataset_handle);

                if self
                    .remote_alias_registry
                    .get_remote_aliases(dataset)
                    .await
                    .int_err()?
                    .contains(remote_ref, RemoteAliasKind::Pull)
                {
                    return Ok(Some(dataset_handle));
                }
            }
        }

        Ok(None)
    }

    fn form_local_alias(
        &self,
        maybe_local_handle: Option<&DatasetHandle>,
        request: &PullRequest,
    ) -> Result<DatasetAlias, PullError> {
        let local_alias = if let Some(hdl) = maybe_local_handle {
            // Target exists
            hdl.alias.clone()
        } else {
            match request {
                PullRequest::Local(local_ref) => {
                    // Target does not exist but was provided
                    if let Some(alias) = local_ref.alias() {
                        alias.clone()
                    } else {
                        return Err(PullError::NotFound(DatasetNotFoundError {
                            dataset_ref: local_ref.clone(),
                        }));
                    }
                }
                PullRequest::Remote(remote) => {
                    if let Some(local_alias) = &remote.maybe_local_alias {
                        local_alias.clone()
                    } else {
                        self.infer_alias_from_remote_ref(&remote.remote_ref)?
                    }
                }
            }
        };

        Ok(local_alias)
    }

    fn infer_alias_from_remote_ref(
        &self,
        remote_ref: &DatasetRefRemote,
    ) -> Result<DatasetAlias, PullError> {
        Ok(match &remote_ref {
            DatasetRefRemote::ID(_, _) => {
                unimplemented!("Pulling from remote by ID is not supported")
            }

            DatasetRefRemote::Alias(alias)
            | DatasetRefRemote::Handle(DatasetHandleRemote { alias, .. }) => {
                DatasetAlias::new(None, alias.dataset_name.clone())
            }

            DatasetRefRemote::Url(url) => DatasetAlias::new(
                if self.in_multi_tenant_mode {
                    Some(self.current_account_subject.account_name().clone())
                } else {
                    None
                },
                self.infer_local_name_from_url(url)?,
            ),
        })
    }

    fn infer_local_name_from_url(&self, url: &Url) -> Result<DatasetName, PullError> {
        // Try to use last path segment for a name (ignoring the trailing slash)
        if let Some(path) = url.path_segments() {
            if let Some(last_segment) = path.rev().find(|s| !s.is_empty()) {
                if let Ok(name) = DatasetName::try_from(last_segment) {
                    return Ok(name);
                }
            }
        }
        // Fall back to using domain name
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

    async fn resolve_remote_ref(
        &self,
        request: &PullRequest,
        maybe_local_handle: Option<&DatasetHandle>,
    ) -> Result<Option<DatasetRefRemote>, PullError> {
        let remote_ref = if let PullRequest::Remote(remote) = request {
            Ok(Some(remote.remote_ref.clone()))
        } else if let Some(hdl) = &maybe_local_handle {
            self.resolve_pull_alias(hdl).await
        } else {
            Ok(None)
        }?;

        Ok(remote_ref)
    }

    async fn resolve_pull_alias(
        &self,
        hdl: &DatasetHandle,
    ) -> Result<Option<DatasetRefRemote>, PullError> {
        let dataset = self.dataset_registry.get_dataset_by_handle(hdl);

        let remote_aliases = match self.remote_alias_registry.get_remote_aliases(dataset).await {
            Ok(v) => Ok(v),
            Err(e) => match e {
                GetAliasesError::Internal(e) => Err(PullError::Internal(e)),
            },
        }?;

        let mut pull_aliases: Vec<_> = remote_aliases.get_by_kind(RemoteAliasKind::Pull).collect();

        match pull_aliases.len() {
            0 => Ok(None),
            1 => Ok(Some(pull_aliases.remove(0).clone())),
            _ => Err(PullError::AmbiguousSource),
        }
    }

    // TODO: consider using data from dependency graph
    async fn traverse_upstream_datasets(
        &mut self,
        summary: DatasetSummary
    ) -> Result<i32, PullError> {
        // TODO: EVO: Should be accounting for historical dependencies, not only current
        // ones?
        let mut max_dep_depth = -1;

        for dependency_id in summary.dependencies {
            tracing::debug!(%dependency_id, "Descending into dependency");

            let depth = self
                .traverse_pull_graph(
                    &PullRequest::local(dependency_id.as_local_ref()),
                    false,
                    true,
                )
                .await?;
            max_dep_depth = std::cmp::max(max_dep_depth, depth);
        }

        Ok(max_dep_depth)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
struct PullItem {
    depth: i32,
    local_target: PullLocalTarget,
    maybe_remote_ref: Option<DatasetRefRemote>,
    maybe_original_request: Option<PullRequest>,
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

        if self.maybe_remote_ref.is_some() != other.maybe_remote_ref.is_some() {
            return if self.maybe_remote_ref.is_some() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        self.local_target.alias().cmp(other.local_target.alias())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
