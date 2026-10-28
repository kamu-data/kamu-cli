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
use internal_error::InternalError;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt, DependencyGraphService};
use url::Url;

use crate::SyncRequestBuilder;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PullRequestPlannerImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    pull_source_resolver: Arc<dyn DatasetPullSourceResolver>,
    update_kind_resolver: Arc<dyn DatasetUpdateKindResolver>,
    transform_planner: Arc<dyn TransformPlanner>,
    sync_request_builder: Arc<SyncRequestBuilder>,
    current_account_subject: Arc<CurrentAccountSubject>,
}

#[component(pub)]
#[interface(dyn PullRequestPlanner)]
impl PullRequestPlannerImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        pull_source_resolver: Arc<dyn DatasetPullSourceResolver>,
        update_kind_resolver: Arc<dyn DatasetUpdateKindResolver>,
        transform_planner: Arc<dyn TransformPlanner>,
        sync_request_builder: Arc<SyncRequestBuilder>,
        current_account_subject: Arc<CurrentAccountSubject>,
    ) -> Self {
        Self {
            dataset_registry,
            dependency_graph_service,
            pull_source_resolver,
            update_kind_resolver,
            transform_planner,
            sync_request_builder,
            current_account_subject,
        }
    }

    // This function descends down the dependency tree of datasets (starting with
    // provided references) assigning depth index to every dataset in the
    // graph(s). Datasets that share the same depth level are independent and
    // can be pulled in parallel.
    #[tracing::instrument(level = "debug", skip_all, fields(?requests, ?options))]
    async fn collect_pull_graph(
        &self,
        requests: &[PullRequest],
        options: &PullOptions,
        tenancy_config: TenancyConfig,
    ) -> (Vec<PullItem>, Vec<PullResponse>) {
        let mut errors = Vec::new();

        let mut depth_first_traversal = PullGraphDepthFirstTraversal::new(
            self.dataset_registry.clone(),
            self.dependency_graph_service.clone(),
            self.pull_source_resolver.clone(),
            self.update_kind_resolver.clone(),
            self.current_account_subject.clone(),
            options,
            tenancy_config,
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

    #[tracing::instrument(level = "debug", skip_all, fields(?request, ?options))]
    async fn build_single_node_pull_graph(
        &self,
        request: &PullRequest,
        options: &PullOptions,
        tenancy_config: TenancyConfig,
    ) -> (Vec<PullItem>, Vec<PullResponse>) {
        let mut depth_first_traversal = PullGraphDepthFirstTraversal::new(
            self.dataset_registry.clone(),
            self.dependency_graph_service.clone(),
            self.pull_source_resolver.clone(),
            self.update_kind_resolver.clone(),
            self.current_account_subject.clone(),
            options,
            tenancy_config,
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

    fn slice_by_depth(&self, mut plan: Vec<PullItem>) -> (i32, Vec<PullItem>, Vec<PullItem>) {
        let first_depth = plan[0].depth;
        let count = plan.iter().take_while(|pi| pi.depth == first_depth).count();
        let rest = plan.split_off(count);
        (first_depth, plan, rest)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?pi))]
    async fn build_ingest_item(&self, pi: PullItem) -> PullIngestItem {
        let hdl = match pi.local_target {
            PullLocalTarget::Existing(local_handle) => local_handle,
            PullLocalTarget::ToCreate(_) => {
                unreachable!("Ingest flows expect to work with existing local targets")
            }
        };

        let target = self.dataset_registry.get_dataset_by_handle(&hdl).await;

        PullIngestItem {
            depth: pi.depth,
            target,
            maybe_original_request: pi.maybe_original_request,
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?pi))]
    async fn build_transform_item(
        &self,
        pi: PullItem,
    ) -> Result<PullTransformItem, Box<PullResponse>> {
        let hdl = match pi.local_target {
            PullLocalTarget::Existing(local_handle) => local_handle,
            PullLocalTarget::ToCreate(_) => {
                unreachable!("Transform flows expect to work with existing local targets")
            }
        };

        let target = self.dataset_registry.get_dataset_by_handle(&hdl).await;

        // Fails a pull with a broken transform before any job runs, and learns the
        // inputs the pull reads
        match self
            .transform_planner
            .describe_transform(target.clone())
            .await
        {
            Ok(description) => Ok(PullTransformItem {
                depth: pi.depth,
                input_handles: description.inputs,
                target,
                maybe_original_request: pi.maybe_original_request,
            }),
            Err(e) => Err(Box::new(PullResponse {
                maybe_original_request: pi.maybe_original_request,
                maybe_local_ref: Some(hdl.as_local_ref()),
                maybe_remote_ref: None,
                result: Err(PullError::TransformError(TransformError::Plan(e))),
            })),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?pi, ?sync_options))]
    async fn build_sync_item(
        &self,
        pi: PullItem,
        remote_ref: odf::DatasetRefRemote,
        sync_options: SyncOptions,
    ) -> Result<PullSyncItem, Box<PullResponse>> {
        match self
            .build_sync_request(&remote_ref, &pi.local_target, sync_options)
            .await
        {
            Ok(sync_request) => Ok(PullSyncItem {
                depth: pi.depth,
                local_target: pi.local_target,
                remote_ref,
                maybe_original_request: pi.maybe_original_request,
                sync_request: Box::new(sync_request),
            }),
            Err(e) => Err(Box::new(PullResponse {
                maybe_original_request: pi.maybe_original_request,
                maybe_local_ref: Some(pi.local_target.as_local_ref()),
                maybe_remote_ref: Some(remote_ref),
                result: Err(PullError::SyncError(e)),
            })),
        }
    }

    async fn build_sync_request(
        &self,
        remote_ref: &odf::DatasetRefRemote,
        local_target: &PullLocalTarget,
        sync_options: SyncOptions,
    ) -> Result<LocalSyncRequest, SyncError> {
        let source = self
            .pull_source_resolver
            .resolve_remote_source(remote_ref)
            .await?;
        self.sync_request_builder
            .build_local_sync_request_from_source(
                source,
                local_target.as_local_ref(),
                sync_options.create_if_not_exists,
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PullRequestPlanner for PullRequestPlannerImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(?request, ?options))]
    async fn build_pull_plan(
        &self,
        request: PullRequest,
        options: &PullOptions,
        tenancy_config: TenancyConfig,
    ) -> Result<PullPlanIterationJob, PullResponse> {
        assert!(!options.recursive);

        let (mut plan, mut errors) = self
            .build_pull_multi_plan(&[request], options, tenancy_config)
            .await;
        assert!(plan.len() == 1 && errors.is_empty() || plan.is_empty() && errors.len() == 1);
        if plan.is_empty() {
            let the_error = errors.remove(0);
            Err(the_error)
        } else {
            let mut the_iteration = plan.remove(0).jobs;
            assert_eq!(the_iteration.len(), 1);
            let the_job = the_iteration.remove(0);
            Ok(the_job)
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?requests, ?options))]
    async fn build_pull_multi_plan(
        &self,
        requests: &[PullRequest],
        options: &PullOptions,
        tenancy_config: TenancyConfig,
    ) -> (Vec<PullPlanIteration>, Vec<PullResponse>) {
        // If there is just 1 dataset, and no recursion set, do a simplified procedure.
        // Otherwise, do a hierarchical scan trying to find relations
        let (mut plan, errors) = if requests.len() == 1 && !options.recursive {
            self.build_single_node_pull_graph(&requests[0], options, tenancy_config)
                .await
        } else {
            self.collect_pull_graph(requests, options, tenancy_config)
                .await
        };

        tracing::info!(
            num_iterations = plan.len(),
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
            let (depth, batch, tail) = self.slice_by_depth(rest);
            rest = tail;

            tracing::debug!(
                depth,
                num_items = batch.len(),
                ?batch,
                "Detailing pull graph iteration"
            );

            let mut jobs = Vec::new();
            for item in batch {
                match item.kind.clone() {
                    DatasetUpdateKind::PollingIngest => {
                        let pii = self.build_ingest_item(item).await;
                        tracing::debug!(depth, ?pii, "Added ingest item to pull plan");
                        jobs.push(PullPlanIterationJob::Ingest(pii));
                    }
                    DatasetUpdateKind::SyncFrom { remote_ref } => {
                        match self
                            .build_sync_item(item, remote_ref, options.sync_options)
                            .await
                        {
                            Ok(psi) => {
                                tracing::debug!(depth, ?psi, "Added sync item to pull plan");
                                jobs.push(PullPlanIterationJob::Sync(psi));
                            }
                            Err(sync_error) => {
                                errors.push(*sync_error);
                            }
                        }
                    }
                    DatasetUpdateKind::Transform => match self.build_transform_item(item).await {
                        Ok(pti) => {
                            tracing::debug!(depth, ?pti, "Added transform item to pull plan");
                            jobs.push(PullPlanIterationJob::Transform(pti));
                        }
                        Err(transform_error) => {
                            errors.push(*transform_error);
                        }
                    },
                }
            }

            if !jobs.is_empty() {
                iterations.push(PullPlanIteration { depth, jobs });
            }
        }

        (iterations, errors)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?options))]
    async fn build_pull_plan_all_owner_datasets(
        &self,
        options: &PullOptions,
        tenancy_config: TenancyConfig,
    ) -> Result<(Vec<PullPlanIteration>, Vec<PullResponse>), InternalError> {
        use futures::TryStreamExt;
        let requests: Vec<_> = self
            .dataset_registry
            .all_dataset_handles_by_owner_name(self.current_account_subject.account_name())
            .map_ok(|hdl| PullRequest::local(hdl.as_local_ref()))
            .try_collect()
            .await?;

        Ok(self
            .build_pull_multi_plan(&requests, options, tenancy_config)
            .await)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PullGraphDepthFirstTraversal<'a> {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    pull_source_resolver: Arc<dyn DatasetPullSourceResolver>,
    update_kind_resolver: Arc<dyn DatasetUpdateKindResolver>,
    current_account_subject: Arc<CurrentAccountSubject>,
    options: &'a PullOptions,
    tenancy_config: TenancyConfig,
    visited: HashMap<odf::DatasetAlias, PullItem>,
}

impl<'a> PullGraphDepthFirstTraversal<'a> {
    fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        pull_source_resolver: Arc<dyn DatasetPullSourceResolver>,
        update_kind_resolver: Arc<dyn DatasetUpdateKindResolver>,
        current_account_subject: Arc<CurrentAccountSubject>,
        options: &'a PullOptions,
        tenancy_config: TenancyConfig,
    ) -> Self {
        Self {
            dataset_registry,
            dependency_graph_service,
            pull_source_resolver,
            update_kind_resolver,
            current_account_subject,
            options,
            tenancy_config,
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
        tracing::debug!(?request, %referenced_explicitly, %traverse_dependencies, "Entering pull graph node");

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

        let kind = self
            .resolve_item_kind(request, maybe_local_handle.as_ref())
            .await?;

        let depth = match &kind {
            // Synced datasets and roots do not depend on other local datasets
            DatasetUpdateKind::SyncFrom { .. } | DatasetUpdateKind::PollingIngest => 0,
            DatasetUpdateKind::Transform => {
                if traverse_dependencies {
                    let local_handle = maybe_local_handle.as_ref().unwrap();
                    self.traverse_upstream_datasets(&local_handle.id).await? + 1
                } else {
                    // Without scanning upstreams the exact depth is not important, as long as
                    // derivatives come after roots
                    1
                }
            }
        };

        let local_target = if let Some(local_handle) = maybe_local_handle {
            PullLocalTarget::existing(local_handle)
        } else {
            PullLocalTarget::to_create(local_alias.clone())
        };

        let mut pull_item = PullItem {
            maybe_original_request: None, // May be set below
            depth,
            local_target,
            kind,
        };

        if referenced_explicitly {
            pull_item.maybe_original_request = Some(request.clone());
        }

        tracing::debug!(?pull_item, "Resolved pull graph node");

        let depth = pull_item.depth;
        self.visited.insert(local_alias.clone(), pull_item);
        Ok(depth)
    }

    async fn try_resolve_local_handle(
        &self,
        request: &PullRequest,
    ) -> Result<Option<odf::DatasetHandle>, PullError> {
        let maybe_local_handle = match request {
            PullRequest::Local(local_ref) => {
                match self
                    .dataset_registry
                    .try_resolve_dataset_handle_by_ref(local_ref)
                    .await?
                {
                    Some(hdl) => Some(hdl),
                    None => {
                        return Err(PullError::NotFound(odf::DatasetNotFoundError {
                            dataset_ref: local_ref.clone(),
                        }));
                    }
                }
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
                    let maybe_resolved_local_handle = self
                        .pull_source_resolver
                        .find_dataset_by_pull_alias(&remote.remote_ref)
                        .await?;
                    if let Some(resolved_local_handle) = &maybe_resolved_local_handle {
                        // Ensure new alias is identical to the old one
                        if let Some(local_alias) = &remote.maybe_local_alias
                            && resolved_local_handle.alias != *local_alias
                        {
                            return Err(PullError::SaveUnderDifferentAlias(
                                resolved_local_handle.alias.to_string(),
                            ));
                        }
                        maybe_resolved_local_handle
                    } else {
                        None
                    }
                } else {
                    maybe_local_handle
                }
            }
        };

        Ok(maybe_local_handle)
    }

    fn form_local_alias(
        &self,
        maybe_local_handle: Option<&odf::DatasetHandle>,
        request: &PullRequest,
    ) -> Result<odf::DatasetAlias, PullError> {
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
                        return Err(PullError::NotFound(odf::DatasetNotFoundError {
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
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<odf::DatasetAlias, PullError> {
        Ok(match &remote_ref {
            odf::DatasetRefRemote::ID(_, _) => {
                return Err(PullError::InvalidOperation(
                    "Unable to infer local name from remote dataset ID, please specify the \
                     destination explicitly"
                        .to_owned(),
                ));
            }

            odf::DatasetRefRemote::Alias(alias)
            | odf::DatasetRefRemote::Handle(odf::metadata::DatasetHandleRemote { alias, .. }) => {
                odf::DatasetAlias::new(None, alias.dataset_name.clone())
            }

            odf::DatasetRefRemote::Url(url) => odf::DatasetAlias::new(
                if self.tenancy_config == TenancyConfig::MultiTenant {
                    Some(self.current_account_subject.account_name().clone())
                } else {
                    None
                },
                self.infer_local_name_from_url(url)?,
            ),
        })
    }

    fn infer_local_name_from_url(&self, url: &Url) -> Result<odf::DatasetName, PullError> {
        // Try to use last path segment for a name (ignoring the trailing slash)
        if let Some(path) = url.path_segments()
            && let Some(last_segment) = path.rev().find(|s| !s.is_empty())
            && let Ok(name) = odf::DatasetName::try_from(last_segment)
        {
            return Ok(name);
        }
        // Fall back to using domain name
        if let Some(url::Host::Domain(host)) = url.host()
            && let Ok(name) = odf::DatasetName::try_from(host)
        {
            return Ok(name);
        }
        Err(PullError::InvalidOperation(
            "Unable to infer local name from remote URL, please specify the destination explicitly"
                .to_owned(),
        ))
    }

    async fn resolve_item_kind(
        &self,
        request: &PullRequest,
        maybe_local_handle: Option<&odf::DatasetHandle>,
    ) -> Result<DatasetUpdateKind, PullError> {
        match request {
            PullRequest::Remote(remote) => Ok(DatasetUpdateKind::SyncFrom {
                remote_ref: remote.remote_ref.clone(),
            }),
            PullRequest::Local(_) => {
                let Some(local_handle) = maybe_local_handle else {
                    unreachable!("Local pull requests are resolved to existing datasets")
                };
                Ok(self
                    .update_kind_resolver
                    .resolve_update_kind(local_handle)
                    .await?)
            }
        }
    }

    async fn traverse_upstream_datasets(
        &mut self,
        dataset_id: &odf::DatasetID,
    ) -> Result<i32, PullError> {
        // TODO: EVO: Should be accounting for historical dependencies, not only current
        // ones?
        let mut max_dep_depth = -1;

        use tokio_stream::StreamExt;
        let upstream_dependencies = self
            .dependency_graph_service
            .get_upstream_dependencies(dataset_id)
            .await
            .collect::<Vec<_>>()
            .await;

        for dependency_id in upstream_dependencies {
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
    kind: DatasetUpdateKind,
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

        if self.kind.is_sync_from() != other.kind.is_sync_from() {
            return if self.kind.is_sync_from() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        self.local_target.alias().cmp(other.local_target.alias())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
