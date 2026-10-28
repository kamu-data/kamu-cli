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
use internal_error::InternalError;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use kamu_datasets::{DatasetRegistry, DependencyGraphService};

use super::pull_graph_traversal::PullGraphDepthFirstTraversal;
use super::pull_item::PullItem;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PullRequestPlannerImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    pull_source_resolver: Arc<dyn DatasetPullSourceResolver>,
    update_kind_resolver: Arc<dyn DatasetUpdateKindResolver>,
    transform_planner: Arc<dyn TransformPlanner>,
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
        current_account_subject: Arc<CurrentAccountSubject>,
    ) -> Self {
        Self {
            dataset_registry,
            dependency_graph_service,
            pull_source_resolver,
            update_kind_resolver,
            transform_planner,
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
    fn build_ingest_item(pi: PullItem) -> PullIngestItem {
        let hdl = match pi.local_target {
            PullLocalTarget::Existing(local_handle) => local_handle,
            PullLocalTarget::ToCreate(_) => {
                unreachable!("Ingest flows expect to work with existing local targets")
            }
        };

        PullIngestItem {
            target: hdl,
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

    #[tracing::instrument(level = "debug", skip_all, fields(?pi))]
    fn build_sync_item(pi: PullItem, remote_ref: odf::DatasetRefRemote) -> PullSyncItem {
        PullSyncItem {
            local_target: pi.local_target,
            remote_ref,
            maybe_original_request: pi.maybe_original_request,
        }
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
                        let pii = Self::build_ingest_item(item);
                        tracing::debug!(depth, ?pii, "Added ingest item to pull plan");
                        jobs.push(PullPlanIterationJob::Ingest(pii));
                    }
                    DatasetUpdateKind::SyncFrom { remote_ref } => {
                        let psi = Self::build_sync_item(item, remote_ref);
                        tracing::debug!(depth, ?psi, "Added sync item to pull plan");
                        jobs.push(PullPlanIterationJob::Sync(psi));
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
