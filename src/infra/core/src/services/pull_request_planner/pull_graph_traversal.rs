// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use kamu_core::*;
use kamu_datasets::DependencyGraphService;
use tokio_stream::StreamExt;

use super::pull_item::{PullItem, PullItemJob};
use super::pull_target_resolution::{PullTarget, PullTargetResolver};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) struct PullGraphDepthFirstTraversal<'a> {
    target_resolver: PullTargetResolver<'a>,
    dependency_graph_service: &'a dyn DependencyGraphService,
    update_kind_resolver: &'a dyn DatasetUpdateKindResolver,
    recursive: bool,
    requested: HashMap<odf::DatasetAlias, PullRequest>,
    visited: HashMap<odf::DatasetAlias, PullItem>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'a> PullGraphDepthFirstTraversal<'a> {
    pub(super) fn new(
        target_resolver: PullTargetResolver<'a>,
        dependency_graph_service: &'a dyn DependencyGraphService,
        update_kind_resolver: &'a dyn DatasetUpdateKindResolver,
        recursive: bool,
    ) -> Self {
        Self {
            target_resolver,
            dependency_graph_service,
            update_kind_resolver,
            recursive,
            requested: HashMap::new(),
            visited: HashMap::new(),
        }
    }

    /// Resolves the requested datasets up front, so an upstream that is also
    /// requested is planned from its request, whatever the order of requests
    pub(super) async fn register_requests(&mut self, requests: &[PullRequest]) {
        for request in requests {
            // A request that cannot be resolved fails when it is traversed
            if let Ok(target) = self.target_resolver.resolve(request).await {
                self.requested
                    .entry(target.local_alias().clone())
                    .or_insert_with(|| request.clone());
            }
        }
    }

    /// The items of every dataset visited so far, in no particular order
    pub(super) fn into_items(self) -> Vec<PullItem> {
        self.visited.into_values().collect()
    }

    #[async_recursion::async_recursion]
    pub(super) async fn traverse_pull_graph(
        &mut self,
        request: &PullRequest,
        referenced_explicitly: bool,
        traverse_dependencies: bool,
    ) -> Result<i32, PullError> {
        tracing::debug!(?request, %referenced_explicitly, %traverse_dependencies, "Entering pull graph node");

        let target = self.target_resolver.resolve(request).await?;
        let local_alias = target.local_alias().clone();

        if let Some(pi) = self.visited.get_mut(&local_alias) {
            tracing::debug!("Already visited - continuing");
            if referenced_explicitly {
                pi.maybe_original_request = Some(request.clone());
            }
            return Ok(pi.depth);
        }

        // Synced datasets and roots do not depend on other local datasets, so they are at depth 0
        let (job, depth) = match target {
            PullTarget::Local(local_handle) => match self
                .update_kind_resolver
                .resolve_update_kind(&local_handle)
                .await?
            {
                DatasetUpdateKind::PollingIngest => (PullItemJob::Ingest(local_handle), 0),
                DatasetUpdateKind::SyncFrom { remote_ref } => (
                    PullItemJob::Sync {
                        local_target: PullLocalTarget::existing(local_handle),
                        remote_ref,
                    },
                    0,
                ),
                DatasetUpdateKind::Transform => {
                    let depth = if traverse_dependencies {
                        self.traverse_upstream_datasets(&local_handle.id).await? + 1
                    } else {
                        // Without scanning upstreams the exact depth is not important, as long as
                        // derivatives come after roots
                        1
                    };
                    (PullItemJob::Transform(local_handle), depth)
                }
            },
            PullTarget::Remote {
                local_target,
                remote_ref,
            } => (
                PullItemJob::Sync {
                    local_target,
                    remote_ref,
                },
                0,
            ),
        };

        let pull_item = PullItem {
            depth,
            job,
            maybe_original_request: referenced_explicitly.then(|| request.clone()),
        };

        tracing::debug!(?pull_item, "Resolved pull graph node");

        self.visited.insert(local_alias, pull_item);
        Ok(depth)
    }

    /// The request an upstream is planned from: its own request when it was
    /// requested too, otherwise a local reference to it
    async fn upstream_request(&self, dependency_id: &odf::DatasetID) -> PullRequest {
        let local_request = PullRequest::local(dependency_id.as_local_ref());
        match self.target_resolver.resolve(&local_request).await {
            Ok(target) => self
                .requested
                .get(target.local_alias())
                .cloned()
                .unwrap_or(local_request),
            Err(_) => local_request,
        }
    }

    async fn traverse_upstream_datasets(
        &mut self,
        dataset_id: &odf::DatasetID,
    ) -> Result<i32, PullError> {
        // TODO: EVO: Should be accounting for historical dependencies, not only current
        // ones?
        let mut max_dep_depth = -1;

        let upstream_dependencies = self
            .dependency_graph_service
            .get_upstream_dependencies(dataset_id)
            .await
            .collect::<Vec<_>>()
            .await;

        for dependency_id in upstream_dependencies {
            tracing::debug!(%dependency_id, "Descending into dependency");

            let request = self.upstream_request(&dependency_id).await;
            let depth = match self.traverse_pull_graph(&request, false, true).await {
                Ok(depth) => depth,
                // Without recursion upstreams only order the requested datasets and are never
                // pulled, so one that cannot be planned is skipped. A requested upstream is planned
                // from its own request, so it fails that request the same way
                Err(error) if !self.recursive => {
                    tracing::debug!(%dependency_id, %error, "Skipping an upstream that cannot be planned");
                    continue;
                }
                Err(error) => return Err(error),
            };
            max_dep_depth = std::cmp::max(max_dep_depth, depth);
        }

        Ok(max_dep_depth)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
