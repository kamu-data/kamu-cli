// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use kamu_datasets::{DatasetRegistry, DependencyGraphService};

use super::pull_item::PullItem;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) struct PullGraphDepthFirstTraversal<'a> {
    pub(super) dataset_registry: Arc<dyn DatasetRegistry>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    pub(super) pull_source_resolver: Arc<dyn DatasetPullSourceResolver>,
    update_kind_resolver: Arc<dyn DatasetUpdateKindResolver>,
    pub(super) current_account_subject: Arc<CurrentAccountSubject>,
    options: &'a PullOptions,
    pub(super) tenancy_config: TenancyConfig,
    pub(super) visited: HashMap<odf::DatasetAlias, PullItem>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'a> PullGraphDepthFirstTraversal<'a> {
    pub(super) fn new(
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
    pub(super) async fn traverse_pull_graph(
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
            return Err(PullError::AutoCreateDisabled);
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
