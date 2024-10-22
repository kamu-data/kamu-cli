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

use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use opendatafabric::*;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PullRequestPlannerImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    current_account_subject: Arc<CurrentAccountSubject>,
}

#[component(pub)]
#[interface(dyn PullRequestPlanner)]
impl PullRequestPlannerImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        current_account_subject: Arc<CurrentAccountSubject>,
    ) -> Self {
        Self {
            dataset_registry,
            remote_alias_reg,
            current_account_subject,
        }
    }

    #[async_recursion::async_recursion]
    async fn collect_pull_graph_depth_first(
        &self,
        request: &PullRequest,
        referenced_explicitly: bool,
        options: &PullMultiOptions,
        in_multi_tenant_mode: bool,
        visited: &mut HashMap<DatasetAlias, PullItem>,
    ) -> Result<i32, PullError> {
        tracing::debug!(?request, "Entering node");

        // Resolve local dataset handle, if dataset exists
        let maybe_local_handle = self.try_resolve_local_handle(request).await?;

        // If dataset is not found, and auto-create is disabled, it's en error
        if maybe_local_handle.is_none() && !options.sync_options.create_if_not_exists {
            return Err(PullError::InvalidOperation(
                "Dataset does not exist and auto-create is switched off".to_owned(),
            ));
        }

        // Resolve the name of a local dataset if it exists
        // or a name to create dataset with if syncing from remote and creation is
        // allowed
        let local_alias =
            self.form_local_alias(maybe_local_handle.as_ref(), request, in_multi_tenant_mode)?;

        // Already visited?
        if let Some(pi) = visited.get_mut(&local_alias) {
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
                PullItemLocalTarget::existing(local_handle)
            } else {
                PullItemLocalTarget::to_create(local_alias.clone())
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

            // Plan up-stream dependencies first
            let max_dep_depth = self
                .plan_upstream_datasets(&local_handle, options, in_multi_tenant_mode, visited)
                .await?;

            // Plan the current dataset as last
            PullItem {
                maybe_original_request: None, // May be set below
                depth: max_dep_depth + 1,
                local_target: PullItemLocalTarget::existing(local_handle),
                maybe_remote_ref: None,
            }
        };

        if referenced_explicitly {
            pull_item.maybe_original_request = Some(request.clone());
        }

        tracing::debug!(?pull_item, "Resolved node");

        let depth = pull_item.depth;
        visited.insert(local_alias.clone(), pull_item);
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
                    .remote_alias_reg
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
                    .remote_alias_reg
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
        in_multi_tenant_mode: bool,
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
                        self.infer_alias_from_remote_ref(&remote.remote_ref, in_multi_tenant_mode)?
                    }
                }
            }
        };

        Ok(local_alias)
    }

    fn infer_alias_from_remote_ref(
        &self,
        remote_ref: &DatasetRefRemote,
        in_multi_tenant_mode: bool,
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
                if in_multi_tenant_mode {
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

        let remote_aliases = match self.remote_alias_reg.get_remote_aliases(dataset).await {
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

    async fn plan_upstream_datasets(
        &self,
        local_handle: &DatasetHandle,
        options: &PullMultiOptions,
        in_multi_tenant_mode: bool,
        visited: &mut HashMap<DatasetAlias, PullItem>,
    ) -> Result<i32, PullError> {
        // Read summary to access dependencies
        let summary = self
            .dataset_registry
            .get_dataset_by_handle(local_handle)
            .get_summary(GetSummaryOpts::default())
            .await
            .int_err()?;

        // TODO: EVO: Should be accounting for historical dependencies, not only current
        // ones?
        let mut max_dep_depth = -1;

        for dependency_id in summary.dependencies {
            tracing::debug!(%dependency_id, "Descending into dependency");

            let depth = self
                .collect_pull_graph_depth_first(
                    &PullRequest::local(dependency_id.as_local_ref()),
                    false,
                    options,
                    in_multi_tenant_mode,
                    visited,
                )
                .await?;
            max_dep_depth = std::cmp::max(max_dep_depth, depth);
        }

        Ok(max_dep_depth)
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

    fn build_update_items(&self, pull_items: Vec<PullItem>) -> Vec<PullUpdateItem> {
        pull_items
            .into_iter()
            .map(|pi| {
                assert!(pi.maybe_remote_ref.is_none());

                PullUpdateItem {
                    depth: pi.depth,
                    local_handle: match pi.local_target {
                        PullItemLocalTarget::Existing(local_handle) => local_handle,
                        PullItemLocalTarget::ToCreate(_) => unreachable!(
                            "Ingest/Transform flows expect to work with existing local targets"
                        ),
                    },
                    maybe_original_request: pi.maybe_original_request,
                }
            })
            .collect()
    }

    fn build_sync_items(&self, pull_items: Vec<PullItem>) -> Vec<PullSyncItem> {
        pull_items
            .into_iter()
            .map(|pi| {
                assert!(pi.maybe_remote_ref.is_some());

                PullSyncItem {
                    depth: pi.depth,
                    local_target: pi.local_target,
                    remote_ref: pi.maybe_remote_ref.unwrap(),
                    maybe_original_request: pi.maybe_original_request,
                }
            })
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PullRequestPlanner for PullRequestPlannerImpl {
    // This function descends down the dependency tree of datasets (starting with
    // provided references) assigning depth index to every dataset in the
    // graph(s). Datasets that share the same depth level are independent and
    // can be pulled in parallel.
    async fn collect_pull_graph(
        &self,
        requests: &[PullRequest],
        options: &PullMultiOptions,
        in_multi_tenant_mode: bool,
    ) -> (Vec<PullItem>, Vec<PullResponse>) {
        let mut visited = HashMap::new();
        let mut errors = Vec::new();

        for pr in requests {
            match self
                .collect_pull_graph_depth_first(
                    pr,
                    true,
                    options,
                    in_multi_tenant_mode,
                    &mut visited,
                )
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

    fn prepare_pull_execution_steps(&self, plan: Vec<PullItem>) -> Vec<PullExecutionStep> {
        let mut steps = Vec::new();

        let mut rest = plan;
        while !rest.is_empty() {
            let (depth, is_remote, batch, tail) = self.slice(rest);
            rest = tail;

            let details = if depth == 0 && !is_remote {
                PullExecutionStepDetails::Ingest(self.build_update_items(batch))
            } else if depth == 0 && is_remote {
                PullExecutionStepDetails::Sync(self.build_sync_items(batch))
            } else {
                PullExecutionStepDetails::Transform(self.build_update_items(batch))
            };

            steps.push(PullExecutionStep { depth, details });
        }

        steps
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
