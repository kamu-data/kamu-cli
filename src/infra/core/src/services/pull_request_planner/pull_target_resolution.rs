// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The dataset a pull request points at, resolved before its update kind is
/// known
pub(super) enum PullTarget {
    /// A local request names an existing dataset
    Local(odf::DatasetHandle),
    /// A remote request syncs into an existing dataset or one to create
    Remote {
        local_target: PullLocalTarget,
        remote_ref: odf::DatasetRefRemote,
    },
}

impl PullTarget {
    pub(super) fn local_alias(&self) -> &odf::DatasetAlias {
        match self {
            Self::Local(handle) => &handle.alias,
            Self::Remote { local_target, .. } => local_target.alias(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves the local dataset each pull request updates, or creates
pub(super) struct PullTargetResolver<'a> {
    dataset_registry: &'a dyn DatasetRegistry,
    pull_source_resolver: &'a dyn DatasetPullSourceResolver,
    current_account_subject: &'a CurrentAccountSubject,
    options: &'a PullOptions,
    tenancy_config: TenancyConfig,
}

impl<'a> PullTargetResolver<'a> {
    pub(super) fn new(
        dataset_registry: &'a dyn DatasetRegistry,
        pull_source_resolver: &'a dyn DatasetPullSourceResolver,
        current_account_subject: &'a CurrentAccountSubject,
        options: &'a PullOptions,
        tenancy_config: TenancyConfig,
    ) -> Self {
        Self {
            dataset_registry,
            pull_source_resolver,
            current_account_subject,
            options,
            tenancy_config,
        }
    }

    pub(super) async fn resolve(&self, request: &PullRequest) -> Result<PullTarget, PullError> {
        match request {
            PullRequest::Local(local_ref) => {
                match self
                    .dataset_registry
                    .try_resolve_dataset_handle_by_ref(local_ref)
                    .await?
                {
                    Some(handle) => Ok(PullTarget::Local(handle)),
                    None => Err(PullError::NotFound(odf::DatasetNotFoundError {
                        dataset_ref: local_ref.clone(),
                    })),
                }
            }
            PullRequest::Remote(remote) => {
                let local_target = match self.find_existing_sync_target(remote).await? {
                    Some(handle) => PullLocalTarget::existing(handle),
                    None if !self.options.sync_options.create_if_not_exists => {
                        return Err(PullError::AutoCreateDisabled);
                    }
                    None => PullLocalTarget::to_create(match &remote.maybe_local_alias {
                        Some(local_alias) => local_alias.clone(),
                        None => self.infer_alias_from_remote_ref(&remote.remote_ref)?,
                    }),
                };
                Ok(PullTarget::Remote {
                    local_target,
                    remote_ref: remote.remote_ref.clone(),
                })
            }
        }
    }

    /// The local dataset a remote request syncs into: the one named by the
    /// request, otherwise the one whose pull alias is the remote
    async fn find_existing_sync_target(
        &self,
        remote: &PullRequestRemote,
    ) -> Result<Option<odf::DatasetHandle>, PullError> {
        if let Some(local_alias) = &remote.maybe_local_alias
            && let Some(handle) = self
                .dataset_registry
                .try_resolve_dataset_handle_by_ref(&local_alias.as_local_ref())
                .await?
        {
            return Ok(Some(handle));
        }

        let Some(handle) = self
            .pull_source_resolver
            .find_dataset_by_pull_alias(&remote.remote_ref)
            .await?
        else {
            return Ok(None);
        };

        // Ensure new alias is identical to the old one
        if let Some(local_alias) = &remote.maybe_local_alias
            && handle.alias != *local_alias
        {
            return Err(PullError::SaveUnderDifferentAlias(handle.alias.to_string()));
        }
        Ok(Some(handle))
    }

    fn infer_alias_from_remote_ref(
        &self,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<odf::DatasetAlias, PullError> {
        let cannot_infer = || CannotInferLocalNameError {
            remote_ref: remote_ref.clone(),
        };

        Ok(match &remote_ref {
            odf::DatasetRefRemote::ID(_, _) => return Err(cannot_infer().into()),

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
                Self::infer_local_name_from_url(url).ok_or_else(cannot_infer)?,
            ),
        })
    }

    fn infer_local_name_from_url(url: &Url) -> Option<odf::DatasetName> {
        // Try to use last path segment for a name (ignoring the trailing slash)
        if let Some(path) = url.path_segments()
            && let Some(last_segment) = path.rev().find(|s| !s.is_empty())
            && let Ok(name) = odf::DatasetName::try_from(last_segment)
        {
            return Some(name);
        }
        // Fall back to using domain name
        if let Some(url::Host::Domain(host)) = url.host()
            && let Ok(name) = odf::DatasetName::try_from(host)
        {
            return Some(name);
        }
        None
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
