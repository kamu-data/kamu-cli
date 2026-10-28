// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::*;
use kamu_datasets::DatasetRegistryExt;
use url::Url;

use super::pull_graph_traversal::PullGraphDepthFirstTraversal;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl PullGraphDepthFirstTraversal<'_> {
    pub(super) async fn try_resolve_local_handle(
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

    pub(super) fn form_local_alias(
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
