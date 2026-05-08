// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::future::Future;

use database_common::PaginationOpts;
use kamu_resources::{ResourceIdentityView, ResourceKindDescriptor};
use kamu_resources_facade::{
    GetResourceError,
    ListAllResourceIdentitiesRequest,
    ListResourceIdentitiesRequest,
    ResourceBatchSelector,
    ResourceFacade,
    ResourceLookupProblem,
};

use crate::CLIError;
use crate::resources::{
    ResourceIgnoredSelector,
    ResourceSelectionItem,
    ResourceSelectionResolution,
    ResourceSelectionResolutionOptions,
    ResourceSelectionResolutionService,
    ResourceSelectionSyntax,
    ResourceTarget,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceSelectionResolutionService)]
pub struct ResourceSelectionResolutionServiceImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const RESOURCE_PAGE_SIZE: usize = 100;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceSelectionResolutionService for ResourceSelectionResolutionServiceImpl {
    async fn resolve(
        &self,
        selection: ResourceSelectionSyntax,
        resource_facade: &dyn ResourceFacade,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<ResourceSelectionResolution, CLIError> {
        let mut targets = Vec::with_capacity(selection.items.len());
        let mut ignored_selectors = Vec::new();
        let mut expanded_results = 0;

        let exact_results = Self::fetch_exact_identities(&selection, resource_facade).await?;
        let mut exact_results = exact_results.into_iter();

        for item in selection.items {
            match item {
                ResourceSelectionItem::All => {
                    let new_targets =
                        Self::process_all_item(resource_facade, expanded_results, options).await?;
                    expanded_results += new_targets.len();
                    targets.extend(new_targets);
                }

                ResourceSelectionItem::AllByKind {
                    kind_descriptor,
                    selector_input,
                } => {
                    let new_targets = Self::process_all_by_kind_item(
                        resource_facade,
                        &kind_descriptor,
                        selector_input,
                        expanded_results,
                        options,
                    )
                    .await?;
                    expanded_results += new_targets.len();
                    targets.extend(new_targets);
                }

                ResourceSelectionItem::Exact(selector) => {
                    Self::process_exact_item(
                        selector,
                        &mut exact_results,
                        &mut targets,
                        &mut ignored_selectors,
                        options,
                    )?;
                }

                ResourceSelectionItem::NamePattern { selector_input, .. }
                | ResourceSelectionItem::KindPatternExactName { selector_input, .. }
                | ResourceSelectionItem::KindPatternAll { selector_input, .. }
                | ResourceSelectionItem::KindPatternNamePattern { selector_input, .. } => {
                    return Err(Self::patterns_not_supported_error(&selector_input));
                }
            }
        }

        Ok(ResourceSelectionResolution {
            targets,
            ignored_selectors,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSelectionResolutionServiceImpl {
    async fn fetch_exact_identities(
        selection: &ResourceSelectionSyntax,
        resource_facade: &dyn ResourceFacade,
    ) -> Result<Vec<Result<ResourceIdentityView, GetResourceError>>, CLIError> {
        let exact_selectors = selection
            .items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| match item {
                ResourceSelectionItem::Exact(selector) => Some((
                    index,
                    selector.kind_descriptor.kind.clone(),
                    selector.kind_descriptor.api_version.clone(),
                    selector.resource_ref.clone(),
                )),
                ResourceSelectionItem::All
                | ResourceSelectionItem::AllByKind { .. }
                | ResourceSelectionItem::NamePattern { .. }
                | ResourceSelectionItem::KindPatternExactName { .. }
                | ResourceSelectionItem::KindPatternAll { .. }
                | ResourceSelectionItem::KindPatternNamePattern { .. } => None,
            })
            .collect::<Vec<_>>();

        let exact_request_count = exact_selectors.len();
        let mut exact_results = (0..exact_request_count)
            .map(|_| None)
            .collect::<Vec<Option<Result<ResourceIdentityView, GetResourceError>>>>();
        let mut groups = BTreeMap::new();

        for (exact_index, (_, kind, api_version, resource_ref)) in
            exact_selectors.into_iter().enumerate()
        {
            groups
                .entry((kind, api_version))
                .or_insert_with(Vec::new)
                .push((exact_index, resource_ref));
        }

        for ((kind, api_version), entries) in groups {
            let exact_batch_result = resource_facade
                .get_identities(ResourceBatchSelector {
                    account: None,
                    kind,
                    api_version: Some(api_version),
                    resource_refs: entries
                        .iter()
                        .map(|(_, resource_ref)| resource_ref.clone())
                        .collect(),
                })
                .await?;

            for problem in exact_batch_result.problems {
                let (exact_index, _) = entries[problem.request_index];
                exact_results[exact_index] =
                    Some(Err(Self::lookup_problem_to_get_error(problem.error)));
            }

            for success in exact_batch_result.successes {
                let (exact_index, _) = entries[success.request_index];
                exact_results[exact_index] = Some(Ok(success.item));
            }
        }

        Ok(exact_results.into_iter().flatten().collect())
    }

    fn lookup_problem_to_get_error(error: ResourceLookupProblem) -> GetResourceError {
        GetResourceError::LookupProblem(error)
    }

    async fn process_all_item(
        resource_facade: &dyn ResourceFacade,
        expanded_results: usize,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let identities = Self::collect_bounded_pages(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            |pagination| async move {
                resource_facade
                    .list_all_identities(ListAllResourceIdentitiesRequest {
                        account: None,
                        pagination,
                    })
                    .await
                    .map_err(Into::into)
            },
        )
        .await?;

        Ok(identities
            .into_iter()
            .map(|identity| Self::target_from_identity(identity, "all".to_owned()))
            .collect())
    }

    async fn process_all_by_kind_item(
        resource_facade: &dyn ResourceFacade,
        kind_descriptor: &ResourceKindDescriptor,
        selector_input: String,
        expanded_results: usize,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let identities = Self::collect_bounded_pages(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            |pagination| async move {
                resource_facade
                    .list_identities(ListResourceIdentitiesRequest {
                        kind: kind_descriptor.kind.clone(),
                        account: None,
                        pagination,
                    })
                    .await
                    .map_err(Into::into)
            },
        )
        .await?;

        Ok(identities
            .into_iter()
            .map(|identity| Self::target_from_identity(identity, selector_input.clone()))
            .collect())
    }

    fn process_exact_item(
        selector: crate::resources::ResourceExactSelector,
        exact_results: &mut std::vec::IntoIter<Result<ResourceIdentityView, GetResourceError>>,
        targets: &mut Vec<ResourceTarget>,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<(), CLIError> {
        match exact_results
            .next()
            .expect("Every exact selector must have a batch result")
        {
            Ok(identity) => {
                targets.push(Self::target_from_identity(
                    identity,
                    selector.selector_input,
                ));
            }
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::UIDNotFound(_),
            )) if options.ignore_not_found => {
                ignored_selectors.push(ResourceIgnoredSelector {
                    kind_descriptor: selector.kind_descriptor,
                    selector_input: selector.selector_input,
                });
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    async fn collect_bounded_pages<T, F, Fut>(
        remaining_limit: Option<usize>,
        error_limit: Option<usize>,
        mut fetch_page: F,
    ) -> Result<Vec<T>, CLIError>
    where
        F: FnMut(PaginationOpts) -> Fut,
        Fut: Future<Output = Result<Vec<T>, CLIError>>,
    {
        if let Some(limit) = remaining_limit {
            let items = fetch_page(PaginationOpts {
                limit: limit.saturating_add(1),
                offset: 0,
            })
            .await?;

            if items.len() > limit {
                return Err(Self::max_expanded_results_exceeded_error(
                    error_limit.unwrap_or(limit),
                ));
            }

            return Ok(items);
        }

        let mut offset = 0;
        let mut items = Vec::new();

        loop {
            let page_items = fetch_page(PaginationOpts {
                limit: RESOURCE_PAGE_SIZE,
                offset,
            })
            .await?;
            let fetched = page_items.len();
            items.extend(page_items);

            if fetched < RESOURCE_PAGE_SIZE {
                break;
            }

            offset += fetched;
        }

        Ok(items)
    }

    fn remaining_expanded_results(
        expanded_results: usize,
        options: ResourceSelectionResolutionOptions,
    ) -> Option<usize> {
        options
            .max_expanded_results
            .map(|max_expanded_results| max_expanded_results.saturating_sub(expanded_results))
    }

    fn max_expanded_results_exceeded_error(limit: usize) -> CLIError {
        CLIError::usage_error(format!(
            "Selection matched more than {limit} resources; refine selectors, pass --max-results \
             N, or pass --unbounded"
        ))
    }

    fn patterns_not_supported_error(selector_input: &str) -> CLIError {
        CLIError::usage_error(format!(
            "Resource selector patterns are not supported yet: `{selector_input}`"
        ))
    }

    fn target_from_identity(
        identity: ResourceIdentityView,
        selector_input: String,
    ) -> ResourceTarget {
        ResourceTarget {
            kind: identity.kind,
            api_version: identity.api_version,
            canonical_kind_name: identity.canonical_kind_name,
            uid: identity.uid,
            name: identity.name,
            selector_input,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
