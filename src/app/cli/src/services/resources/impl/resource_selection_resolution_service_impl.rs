// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashSet};
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
    ResourceSelector,
    SearchResourceIdentitiesRequest,
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
        let mut seen_target_keys = HashSet::new();

        let supported_kinds =
            Self::supported_kinds_for_patterns(&selection, resource_facade).await?;

        // Exact selectors are prefetched in batches before the main loop so we can
        // collapse many single-item lookups into grouped backend calls while still
        // replaying results in the original selector order below. At this point
        // shadowed selectors are already absent from `selection.items`, because the
        // syntax layer moved them into `shadowed_selectors` instead.
        let exact_results = Self::fetch_exact_identities(&selection, resource_facade).await?;
        let mut exact_results = exact_results.into_iter();

        for item in selection.items {
            match item {
                ResourceSelectionItem::All => {
                    let new_targets = Self::process_all_item(
                        resource_facade,
                        &seen_target_keys,
                        expanded_results,
                        options,
                    )
                    .await?;
                    expanded_results +=
                        Self::append_new_targets(&mut targets, &mut seen_target_keys, new_targets);
                }

                ResourceSelectionItem::AllByKind {
                    kind_descriptor,
                    selector_input,
                } => {
                    let new_targets = Self::process_all_by_kind_item(
                        resource_facade,
                        &kind_descriptor,
                        &seen_target_keys,
                        selector_input,
                        expanded_results,
                        options,
                    )
                    .await?;
                    expanded_results +=
                        Self::append_new_targets(&mut targets, &mut seen_target_keys, new_targets);
                }

                ResourceSelectionItem::Exact(selector) => {
                    Self::process_exact_item(
                        selector,
                        &mut exact_results,
                        &mut seen_target_keys,
                        &mut targets,
                        &mut ignored_selectors,
                        options,
                    )?;
                }

                ResourceSelectionItem::NamePattern {
                    kind_descriptor,
                    selector_input,
                    name_pattern,
                } => {
                    let new_targets = Self::process_name_pattern_item(
                        resource_facade,
                        &kind_descriptor,
                        &seen_target_keys,
                        selector_input,
                        name_pattern,
                        expanded_results,
                        &mut ignored_selectors,
                        options,
                    )
                    .await?;
                    expanded_results +=
                        Self::append_new_targets(&mut targets, &mut seen_target_keys, new_targets);
                }

                ResourceSelectionItem::KindPatternExactName {
                    kind_pattern,
                    selector_input,
                    resource_ref,
                } => {
                    let matched_supported_kinds = supported_kinds
                        .as_deref()
                        .expect("kind patterns require supported kinds");
                    let new_targets = Self::process_kind_pattern_exact_name_item(
                        resource_facade,
                        matched_supported_kinds,
                        &seen_target_keys,
                        kind_pattern,
                        selector_input,
                        resource_ref,
                        expanded_results,
                        &mut ignored_selectors,
                        options,
                    )
                    .await?;
                    expanded_results +=
                        Self::append_new_targets(&mut targets, &mut seen_target_keys, new_targets);
                }

                ResourceSelectionItem::KindPatternAll {
                    kind_pattern,
                    selector_input,
                } => {
                    let matched_supported_kinds = supported_kinds
                        .as_deref()
                        .expect("kind patterns require supported kinds");
                    let new_targets = Self::process_kind_pattern_all_item(
                        resource_facade,
                        matched_supported_kinds,
                        &seen_target_keys,
                        kind_pattern,
                        selector_input,
                        expanded_results,
                        options,
                    )
                    .await?;
                    expanded_results +=
                        Self::append_new_targets(&mut targets, &mut seen_target_keys, new_targets);
                }

                ResourceSelectionItem::KindPatternNamePattern {
                    kind_pattern,
                    selector_input,
                    name_pattern,
                } => {
                    let matched_supported_kinds = supported_kinds
                        .as_deref()
                        .expect("kind patterns require supported kinds");
                    let new_targets = Self::process_kind_pattern_name_pattern_item(
                        resource_facade,
                        matched_supported_kinds,
                        &seen_target_keys,
                        kind_pattern,
                        selector_input,
                        name_pattern,
                        expanded_results,
                        &mut ignored_selectors,
                        options,
                    )
                    .await?;
                    expanded_results +=
                        Self::append_new_targets(&mut targets, &mut seen_target_keys, new_targets);
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
    fn target_key_from_identity(identity: &ResourceIdentityView) -> ResourceTargetKey {
        (
            identity.kind.clone(),
            identity.api_version.clone(),
            identity.id,
        )
    }

    fn target_key(target: &ResourceTarget) -> ResourceTargetKey {
        (target.kind.clone(), target.api_version.clone(), target.id)
    }

    fn append_new_targets(
        targets: &mut Vec<ResourceTarget>,
        seen_target_keys: &mut HashSet<ResourceTargetKey>,
        new_targets: Vec<ResourceTarget>,
    ) -> usize {
        let mut appended = 0;

        for target in new_targets {
            if seen_target_keys.insert(Self::target_key(&target)) {
                targets.push(target);
                appended += 1;
            }
        }

        appended
    }

    async fn supported_kinds_for_patterns(
        selection: &ResourceSelectionSyntax,
        resource_facade: &dyn ResourceFacade,
    ) -> Result<Option<Vec<ResourceKindDescriptor>>, CLIError> {
        let needs_supported_kinds = selection.items.iter().any(|item| {
            matches!(
                item,
                ResourceSelectionItem::KindPatternExactName { .. }
                    | ResourceSelectionItem::KindPatternAll { .. }
                    | ResourceSelectionItem::KindPatternNamePattern { .. }
            )
        });

        if needs_supported_kinds {
            Ok(Some(resource_facade.list_supported_kinds().await?))
        } else {
            Ok(None)
        }
    }

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
        seen_target_keys: &HashSet<ResourceTargetKey>,
        expanded_results: usize,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let collected = Self::collect_unique_bounded_identities(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            seen_target_keys,
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

        Ok(collected
            .identities
            .into_iter()
            .map(|identity| Self::target_from_identity(identity, "all".to_owned()))
            .collect())
    }

    async fn process_all_by_kind_item(
        resource_facade: &dyn ResourceFacade,
        kind_descriptor: &ResourceKindDescriptor,
        seen_target_keys: &HashSet<ResourceTargetKey>,
        selector_input: String,
        expanded_results: usize,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let collected = Self::collect_unique_bounded_identities(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            seen_target_keys,
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

        Ok(collected
            .identities
            .into_iter()
            .map(|identity| Self::target_from_identity(identity, selector_input.clone()))
            .collect())
    }

    async fn process_kind_pattern_all_item(
        resource_facade: &dyn ResourceFacade,
        supported_kinds: &[ResourceKindDescriptor],
        seen_target_keys: &HashSet<ResourceTargetKey>,
        kind_pattern: String,
        selector_input: String,
        expanded_results: usize,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let matched_kinds = Self::matched_kind_descriptors(supported_kinds, &kind_pattern);

        if matched_kinds.is_empty() {
            return Err(Self::unsupported_kind_pattern_error(
                supported_kinds,
                &kind_pattern,
            ));
        }

        let matched_kind_names = matched_kinds
            .iter()
            .map(|descriptor| descriptor.kind.clone())
            .collect::<Vec<_>>();

        let collected = Self::collect_unique_bounded_identities(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            seen_target_keys,
            |pagination| {
                let request_kinds = matched_kind_names.clone();
                async move {
                    resource_facade
                        .search_identities(SearchResourceIdentitiesRequest {
                            kinds: request_kinds,
                            exact_names: None,
                            name_pattern: None,
                            account: None,
                            pagination,
                        })
                        .await
                        .map(|response| response.items)
                        .map_err(Into::into)
                }
            },
        )
        .await?;

        Ok(collected
            .identities
            .into_iter()
            .map(|identity| Self::target_from_identity(identity, selector_input.clone()))
            .collect())
    }

    async fn process_name_pattern_item(
        resource_facade: &dyn ResourceFacade,
        kind_descriptor: &ResourceKindDescriptor,
        seen_target_keys: &HashSet<ResourceTargetKey>,
        selector_input: String,
        name_pattern: String,
        expanded_results: usize,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let collected = Self::collect_unique_bounded_identities(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            seen_target_keys,
            |pagination| {
                let request_name_pattern = name_pattern.clone();
                async move {
                    resource_facade
                        .search_identities(SearchResourceIdentitiesRequest {
                            kinds: vec![kind_descriptor.kind.clone()],
                            exact_names: None,
                            name_pattern: Some(request_name_pattern),
                            account: None,
                            pagination,
                        })
                        .await
                        .map(|response| response.items)
                        .map_err(Into::into)
                }
            },
        )
        .await?;

        if collected.identities.is_empty() && !collected.had_any_match {
            if options.ignore_not_found {
                ignored_selectors.push(ResourceIgnoredSelector {
                    kind_descriptor: kind_descriptor.clone(),
                    selector_input,
                });
                return Ok(Vec::new());
            }

            return Err(Self::name_pattern_not_found_error(
                kind_descriptor,
                &name_pattern,
            ));
        }

        Ok(collected
            .identities
            .into_iter()
            .map(|identity| Self::target_from_identity(identity, selector_input.clone()))
            .collect())
    }

    async fn process_kind_pattern_exact_name_item(
        resource_facade: &dyn ResourceFacade,
        supported_kinds: &[ResourceKindDescriptor],
        seen_target_keys: &HashSet<ResourceTargetKey>,
        kind_pattern: String,
        selector_input: String,
        resource_ref: kamu_resources_facade::ResourceRef,
        expanded_results: usize,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let matched_kinds = Self::matched_kind_descriptors(supported_kinds, &kind_pattern);

        if matched_kinds.is_empty() {
            return Err(Self::unsupported_kind_pattern_error(
                supported_kinds,
                &kind_pattern,
            ));
        }

        let mut targets = Vec::new();
        let mut local_seen_target_keys = HashSet::new();
        let remaining_limit = Self::remaining_expanded_results(expanded_results, options);
        let mut had_any_match = false;

        for kind_descriptor in &matched_kinds {
            match resource_facade
                .get_identity(ResourceSelector {
                    account: None,
                    kind: kind_descriptor.kind.clone(),
                    api_version: Some(kind_descriptor.api_version.clone()),
                    resource_ref: resource_ref.clone(),
                })
                .await
            {
                Ok(identity) => {
                    had_any_match = true;
                    let target_key = Self::target_key_from_identity(&identity);

                    if seen_target_keys.contains(&target_key)
                        || !local_seen_target_keys.insert(target_key)
                    {
                        continue;
                    }

                    targets.push(Self::target_from_identity(identity, selector_input.clone()));

                    if let Some(limit) = remaining_limit
                        && targets.len() > limit
                    {
                        return Err(Self::max_expanded_results_exceeded_error(
                            options.max_expanded_results.unwrap_or(limit),
                        ));
                    }
                }
                Err(GetResourceError::LookupProblem(
                    ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::IDNotFound(_),
                )) => {}
                Err(error) => return Err(error.into()),
            }
        }

        if targets.is_empty() && !had_any_match {
            if options.ignore_not_found {
                ignored_selectors.push(ResourceIgnoredSelector {
                    kind_descriptor: matched_kinds
                        .first()
                        .expect("matched kinds should be non-empty")
                        .clone(),
                    selector_input,
                });
                return Ok(Vec::new());
            }

            return Err(Self::kind_pattern_exact_selector_not_found_error(
                &kind_pattern,
                &resource_ref,
            ));
        }

        Ok(targets)
    }

    async fn process_kind_pattern_name_pattern_item(
        resource_facade: &dyn ResourceFacade,
        supported_kinds: &[ResourceKindDescriptor],
        seen_target_keys: &HashSet<ResourceTargetKey>,
        kind_pattern: String,
        selector_input: String,
        name_pattern: String,
        expanded_results: usize,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let matched_kinds = Self::matched_kind_descriptors(supported_kinds, &kind_pattern);

        if matched_kinds.is_empty() {
            return Err(Self::unsupported_kind_pattern_error(
                supported_kinds,
                &kind_pattern,
            ));
        }

        let matched_kind_names = matched_kinds
            .iter()
            .map(|descriptor| descriptor.kind.clone())
            .collect::<Vec<_>>();

        let collected = Self::collect_unique_bounded_identities(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            seen_target_keys,
            |pagination| {
                let request_kinds = matched_kind_names.clone();
                let request_name_pattern = name_pattern.clone();
                async move {
                    resource_facade
                        .search_identities(SearchResourceIdentitiesRequest {
                            kinds: request_kinds,
                            exact_names: None,
                            name_pattern: Some(request_name_pattern),
                            account: None,
                            pagination,
                        })
                        .await
                        .map(|response| response.items)
                        .map_err(Into::into)
                }
            },
        )
        .await?;

        if collected.identities.is_empty() && !collected.had_any_match {
            if options.ignore_not_found {
                ignored_selectors.push(ResourceIgnoredSelector {
                    kind_descriptor: matched_kinds
                        .first()
                        .expect("matched kinds should be non-empty")
                        .clone(),
                    selector_input,
                });
                return Ok(Vec::new());
            }

            return Err(Self::kind_pattern_name_pattern_not_found_error(
                &kind_pattern,
                &name_pattern,
            ));
        }

        Ok(collected
            .identities
            .into_iter()
            .map(|identity| Self::target_from_identity(identity, selector_input.clone()))
            .collect())
    }

    fn process_exact_item(
        selector: crate::resources::ResourceExactSelector,
        exact_results: &mut std::vec::IntoIter<Result<ResourceIdentityView, GetResourceError>>,
        seen_target_keys: &mut HashSet<ResourceTargetKey>,
        targets: &mut Vec<ResourceTarget>,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<(), CLIError> {
        match exact_results
            .next()
            .expect("Every exact selector must have a batch result")
        {
            Ok(identity) => {
                let target = Self::target_from_identity(identity, selector.selector_input);

                if seen_target_keys.insert(Self::target_key(&target)) {
                    targets.push(target);
                }
            }
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::IDNotFound(_),
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

    async fn collect_unique_bounded_identities<F, Fut>(
        remaining_limit: Option<usize>,
        error_limit: Option<usize>,
        seen_target_keys: &HashSet<ResourceTargetKey>,
        mut fetch_page: F,
    ) -> Result<CollectedUniqueIdentities, CLIError>
    where
        F: FnMut(PaginationOpts) -> Fut,
        Fut: Future<Output = Result<Vec<ResourceIdentityView>, CLIError>>,
    {
        let mut offset = 0;
        let mut items = Vec::new();
        let mut local_seen_target_keys = HashSet::new();
        let mut had_any_match = false;

        loop {
            let page_items = fetch_page(PaginationOpts {
                limit: RESOURCE_PAGE_SIZE,
                offset,
            })
            .await?;
            let fetched = page_items.len();
            had_any_match |= fetched > 0;

            for identity in page_items {
                let target_key = Self::target_key_from_identity(&identity);

                if seen_target_keys.contains(&target_key)
                    || !local_seen_target_keys.insert(target_key)
                {
                    continue;
                }

                items.push(identity);

                if let Some(limit) = remaining_limit
                    && items.len() > limit
                {
                    return Err(Self::max_expanded_results_exceeded_error(
                        error_limit.unwrap_or(limit),
                    ));
                }
            }

            if fetched < RESOURCE_PAGE_SIZE {
                break;
            }

            offset += fetched;
        }

        Ok(CollectedUniqueIdentities {
            identities: items,
            had_any_match,
        })
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

    fn matched_kind_descriptors(
        supported_kinds: &[ResourceKindDescriptor],
        kind_pattern: &str,
    ) -> Vec<ResourceKindDescriptor> {
        supported_kinds
            .iter()
            .filter(|descriptor| descriptor.matches_selector_pattern(kind_pattern))
            .cloned()
            .collect()
    }

    fn unsupported_kind_pattern_error(
        supported_kinds: &[ResourceKindDescriptor],
        kind_pattern: &str,
    ) -> CLIError {
        CLIError::usage_error(format!(
            "Unsupported get target '{kind_pattern}'. Supported targets: {}",
            Self::supported_targets(supported_kinds).join(", ")
        ))
    }

    fn name_pattern_not_found_error(
        kind_descriptor: &ResourceKindDescriptor,
        name_pattern: &str,
    ) -> CLIError {
        CLIError::usage_error(format!(
            "Pattern `{name_pattern}` did not match any {}",
            kind_descriptor.name
        ))
    }

    fn kind_pattern_exact_selector_not_found_error(
        kind_pattern: &str,
        resource_ref: &kamu_resources_facade::ResourceRef,
    ) -> CLIError {
        let selector = match resource_ref {
            kamu_resources_facade::ResourceRef::ById(id) => id.to_string(),
            kamu_resources_facade::ResourceRef::ByName(name) => name.clone(),
        };

        CLIError::usage_error(format!(
            "Selector `{selector}` did not match any resource kind matched by `{kind_pattern}`"
        ))
    }

    fn kind_pattern_name_pattern_not_found_error(
        kind_pattern: &str,
        name_pattern: &str,
    ) -> CLIError {
        CLIError::usage_error(format!(
            "Pattern `{name_pattern}` did not match any resource kind matched by `{kind_pattern}`"
        ))
    }

    fn supported_targets(supported_kinds: &[ResourceKindDescriptor]) -> Vec<String> {
        let mut targets = Vec::new();

        for descriptor in supported_kinds {
            targets.push(descriptor.name.clone());
            targets.extend(descriptor.short_names.iter().cloned());
        }

        targets.sort();
        targets.dedup();
        targets
    }

    fn target_from_identity(
        identity: ResourceIdentityView,
        selector_input: String,
    ) -> ResourceTarget {
        ResourceTarget {
            kind: identity.kind,
            api_version: identity.api_version,
            canonical_kind_name: identity.canonical_kind_name,
            id: identity.id,
            name: identity.name,
            selector_input,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type ResourceTargetKey = (String, String, kamu_resources::ResourceID);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CollectedUniqueIdentities {
    identities: Vec<ResourceIdentityView>,
    had_any_match: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
