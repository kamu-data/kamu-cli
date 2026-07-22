// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;

use database_common::PaginationOpts;
use internal_error::InternalError;
use kamu_resources::{ResourceHandle, ResourceTypeDescriptor};
use kamu_resources_facade::{
    GetResourceError,
    ListAllResourceHandlesRequest,
    ListResourceHandlesRequest,
    ResourceBatchSelector,
    ResourceFacade,
    ResourceLookupProblem,
    ResourceSelector,
    SearchResourceHandlesRequest,
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

        let supported_resource_types =
            Self::supported_resource_types_for_patterns(&selection, resource_facade).await?;

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
                    let matched_resource_types = supported_resource_types
                        .as_deref()
                        .expect("`all` requires supported types");
                    let new_targets = Self::process_all_item(
                        resource_facade,
                        matched_resource_types,
                        &seen_target_keys,
                        expanded_results,
                        options,
                    )
                    .await?;
                    expanded_results +=
                        Self::append_new_targets(&mut targets, &mut seen_target_keys, new_targets);
                }

                ResourceSelectionItem::AllByType {
                    type_descriptor,
                    selector_input,
                } => {
                    let new_targets = Self::process_all_by_type_item(
                        resource_facade,
                        &type_descriptor,
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
                    type_descriptor,
                    selector_input,
                    name_pattern,
                } => {
                    let new_targets = Self::process_name_pattern_item(
                        resource_facade,
                        &type_descriptor,
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

                ResourceSelectionItem::TypePatternExactName {
                    type_pattern,
                    selector_input,
                    resource_ref,
                } => {
                    let matched_resource_types = supported_resource_types
                        .as_deref()
                        .expect("type patterns require supported types");
                    let new_targets = Self::process_type_pattern_exact_name_item(
                        resource_facade,
                        matched_resource_types,
                        &seen_target_keys,
                        type_pattern,
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

                ResourceSelectionItem::TypePatternAll {
                    type_pattern,
                    selector_input,
                } => {
                    let matched_resource_types = supported_resource_types
                        .as_deref()
                        .expect("type patterns require supported types");
                    let new_targets = Self::process_type_pattern_all_item(
                        resource_facade,
                        matched_resource_types,
                        &seen_target_keys,
                        type_pattern,
                        selector_input,
                        expanded_results,
                        options,
                    )
                    .await?;
                    expanded_results +=
                        Self::append_new_targets(&mut targets, &mut seen_target_keys, new_targets);
                }

                ResourceSelectionItem::TypePatternNamePattern {
                    type_pattern,
                    selector_input,
                    name_pattern,
                } => {
                    let matched_resource_types = supported_resource_types
                        .as_deref()
                        .expect("type patterns require supported types");
                    let new_targets = Self::process_type_pattern_name_pattern_item(
                        resource_facade,
                        matched_resource_types,
                        &seen_target_keys,
                        type_pattern,
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
    fn target_key_from_handle(handle: &ResourceHandle) -> ResourceTargetKey {
        (handle.r#type.clone(), handle.id)
    }

    fn target_key(target: &ResourceTarget) -> ResourceTargetKey {
        (target.schema.clone(), target.id)
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

    async fn supported_resource_types_for_patterns(
        selection: &ResourceSelectionSyntax,
        resource_facade: &dyn ResourceFacade,
    ) -> Result<Option<Vec<ResourceTypeDescriptor>>, CLIError> {
        let needs_supported_types = selection.items.iter().any(|item| {
            matches!(
                item,
                ResourceSelectionItem::All
                    | ResourceSelectionItem::TypePatternExactName { .. }
                    | ResourceSelectionItem::TypePatternAll { .. }
                    | ResourceSelectionItem::TypePatternNamePattern { .. }
            )
        });

        if needs_supported_types {
            Ok(Some(resource_facade.list_supported_resource_types().await?))
        } else {
            Ok(None)
        }
    }

    async fn fetch_exact_identities(
        selection: &ResourceSelectionSyntax,
        resource_facade: &dyn ResourceFacade,
    ) -> Result<Vec<Result<ResourceHandle, GetResourceError>>, CLIError> {
        let exact_selectors = selection
            .items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| match item {
                ResourceSelectionItem::Exact(selector) => Some((
                    index,
                    selector.type_descriptor.canonical_selector.clone().into(),
                    selector.resource_ref.clone(),
                )),
                ResourceSelectionItem::All
                | ResourceSelectionItem::AllByType { .. }
                | ResourceSelectionItem::NamePattern { .. }
                | ResourceSelectionItem::TypePatternExactName { .. }
                | ResourceSelectionItem::TypePatternAll { .. }
                | ResourceSelectionItem::TypePatternNamePattern { .. } => None,
            })
            .collect::<Vec<_>>();

        let exact_request_count = exact_selectors.len();
        let mut exact_results = (0..exact_request_count)
            .map(|_| None)
            .collect::<Vec<Option<Result<ResourceHandle, GetResourceError>>>>();
        let mut groups = BTreeMap::new();

        for (exact_index, (_, resource_type, resource_ref)) in
            exact_selectors.into_iter().enumerate()
        {
            groups
                .entry(resource_type)
                .or_insert_with(Vec::new)
                .push((exact_index, resource_ref));
        }

        for (resource_type, entries) in groups {
            let exact_batch_result = resource_facade
                .get_handles(ResourceBatchSelector {
                    account: None,
                    resource_type,
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
        supported_resource_types: &[ResourceTypeDescriptor],
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
                    .list_all_handles(ListAllResourceHandlesRequest {
                        account: None,
                        pagination,
                    })
                    .await
                    .map_err(Into::into)
            },
        )
        .await?;

        // Handles intentionally carry a schema TypeUri, not a CLI selector.
        // Reconstruct command-routing selectors from the descriptor set that
        // was already loaded for this expansion.
        let canonical_selectors_by_schema =
            Self::canonical_selectors_by_schema(supported_resource_types);

        collected
            .identities
            .into_iter()
            .map(|handle| {
                let canonical_selector = Self::canonical_selector_for_schema(
                    &canonical_selectors_by_schema,
                    &handle.r#type,
                )?;
                Ok(Self::target_from_handle(
                    handle,
                    canonical_selector.clone(),
                    "all".to_owned(),
                ))
            })
            .collect()
    }

    /// Resolves the canonical selector for a handle's schema against an
    /// already-fetched descriptor set. A miss is an internal inconsistency:
    /// the backend returned a handle outside the selector scope requested by
    /// the CLI expansion.
    fn canonical_selector_for_schema<'a>(
        canonical_selectors_by_schema: &'a CanonicalSelectorsBySchema<'a>,
        schema: &kamu_resources::TypeUri,
    ) -> Result<&'a kamu_resources::ResourceSelectorName, CLIError> {
        canonical_selectors_by_schema
            .get(schema)
            .copied()
            .ok_or_else(|| {
                CLIError::critical(InternalError::new(format!(
                    "No resource descriptor registered for {schema}"
                )))
            })
    }

    async fn process_all_by_type_item(
        resource_facade: &dyn ResourceFacade,
        type_descriptor: &ResourceTypeDescriptor,
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
                    .list_handles(ListResourceHandlesRequest {
                        raw_type_selector: (&type_descriptor.canonical_selector).into(),
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
            .map(|handle| {
                Self::target_from_handle(
                    handle,
                    type_descriptor.canonical_selector.clone(),
                    selector_input.clone(),
                )
            })
            .collect())
    }

    async fn process_type_pattern_all_item(
        resource_facade: &dyn ResourceFacade,
        supported_resource_types: &[ResourceTypeDescriptor],
        seen_target_keys: &HashSet<ResourceTargetKey>,
        type_pattern: String,
        selector_input: String,
        expanded_results: usize,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let matched_types = Self::matched_type_descriptors(supported_resource_types, &type_pattern);

        if matched_types.is_empty() {
            return Err(Self::unsupported_resource_type_pattern_error(
                supported_resource_types,
                &type_pattern,
            ));
        }

        let matched_resource_type_selectors = matched_types
            .iter()
            .map(|descriptor| (&descriptor.canonical_selector).into())
            .collect::<Vec<_>>();

        let canonical_selectors_by_schema = Self::canonical_selectors_by_schema(&matched_types);

        let collected = Self::collect_unique_bounded_identities(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            seen_target_keys,
            |pagination| {
                let raw_type_selectors = matched_resource_type_selectors.clone();
                async move {
                    resource_facade
                        .search_handles(SearchResourceHandlesRequest {
                            raw_type_selectors,
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

        collected
            .identities
            .into_iter()
            .map(|handle| {
                let canonical_selector = Self::canonical_selector_for_schema(
                    &canonical_selectors_by_schema,
                    &handle.r#type,
                )?;
                Ok(Self::target_from_handle(
                    handle,
                    canonical_selector.clone(),
                    selector_input.clone(),
                ))
            })
            .collect()
    }

    async fn process_name_pattern_item(
        resource_facade: &dyn ResourceFacade,
        type_descriptor: &ResourceTypeDescriptor,
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
                        .search_handles(SearchResourceHandlesRequest {
                            raw_type_selectors: vec![(&type_descriptor.canonical_selector).into()],
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
                    type_descriptor: type_descriptor.clone(),
                    selector_input,
                });
                return Ok(Vec::new());
            }

            return Err(Self::name_pattern_not_found_error(
                type_descriptor,
                &name_pattern,
            ));
        }

        Ok(collected
            .identities
            .into_iter()
            .map(|handle| {
                Self::target_from_handle(
                    handle,
                    type_descriptor.canonical_selector.clone(),
                    selector_input.clone(),
                )
            })
            .collect())
    }

    async fn process_type_pattern_exact_name_item(
        resource_facade: &dyn ResourceFacade,
        supported_resource_types: &[ResourceTypeDescriptor],
        seen_target_keys: &HashSet<ResourceTargetKey>,
        type_pattern: String,
        selector_input: String,
        resource_ref: kamu_resources_facade::ResourceRef,
        expanded_results: usize,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let matched_types = Self::matched_type_descriptors(supported_resource_types, &type_pattern);

        if matched_types.is_empty() {
            return Err(Self::unsupported_resource_type_pattern_error(
                supported_resource_types,
                &type_pattern,
            ));
        }

        let mut targets = Vec::new();
        let mut local_seen_target_keys = HashSet::new();
        let remaining_limit = Self::remaining_expanded_results(expanded_results, options);
        let mut had_any_match = false;

        for type_descriptor in &matched_types {
            match resource_facade
                .get_handle(ResourceSelector {
                    account: None,
                    resource_type: (&type_descriptor.canonical_selector).into(),
                    resource_ref: resource_ref.clone(),
                })
                .await
            {
                Ok(handle) => {
                    had_any_match = true;
                    let target_key = Self::target_key_from_handle(&handle);

                    if seen_target_keys.contains(&target_key)
                        || !local_seen_target_keys.insert(target_key)
                    {
                        continue;
                    }

                    targets.push(Self::target_from_handle(
                        handle,
                        type_descriptor.canonical_selector.clone(),
                        selector_input.clone(),
                    ));

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
                    type_descriptor: matched_types
                        .first()
                        .expect("matched types should be non-empty")
                        .clone(),
                    selector_input,
                });
                return Ok(Vec::new());
            }

            return Err(Self::type_pattern_exact_selector_not_found_error(
                &type_pattern,
                &resource_ref,
            ));
        }

        Ok(targets)
    }

    async fn process_type_pattern_name_pattern_item(
        resource_facade: &dyn ResourceFacade,
        supported_resource_types: &[ResourceTypeDescriptor],
        seen_target_keys: &HashSet<ResourceTargetKey>,
        type_pattern: String,
        selector_input: String,
        name_pattern: String,
        expanded_results: usize,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let matched_types = Self::matched_type_descriptors(supported_resource_types, &type_pattern);

        if matched_types.is_empty() {
            return Err(Self::unsupported_resource_type_pattern_error(
                supported_resource_types,
                &type_pattern,
            ));
        }

        let matched_resource_type_selectors = matched_types
            .iter()
            .map(|descriptor| (&descriptor.canonical_selector).into())
            .collect::<Vec<_>>();

        let canonical_selectors_by_schema = Self::canonical_selectors_by_schema(&matched_types);

        let collected = Self::collect_unique_bounded_identities(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            seen_target_keys,
            |pagination| {
                let request_resource_types = matched_resource_type_selectors.clone();
                let request_name_pattern = name_pattern.clone();
                async move {
                    resource_facade
                        .search_handles(SearchResourceHandlesRequest {
                            raw_type_selectors: request_resource_types,
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
                    type_descriptor: matched_types
                        .first()
                        .expect("matched types should be non-empty")
                        .clone(),
                    selector_input,
                });
                return Ok(Vec::new());
            }

            return Err(Self::type_pattern_name_pattern_not_found_error(
                &type_pattern,
                &name_pattern,
            ));
        }

        collected
            .identities
            .into_iter()
            .map(|handle| {
                let canonical_selector = Self::canonical_selector_for_schema(
                    &canonical_selectors_by_schema,
                    &handle.r#type,
                )?;
                Ok(Self::target_from_handle(
                    handle,
                    canonical_selector.clone(),
                    selector_input.clone(),
                ))
            })
            .collect()
    }

    fn process_exact_item(
        selector: crate::resources::ResourceExactSelector,
        exact_results: &mut std::vec::IntoIter<Result<ResourceHandle, GetResourceError>>,
        seen_target_keys: &mut HashSet<ResourceTargetKey>,
        targets: &mut Vec<ResourceTarget>,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: ResourceSelectionResolutionOptions,
    ) -> Result<(), CLIError> {
        match exact_results
            .next()
            .expect("Every exact selector must have a batch result")
        {
            Ok(handle) => {
                let target = Self::target_from_handle(
                    handle,
                    selector.type_descriptor.canonical_selector,
                    selector.selector_input,
                );

                if seen_target_keys.insert(Self::target_key(&target)) {
                    targets.push(target);
                }
            }
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::IDNotFound(_),
            )) if options.ignore_not_found => {
                ignored_selectors.push(ResourceIgnoredSelector {
                    type_descriptor: selector.type_descriptor,
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
        Fut: Future<Output = Result<Vec<ResourceHandle>, CLIError>>,
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

            for handle in page_items {
                let target_key = Self::target_key_from_handle(&handle);

                if seen_target_keys.contains(&target_key)
                    || !local_seen_target_keys.insert(target_key)
                {
                    continue;
                }

                items.push(handle);

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

    fn matched_type_descriptors(
        supported_resource_types: &[ResourceTypeDescriptor],
        type_pattern: &str,
    ) -> Vec<ResourceTypeDescriptor> {
        supported_resource_types
            .iter()
            .filter(|descriptor| descriptor.matches_selector_pattern(type_pattern))
            .cloned()
            .collect()
    }

    fn canonical_selectors_by_schema(
        resource_types: &[ResourceTypeDescriptor],
    ) -> CanonicalSelectorsBySchema<'_> {
        resource_types
            .iter()
            .map(|descriptor| (&descriptor.schema, &descriptor.canonical_selector))
            .collect()
    }

    fn unsupported_resource_type_pattern_error(
        supported_resource_types: &[ResourceTypeDescriptor],
        type_pattern: &str,
    ) -> CLIError {
        CLIError::usage_error(format!(
            "Unsupported get target '{type_pattern}'. Supported targets: {}",
            Self::supported_targets(supported_resource_types).join(", ")
        ))
    }

    fn name_pattern_not_found_error(
        type_descriptor: &ResourceTypeDescriptor,
        name_pattern: &str,
    ) -> CLIError {
        CLIError::usage_error(format!(
            "Pattern `{name_pattern}` did not match any {}",
            type_descriptor.canonical_selector
        ))
    }

    fn type_pattern_exact_selector_not_found_error(
        type_pattern: &str,
        resource_ref: &kamu_resources_facade::ResourceRef,
    ) -> CLIError {
        let selector = match resource_ref {
            kamu_resources_facade::ResourceRef::ById(id) => id.to_string(),
            kamu_resources_facade::ResourceRef::ByName(name) => name.to_string(),
        };

        CLIError::usage_error(format!(
            "Selector `{selector}` did not match any resource type matched by `{type_pattern}`"
        ))
    }

    fn type_pattern_name_pattern_not_found_error(
        type_pattern: &str,
        name_pattern: &str,
    ) -> CLIError {
        CLIError::usage_error(format!(
            "Pattern `{name_pattern}` did not match any resource type matched by `{type_pattern}`"
        ))
    }

    fn supported_targets(supported_resource_types: &[ResourceTypeDescriptor]) -> Vec<String> {
        let mut targets = Vec::new();

        for descriptor in supported_resource_types {
            targets.push(descriptor.canonical_selector.to_string());
            targets.extend(descriptor.selector_aliases.iter().map(ToString::to_string));
        }

        targets.sort();
        targets.dedup();
        targets
    }

    fn target_from_handle(
        handle: ResourceHandle,
        canonical_selector: kamu_resources::ResourceSelectorName,
        selector_input: String,
    ) -> ResourceTarget {
        ResourceTarget {
            canonical_selector,
            schema: handle.r#type,
            id: handle.id,
            name: handle.name,
            selector_input,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type ResourceTargetKey = (kamu_resources::TypeUri, kamu_resources::ResourceID);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type CanonicalSelectorsBySchema<'a> =
    HashMap<&'a kamu_resources::TypeUri, &'a kamu_resources::ResourceSelectorName>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CollectedUniqueIdentities {
    identities: Vec<ResourceHandle>,
    had_any_match: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
