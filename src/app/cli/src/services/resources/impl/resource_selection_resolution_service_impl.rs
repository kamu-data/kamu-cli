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
    ResourceFacade,
    ResourceLookupProblem,
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
        options: &ResourceSelectionResolutionOptions,
    ) -> Result<ResourceSelectionResolution, CLIError> {
        let mut targets = Vec::with_capacity(selection.items.len());
        let mut ignored_selectors = Vec::new();
        let mut expanded_results = 0;
        let mut seen_target_keys = HashSet::new();

        let supported_resource_types =
            Self::supported_resource_types_for_cross_type_items(&selection, resource_facade)
                .await?;

        // Prefetch exact-any-type selectors (bare IDs) in batches, replayed by index.
        let exact_any_type_results = Self::fetch_exact_any_type_identities(
            &selection,
            resource_facade,
            options.label_filter.as_ref(),
        )
        .await?;
        let mut exact_any_type_results = exact_any_type_results.into_iter();

        // Prefetch exact selectors in batches, then replay them in input order.
        let exact_results = Self::fetch_exact_identities(
            &selection,
            resource_facade,
            options.label_filter.as_ref(),
        )
        .await?;
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

                ResourceSelectionItem::ExactAnyType { selector_input, .. } => {
                    let matched_resource_types = supported_resource_types
                        .as_deref()
                        .expect("ExactAnyType requires supported types");
                    Self::process_exact_any_type_item(
                        selector_input,
                        matched_resource_types,
                        &mut exact_any_type_results,
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

                ResourceSelectionItem::AnyTypeExactRef {
                    selector_input,
                    resource_ref,
                } => {
                    let all_resource_types = supported_resource_types
                        .as_deref()
                        .expect("AnyTypeExactRef requires supported types");
                    let new_targets = Self::process_any_type_exact_ref_item(
                        resource_facade,
                        all_resource_types,
                        &seen_target_keys,
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

                ResourceSelectionItem::AnyTypeNamePattern {
                    selector_input,
                    name_pattern,
                } => {
                    let all_resource_types = supported_resource_types
                        .as_deref()
                        .expect("AnyTypeNamePattern requires supported types");
                    let new_targets = Self::process_any_type_name_pattern_item(
                        resource_facade,
                        all_resource_types,
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

    /// Cross-type items label their results by each handle's own schema, so
    /// they need the full descriptor list; single-type items already carry it.
    async fn supported_resource_types_for_cross_type_items(
        selection: &ResourceSelectionSyntax,
        resource_facade: &dyn ResourceFacade,
    ) -> Result<Option<Vec<ResourceTypeDescriptor>>, CLIError> {
        let needs_supported_types = selection.items.iter().any(|item| {
            matches!(
                item,
                ResourceSelectionItem::All
                    | ResourceSelectionItem::ExactAnyType { .. }
                    | ResourceSelectionItem::AnyTypeExactRef { .. }
                    | ResourceSelectionItem::AnyTypeNamePattern { .. }
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
        label_filter: Option<&kamu_resources::ResourceLabelFilterInput>,
    ) -> Result<Vec<Result<ResourceHandle, GetResourceError>>, CLIError> {
        let exact_selectors = selection
            .items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| match item {
                ResourceSelectionItem::Exact(selector) => Some((
                    index,
                    selector.type_descriptor.canonical_selector.clone().into(),
                    selector.type_descriptor.schema.clone(),
                    selector.resource_ref.clone(),
                )),
                ResourceSelectionItem::All
                | ResourceSelectionItem::AllByType { .. }
                | ResourceSelectionItem::ExactAnyType { .. }
                | ResourceSelectionItem::NamePattern { .. }
                | ResourceSelectionItem::AnyTypeExactRef { .. }
                | ResourceSelectionItem::AnyTypeNamePattern { .. } => None,
            })
            .collect::<Vec<_>>();

        let exact_request_count = exact_selectors.len();
        let mut exact_results = (0..exact_request_count)
            .map(|_| None)
            .collect::<Vec<Option<Result<ResourceHandle, GetResourceError>>>>();
        let mut groups: BTreeMap<
            kamu_resources::ResourceTypeSelectorRaw,
            (kamu_resources::TypeUri, Vec<_>),
        > = BTreeMap::new();

        for (exact_index, (_, resource_type, schema, resource_ref)) in
            exact_selectors.into_iter().enumerate()
        {
            groups
                .entry(resource_type)
                .or_insert_with(|| (schema, Vec::new()))
                .1
                .push((exact_index, resource_ref));
        }

        for (resource_type, (schema, entries)) in groups {
            // `search_handles` carries one query mode at a time.
            let mut by_id = Vec::new();
            let mut by_name = Vec::new();
            for (exact_index, resource_ref) in &entries {
                match resource_ref {
                    kamu_resources_facade::ResourceRef::ById(id) => by_id.push((*exact_index, *id)),
                    kamu_resources_facade::ResourceRef::ByName(name) => {
                        by_name.push((*exact_index, name.clone()));
                    }
                }
            }

            if !by_id.is_empty() {
                let ids = by_id.iter().map(|(_, id)| *id).collect::<Vec<_>>();
                let found = resource_facade
                    .search_handles(SearchResourceHandlesRequest {
                        type_scope: kamu_resources_facade::SearchResourceTypeScope::Types(vec![
                            resource_type.clone(),
                        ]),
                        query: kamu_resources::ResourceSearchQuery::ExactIds(ids),
                        account: None,
                        label_filter: label_filter.cloned(),
                        pagination: PaginationOpts {
                            limit: by_id.len(),
                            offset: 0,
                        },
                    })
                    .await?
                    .items
                    .into_iter()
                    .map(|handle| (handle.id, handle))
                    .collect::<HashMap<_, _>>();

                for (exact_index, id) in by_id {
                    exact_results[exact_index] = Some(match found.get(&id) {
                        Some(handle) => Ok(handle.clone()),
                        None => Err(GetResourceError::LookupProblem(
                            kamu_resources_facade::ResourceLookupProblem::IDNotFound(
                                kamu_resources::ResourceIDNotFoundError(id),
                            ),
                        )),
                    });
                }
            }

            if !by_name.is_empty() {
                let names = by_name
                    .iter()
                    .map(|(_, name)| name.clone())
                    .collect::<Vec<_>>();
                let found = resource_facade
                    .search_handles(SearchResourceHandlesRequest {
                        type_scope: kamu_resources_facade::SearchResourceTypeScope::Types(vec![
                            resource_type.clone(),
                        ]),
                        query: kamu_resources::ResourceSearchQuery::ExactNames(names),
                        account: None,
                        label_filter: label_filter.cloned(),
                        pagination: PaginationOpts {
                            limit: by_name.len(),
                            offset: 0,
                        },
                    })
                    .await?
                    .items
                    .into_iter()
                    .map(|handle| (handle.name.to_ascii_lowercase(), handle))
                    .collect::<HashMap<_, _>>();

                // Registered descriptor schemas must parse back into TypeUri.
                let type_name =
                    kamu_resources::resource_type_name(&schema).map_err(CLIError::critical)?;

                for (exact_index, name) in by_name {
                    exact_results[exact_index] =
                        Some(match found.get(&name.to_ascii_lowercase()) {
                            Some(handle) => Ok(handle.clone()),
                            None => Err(GetResourceError::LookupProblem(
                                kamu_resources_facade::ResourceLookupProblem::NameNotFound(
                                    kamu_resources::ResourceNameNotFoundError {
                                        type_name: type_name.clone(),
                                        name,
                                    },
                                ),
                            )),
                        });
                }
            }
        }

        Ok(exact_results.into_iter().flatten().collect())
    }

    /// Batches `ExactAnyType` selectors (bare IDs) into one search spanning
    /// every resource type, with no schema filter needed.
    async fn fetch_exact_any_type_identities(
        selection: &ResourceSelectionSyntax,
        resource_facade: &dyn ResourceFacade,
        label_filter: Option<&kamu_resources::ResourceLabelFilterInput>,
    ) -> Result<Vec<Result<ResourceHandle, GetResourceError>>, CLIError> {
        let ids = selection
            .items
            .iter()
            .filter_map(|item| match item {
                ResourceSelectionItem::ExactAnyType { resource_ref, .. } => match resource_ref {
                    kamu_resources_facade::ResourceRef::ById(id) => Some(*id),
                    kamu_resources_facade::ResourceRef::ByName(_) => None,
                },
                _ => None,
            })
            .collect::<Vec<_>>();

        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let found = resource_facade
            .search_handles(SearchResourceHandlesRequest {
                type_scope: kamu_resources_facade::SearchResourceTypeScope::AnyType,
                query: kamu_resources::ResourceSearchQuery::ExactIds(ids.clone()),
                account: None,
                label_filter: label_filter.cloned(),
                pagination: PaginationOpts {
                    limit: ids.len(),
                    offset: 0,
                },
            })
            .await?
            .items
            .into_iter()
            .map(|handle| (handle.id, handle))
            .collect::<HashMap<_, _>>();

        Ok(ids
            .into_iter()
            .map(|id| match found.get(&id) {
                Some(handle) => Ok(handle.clone()),
                None => Err(GetResourceError::LookupProblem(
                    kamu_resources_facade::ResourceLookupProblem::IDNotFound(
                        kamu_resources::ResourceIDNotFoundError(id),
                    ),
                )),
            })
            .collect())
    }

    fn process_exact_any_type_item(
        selector_input: String,
        supported_resource_types: &[ResourceTypeDescriptor],
        exact_any_type_results: &mut std::vec::IntoIter<Result<ResourceHandle, GetResourceError>>,
        seen_target_keys: &mut HashSet<ResourceTargetKey>,
        targets: &mut Vec<ResourceTarget>,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: &ResourceSelectionResolutionOptions,
    ) -> Result<(), CLIError> {
        let result = exact_any_type_results
            .next()
            .expect("Every ExactAnyType selector must have a batch result");

        match result {
            Ok(handle) => {
                let canonical_selectors_by_schema =
                    Self::canonical_selectors_by_schema(supported_resource_types);
                let canonical_selector = Self::canonical_selector_for_schema(
                    &canonical_selectors_by_schema,
                    &handle.r#type,
                )?
                .clone();

                let target = Self::target_from_handle(handle, canonical_selector, selector_input);

                if seen_target_keys.insert(Self::target_key(&target)) {
                    targets.push(target);
                }
            }
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::IDNotFound(_),
            )) if options.ignore_not_found => {
                // Type is unknown for an unmatched ID; only `selector_input` is
                // actually surfaced, so any type descriptor works as a placeholder.
                let type_descriptor = supported_resource_types
                    .first()
                    .expect("at least one resource type must be supported")
                    .clone();
                ignored_selectors.push(ResourceIgnoredSelector {
                    type_descriptor,
                    selector_input,
                });
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    async fn process_all_item(
        resource_facade: &dyn ResourceFacade,
        supported_resource_types: &[ResourceTypeDescriptor],
        seen_target_keys: &HashSet<ResourceTargetKey>,
        expanded_results: usize,
        options: &ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let collected = Self::collect_unique_bounded_identities(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            seen_target_keys,
            |pagination| {
                let request_label_filter = options.label_filter.clone();
                async move {
                    resource_facade
                        .list_all_handles(ListAllResourceHandlesRequest {
                            account: None,
                            label_filter: request_label_filter,
                            pagination,
                        })
                        .await
                        .map_err(Into::into)
                }
            },
        )
        .await?;

        // Reconstruct command-routing selectors from the loaded descriptors.
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

    /// Resolves the canonical selector for a handle's schema.
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
        options: &ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let collected = Self::collect_unique_bounded_identities(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            seen_target_keys,
            |pagination| {
                let request_label_filter = options.label_filter.clone();
                async move {
                    resource_facade
                        .list_handles(ListResourceHandlesRequest {
                            raw_type_selector: (&type_descriptor.canonical_selector).into(),
                            account: None,
                            label_filter: request_label_filter,
                            pagination,
                        })
                        .await
                        .map_err(Into::into)
                }
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

    async fn process_name_pattern_item(
        resource_facade: &dyn ResourceFacade,
        type_descriptor: &ResourceTypeDescriptor,
        seen_target_keys: &HashSet<ResourceTargetKey>,
        selector_input: String,
        name_pattern: String,
        expanded_results: usize,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: &ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let collected = Self::collect_unique_bounded_identities(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            seen_target_keys,
            |pagination| {
                let request_name_pattern = name_pattern.clone();
                let request_label_filter = options.label_filter.clone();
                async move {
                    resource_facade
                        .search_handles(SearchResourceHandlesRequest {
                            type_scope: kamu_resources_facade::SearchResourceTypeScope::Types(
                                vec![(&type_descriptor.canonical_selector).into()],
                            ),
                            query: kamu_resources::ResourceSearchQuery::NamePattern(
                                request_name_pattern,
                            ),
                            account: None,
                            label_filter: request_label_filter,
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

    /// `%/<name>` or `%/<id>` — one exact reference, searched across every
    /// type.
    async fn process_any_type_exact_ref_item(
        resource_facade: &dyn ResourceFacade,
        supported_resource_types: &[ResourceTypeDescriptor],
        seen_target_keys: &HashSet<ResourceTargetKey>,
        selector_input: String,
        resource_ref: kamu_resources_facade::ResourceRef,
        expanded_results: usize,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: &ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let query = match &resource_ref {
            kamu_resources_facade::ResourceRef::ById(id) => {
                kamu_resources::ResourceSearchQuery::ExactIds(vec![*id])
            }
            kamu_resources_facade::ResourceRef::ByName(name) => {
                kamu_resources::ResourceSearchQuery::ExactNames(vec![name.clone()])
            }
        };

        let collected = Self::collect_unique_bounded_identities(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            seen_target_keys,
            |pagination| {
                let request_query = query.clone();
                let request_label_filter = options.label_filter.clone();
                async move {
                    resource_facade
                        .search_handles(SearchResourceHandlesRequest {
                            type_scope: kamu_resources_facade::SearchResourceTypeScope::AnyType,
                            query: request_query,
                            account: None,
                            label_filter: request_label_filter,
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
                Self::push_any_type_ignored_selector(
                    ignored_selectors,
                    supported_resource_types,
                    selector_input,
                );
                return Ok(Vec::new());
            }

            return Err(Self::any_type_exact_selector_not_found_error(&resource_ref));
        }

        Self::targets_labelled_by_schema(
            collected.identities,
            supported_resource_types,
            &selector_input,
        )
    }

    /// `%/<pattern>` — one name pattern, searched across every type.
    async fn process_any_type_name_pattern_item(
        resource_facade: &dyn ResourceFacade,
        supported_resource_types: &[ResourceTypeDescriptor],
        seen_target_keys: &HashSet<ResourceTargetKey>,
        selector_input: String,
        name_pattern: String,
        expanded_results: usize,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: &ResourceSelectionResolutionOptions,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let collected = Self::collect_unique_bounded_identities(
            Self::remaining_expanded_results(expanded_results, options),
            options.max_expanded_results,
            seen_target_keys,
            |pagination| {
                let request_name_pattern = name_pattern.clone();
                let request_label_filter = options.label_filter.clone();
                async move {
                    resource_facade
                        .search_handles(SearchResourceHandlesRequest {
                            type_scope: kamu_resources_facade::SearchResourceTypeScope::AnyType,
                            query: kamu_resources::ResourceSearchQuery::NamePattern(
                                request_name_pattern,
                            ),
                            account: None,
                            label_filter: request_label_filter,
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
                Self::push_any_type_ignored_selector(
                    ignored_selectors,
                    supported_resource_types,
                    selector_input,
                );
                return Ok(Vec::new());
            }

            return Err(Self::any_type_name_pattern_not_found_error(&name_pattern));
        }

        Self::targets_labelled_by_schema(
            collected.identities,
            supported_resource_types,
            &selector_input,
        )
    }

    /// Labels each handle with the canonical selector of its own schema.
    fn targets_labelled_by_schema(
        identities: Vec<ResourceHandle>,
        supported_resource_types: &[ResourceTypeDescriptor],
        selector_input: &str,
    ) -> Result<Vec<ResourceTarget>, CLIError> {
        let canonical_selectors_by_schema =
            Self::canonical_selectors_by_schema(supported_resource_types);

        identities
            .into_iter()
            .map(|handle| {
                let canonical_selector = Self::canonical_selector_for_schema(
                    &canonical_selectors_by_schema,
                    &handle.r#type,
                )?;
                Ok(Self::target_from_handle(
                    handle,
                    canonical_selector.clone(),
                    selector_input.to_owned(),
                ))
            })
            .collect()
    }

    /// An unmatched any-type selector has no single type to attribute; only
    /// `selector_input` is surfaced, so any descriptor serves as a placeholder.
    fn push_any_type_ignored_selector(
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        supported_resource_types: &[ResourceTypeDescriptor],
        selector_input: String,
    ) {
        let type_descriptor = supported_resource_types
            .first()
            .expect("at least one resource type must be supported")
            .clone();

        ignored_selectors.push(ResourceIgnoredSelector {
            type_descriptor,
            selector_input,
        });
    }

    fn process_exact_item(
        selector: crate::resources::ResourceExactSelector,
        exact_results: &mut std::vec::IntoIter<Result<ResourceHandle, GetResourceError>>,
        seen_target_keys: &mut HashSet<ResourceTargetKey>,
        targets: &mut Vec<ResourceTarget>,
        ignored_selectors: &mut Vec<ResourceIgnoredSelector>,
        options: &ResourceSelectionResolutionOptions,
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
        options: &ResourceSelectionResolutionOptions,
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

    fn canonical_selectors_by_schema(
        resource_types: &[ResourceTypeDescriptor],
    ) -> CanonicalSelectorsBySchema<'_> {
        resource_types
            .iter()
            .map(|descriptor| (&descriptor.schema, &descriptor.canonical_selector))
            .collect()
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

    fn any_type_exact_selector_not_found_error(
        resource_ref: &kamu_resources_facade::ResourceRef,
    ) -> CLIError {
        let selector = match resource_ref {
            kamu_resources_facade::ResourceRef::ById(id) => id.to_string(),
            kamu_resources_facade::ResourceRef::ByName(name) => name.to_string(),
        };

        CLIError::usage_error(format!(
            "Selector `{selector}` did not match any resource of any type"
        ))
    }

    fn any_type_name_pattern_not_found_error(name_pattern: &str) -> CLIError {
        CLIError::usage_error(format!(
            "Pattern `{name_pattern}` did not match any resource of any type"
        ))
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
