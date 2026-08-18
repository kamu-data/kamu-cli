// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::*;
use kamu_resources_services::{
    ResourceExtensionSchemaResolver,
    get_resource_crud_dispatcher,
    get_resource_crud_dispatcher_for_trusted_schema,
};

use super::helpers::*;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceFacade)]
pub struct LocalResourceFacadeImpl {
    catalog: dill::Catalog,
    resource_account_resolver: Arc<dyn ResourceAccountResolver>,
    generic_resource_query_service: Arc<dyn GenericResourceQueryService>,
    resource_extension_schema_resolver: Arc<ResourceExtensionSchemaResolver>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceFacade for LocalResourceFacadeImpl {
    async fn list_supported_resource_types(
        &self,
    ) -> Result<Vec<ResourceTypeDescriptor>, ListSupportedResourceTypesError> {
        Ok(self.list_resource_type_descriptors())
    }

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<ResourcesSummary, ResourcesSummaryError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let resource_counts = self
            .generic_resource_query_service
            .summarize_resources(target_account.did)
            .await?
            .into_iter()
            .map(|row| {
                let schema = TypeUri::new_unchecked(row.schema);
                let type_name = resource_type_name(&schema)?;

                Ok(ResourceTypeCountSummary {
                    schema,
                    type_name,
                    total_count: row.total_count,
                    phase_counts: row.phase_counts,
                })
            })
            .collect::<Result<Vec<_>, InternalError>>()
            .map_err(ResourcesSummaryError::Internal)?;

        Ok(ResourcesSummary { resource_counts })
    }

    async fn get(
        &self,
        resource_refs: Vec<ResourceRef>,
        spec_view: SpecViewOpts,
    ) -> Result<BatchResourceResponse<Resource, ResourceLookupProblem>, BatchResourceError> {
        let (mut indexed_resources, problems) =
            self.resolve_multiple_resource_views(resource_refs).await?;

        self.apply_spec_view_mode_batch::<BatchResourceError>(&mut indexed_resources, spec_view)?;

        let successes = indexed_resources
            .into_iter()
            .map(|resource| BatchResourceSuccess {
                request_index: resource.request_index,
                item: resource.item,
            })
            .collect();

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn get_handles(
        &self,
        resource_refs: Vec<ResourceRef>,
    ) -> Result<BatchResourceResponse<ResourceHandle, ResourceLookupProblem>, BatchResourceError>
    {
        // An empty batch names nothing, so it yields an empty response.
        let mut successes = Vec::new();
        let (groups, mut problems) = self.group_refs_by_target(resource_refs).await?;

        for group in groups {
            let (indexes, refs): (Vec<_>, Vec<_>) = group.entries.into_iter().unzip();

            let grouped = group_batch_resource_refs(refs);
            let resolution_response = resolve_batch_ids(
                self.generic_resource_query_service.as_ref(),
                &group.account.did,
                &group.schema,
                grouped,
            )
            .await?;

            let (handles, group_problems) = self
                .resolve_id_handle_groups(
                    &group.account.did,
                    &group.schema,
                    resolution_response.id_entries,
                    resolution_response.problems,
                )
                .await?;

            // `group_batch_resource_refs` re-indexes from zero within the
            // group, so map back to the caller's positions before merging.
            successes.extend(handles.into_iter().map(|handle| BatchResourceSuccess {
                request_index: indexes[handle.request_index],
                item: handle.item,
            }));
            problems.extend(
                group_problems
                    .into_iter()
                    .map(|problem| BatchResourceProblem {
                        request_index: indexes[problem.request_index],
                        error: problem.error,
                    }),
            );
        }

        successes.sort_by_key(|success| success.request_index);
        problems.sort_by_key(|problem| problem.request_index);

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn render_manifests(
        &self,
        resource_refs: Vec<ResourceRef>,
        format: ResourceManifestFormat,
        spec_view: SpecViewOpts,
    ) -> Result<
        BatchResourceResponse<RenderResourceManifestResult, ResourceLookupProblem>,
        BatchResourceError,
    > {
        let (mut indexed_resources, problems) =
            self.resolve_multiple_resource_views(resource_refs).await?;

        self.apply_spec_view_mode_batch::<BatchResourceError>(&mut indexed_resources, spec_view)?;

        let successes = indexed_resources
            .into_iter()
            .map(|resource| {
                let manifest =
                    resource_to_manifest(resource.item).map_err(BatchResourceError::Internal)?;
                let manifest =
                    serialize_manifest(&manifest, format).map_err(BatchResourceError::Internal)?;
                Ok(BatchResourceSuccess {
                    request_index: resource.request_index,
                    item: RenderResourceManifestResult { manifest, format },
                })
            })
            .collect::<Result<Vec<_>, BatchResourceError>>()?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn search(
        &self,
        request: SearchResourcesRequest,
    ) -> Result<SearchResourcesResponse, ListResourcesError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let scope = self.resolve_scope(request.selectors).await?;

        if scope.is_vacuous() {
            return Ok(SearchResourcesResponse {
                items: Vec::new(),
                total_count: 0,
            });
        }

        // One scoped query covering every type in the scope, so pagination is
        // global rather than per type. Querying each type separately cannot
        // produce this: page 2 of a merged result is not page 2 of each type.
        let snapshots = self
            .generic_resource_query_service
            .list_snapshots(&target_account.did, &scope, request.pagination)
            .await?;
        let total_count = self
            .generic_resource_query_service
            .count_search_resource_handles(&target_account.did, &scope)
            .await?;

        let items = self.summary_views_with_columns(snapshots)?;

        Ok(SearchResourcesResponse { items, total_count })
    }

    async fn search_handles(
        &self,
        request: SearchResourcesRequest,
    ) -> Result<SearchResourceHandlesResponse, ListResourcesError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let scope = self.resolve_scope(request.selectors).await?;

        // Empty exact-name/id queries are vacuous.
        if scope.is_vacuous() {
            return Ok(SearchResourceHandlesResponse {
                items: Vec::new(),
                total_count: 0,
            });
        }

        let rows = self
            .generic_resource_query_service
            .search_resource_handles(&target_account.did, &scope, request.pagination)
            .await?;
        let total_count = self
            .generic_resource_query_service
            .count_search_resource_handles(&target_account.did, &scope)
            .await?;

        let items = rows
            .into_iter()
            .map(resource_handle_from_row)
            .collect::<Vec<_>>();

        Ok(SearchResourceHandlesResponse { items, total_count })
    }

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestPlanningDecision, ApplyManifestError> {
        let prepared = self.prepare_apply_manifest(request).await?;

        let plan = prepared
            .dispatcher
            .plan_apply(ResourceCrudDispatcherApplyRequest {
                id: prepared.id,
                headers: prepared.header,
                spec: prepared.spec,
            })
            .await?;

        Ok(match plan {
            ApplyManifestPlanningDecision::Planned(mut plan) => {
                plan.warnings.splice(0..0, prepared.header_warnings);
                plan.resource.headers.account = prepared.target_account;

                ApplyManifestPlanningDecision::Planned(plan)
            }
            ApplyManifestPlanningDecision::Rejected(rejection) => {
                ApplyManifestPlanningDecision::Rejected(rejection)
            }
        })
    }

    async fn apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestApplicationDecision, ApplyManifestError> {
        let prepared = self.prepare_apply_manifest(request).await?;

        let result = prepared
            .dispatcher
            .apply(ResourceCrudDispatcherApplyRequest {
                id: prepared.id,
                headers: prepared.header,
                spec: prepared.spec,
            })
            .await?;

        Ok(match result {
            ApplyManifestApplicationDecision::Applied(mut result) => {
                result.warnings.splice(0..0, prepared.header_warnings);
                result.resource.headers.account = prepared.target_account;

                ApplyManifestApplicationDecision::Applied(result)
            }
            ApplyManifestApplicationDecision::Rejected(rejection) => {
                ApplyManifestApplicationDecision::Rejected(rejection)
            }
        })
    }

    async fn plan_apply_manifests(
        &self,
        request: ApplyManifestBatchRequest,
    ) -> Result<ApplyManifestBatchResponse<ApplyManifestPlanningDecision>, BatchResourceError> {
        let mut items = Vec::with_capacity(request.items.len());

        for (request_index, item) in request.items.into_iter().enumerate() {
            let outcome = self.plan_apply_manifest(item).await;
            let stop = matches!(outcome, Ok(ApplyManifestPlanningDecision::Rejected(_)))
                || outcome.is_err();

            items.push(ApplyManifestBatchItemResult {
                request_index,
                outcome,
            });

            if stop {
                break;
            }
        }

        Ok(ApplyManifestBatchResponse {
            items,
            rolled_back_successes: Vec::new(),
        })
    }

    async fn apply_manifests(
        &self,
        request: ApplyManifestBatchRequest,
    ) -> Result<ApplyManifestBatchResponse<ApplyManifestApplicationDecision>, BatchResourceError>
    {
        let mut items = Vec::with_capacity(request.items.len());

        for (request_index, item) in request.items.into_iter().enumerate() {
            let outcome = self.apply_manifest(item).await;
            let stop = matches!(outcome, Ok(ApplyManifestApplicationDecision::Rejected(_)))
                || outcome.is_err();

            items.push(ApplyManifestBatchItemResult {
                request_index,
                outcome,
            });

            if stop {
                break;
            }
        }

        Ok(ApplyManifestBatchResponse {
            items,
            rolled_back_successes: Vec::new(),
        })
    }

    async fn delete(
        &self,
        resource_refs: Vec<ResourceRef>,
    ) -> Result<BatchResourceResponse<ResourceID, ResourceLookupProblem>, BatchResourceError> {
        // An empty batch names nothing, so it yields an empty response.
        let mut successes = Vec::new();
        let (groups, mut problems) = self.group_refs_by_target(resource_refs).await?;

        // One dispatcher call per `(account, schema)` group. The whole request
        // runs inside a single database transaction — opened by the
        // `#[transactional_handler]` GraphQL handler, or by the CLI's
        // `DatabaseTransactionRunner` — so a failure in a later group rolls back
        // the deletes an earlier one performed. Fanning out therefore does not
        // introduce partial-delete semantics.
        for group in groups {
            let (indexes, refs): (Vec<_>, Vec<_>) = group.entries.into_iter().unzip();

            let grouped = group_batch_resource_refs(refs);
            let resolution_response = resolve_batch_ids(
                self.generic_resource_query_service.as_ref(),
                &group.account.did,
                &group.schema,
                grouped,
            )
            .await?;

            problems.extend(resolution_response.problems.into_iter().map(|problem| {
                BatchResourceProblem {
                    request_index: indexes[problem.request_index],
                    error: problem.error,
                }
            }));

            let mut seen_valid_ids = HashSet::new();
            let mut ids_to_delete = Vec::<ResourceID>::new();

            let ids = resolution_response
                .id_entries
                .iter()
                .map(|(_, _, id)| *id)
                .collect::<Vec<_>>();

            let rows_by_id = self
                .generic_resource_query_service
                .find_resource_handles_by_ids(&group.account.did, &ids)
                .await?
                .into_iter()
                .map(|row| (row.id, row))
                .collect::<HashMap<_, _>>();

            for (request_index, resource_ref, id) in resolution_response.id_entries {
                let row_result = rows_by_id
                    .get(id.as_ref())
                    .cloned()
                    .ok_or_else(|| id_not_found(id));

                // Only a ref that supplied its own id asserts a name to verify:
                // for a name-keyed ref the name *is* how the id was found.
                let expected_name = if resource_ref.id.is_some() {
                    resource_ref.name.as_ref()
                } else {
                    None
                };

                match row_result.and_then(|row| {
                    validate_handle_row(
                        row,
                        &group.schema,
                        expected_name,
                        ensure_schema_matches::<ResourceLookupProblem>,
                    )
                }) {
                    Ok(_) => {
                        successes.push(BatchResourceSuccess {
                            request_index: indexes[request_index],
                            item: id,
                        });

                        if seen_valid_ids.insert(id) {
                            ids_to_delete.push(id);
                        }
                    }
                    Err(error) => problems.push(BatchResourceProblem {
                        request_index: indexes[request_index],
                        error,
                    }),
                }
            }

            if !ids_to_delete.is_empty() {
                // Registered selector schemas must have a dispatcher.
                let dispatcher = get_resource_crud_dispatcher_for_trusted_schema(
                    &self.catalog,
                    group.schema.as_str(),
                )?;
                dispatcher
                    .delete(ResourceCrudDispatcherDeleteRequest {
                        account_id: group.account.did.clone(),
                        ids: ids_to_delete,
                    })
                    .await?;
            }
        }

        successes.sort_by_key(|success| success.request_index);
        problems.sort_by_key(|problem| problem.request_index);

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves one raw type selector against an already-built descriptor list.
///
/// Building that list constructs every registered dispatcher, so callers
/// resolving more than one selector must build it once and reuse it here rather
/// than rebuilding it per selector.
///
/// Accepts anything spelling a type — a raw CLI selector (`vs`), an ODF
/// `TypeRef` (`VariableSet`), or a schema URI — since descriptors match all
/// four forms.
fn resolve_schema_in_descriptors<E>(
    descriptors: &[ResourceTypeDescriptor],
    selector: impl AsRef<str>,
) -> Result<TypeUri, E>
where
    E: From<UnsupportedResourceSelectorError>,
{
    let selector = selector.as_ref();

    descriptors
        .iter()
        .find(|descriptor| descriptor.matches_selector(selector))
        .map(|descriptor| descriptor.schema.clone())
        .ok_or_else(|| UnsupportedResourceSelectorError::NotFound {
            raw_selector: ResourceTypeSelectorRaw::new_unchecked(selector),
        })
        .map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl LocalResourceFacadeImpl {
    fn list_resource_type_descriptors(&self) -> Vec<ResourceTypeDescriptor> {
        let mut seen = HashSet::new();
        let mut descriptors = Vec::new();

        for builder in self
            .catalog
            .builders_for::<dyn ResourcePresentationDispatcher>()
        {
            let dispatcher = builder
                .get(&self.catalog)
                .expect("Resource presentation dispatcher construction failed");

            let schema = dispatcher.schema();
            let presentation = dispatcher.presentation();

            if seen.insert(schema) {
                descriptors.push(ResourceTypeDescriptor {
                    canonical_selector: presentation.canonical_selector,
                    selector_aliases: presentation.selector_aliases.to_vec(),
                    schema: schema.clone(),
                    list_columns: presentation
                        .list_columns
                        .iter()
                        .copied()
                        .map(Into::into)
                        .collect(),
                });
            }
        }

        descriptors.sort_by(|a, b| a.canonical_selector.cmp(&b.canonical_selector));

        descriptors
    }

    /// Resolves the one schema a ref addresses, searching every registered type
    /// when the ref names none.
    ///
    /// A [`ResourceRef`] names *exactly one* resource, so a type-less ref that
    /// matches in several types is ambiguous rather than a multi-match: it is
    /// reported as [`ResourceLookupProblem::AmbiguousType`] instead of picking
    /// a winner. Contrast a type-less `ResourceSelector`, for which several
    /// matches are the expected outcome.
    async fn resolve_ref_schema(
        &self,
        descriptors: &[ResourceTypeDescriptor],
        account_id: &odf::AccountID,
        resource_ref: &ResourceRef,
    ) -> Result<Result<TypeUri, ResourceLookupProblem>, BatchResourceError> {
        let Some(r#type) = resource_ref.r#type.as_ref() else {
            return self
                .search_ref_schema(descriptors, account_id, resource_ref)
                .await;
        };

        Ok(Ok(resolve_schema_in_descriptors::<BatchResourceError>(
            descriptors,
            r#type,
        )?))
    }

    /// Finds which registered type holds the resource a type-less ref names.
    async fn search_ref_schema(
        &self,
        descriptors: &[ResourceTypeDescriptor],
        account_id: &odf::AccountID,
        resource_ref: &ResourceRef,
    ) -> Result<Result<TypeUri, ResourceLookupProblem>, BatchResourceError> {
        // An id is globally unique, so the stored row already carries the
        // schema — no per-type search needed.
        if let Some(id) = resource_ref.id {
            let rows = self
                .generic_resource_query_service
                .find_resource_handles_by_ids(account_id, &[id])
                .await?;

            return Ok(match rows.into_iter().next() {
                Some(row) => Ok(TypeUri::new_unchecked(row.schema)),
                None => Err(id_not_found(id)),
            });
        }

        let Some(name) = resource_ref.name.as_ref() else {
            return Ok(Err(ResourceLookupProblem::EmptyRef));
        };

        let mut matched = Vec::new();
        for descriptor in descriptors {
            if self
                .generic_resource_query_service
                .find_resource_id_by_name(account_id, &descriptor.schema, name)
                .await?
                .is_some()
            {
                matched.push(descriptor.schema.clone());
            }
        }

        Ok(match matched.len() {
            1 => Ok(matched.into_iter().next().unwrap()),
            0 => Err(ResourceLookupProblem::AnyTypeNameNotFound(
                ResourceAnyTypeNameNotFoundError { name: name.clone() },
            )),
            _ => Err(ResourceLookupProblem::AmbiguousType(
                ResourceAmbiguousTypeError {
                    name: name.clone(),
                    type_names: matched
                        .iter()
                        .map(resource_type_name)
                        .collect::<Result<Vec<_>, _>>()?,
                },
            )),
        })
    }

    /// Resolves every ref's account and schema, then splits the batch into
    /// `(account, schema)` groups.
    ///
    /// The shared front half of all three batch-ref pipelines. Accounts are
    /// resolved in one deduplicated pass and schemas against a descriptor list
    /// built once, so a batch costs one account lookup per distinct spelling
    /// and no per-ref catalog work.
    ///
    /// An unresolvable account or an *unknown named* type fails the **whole**
    /// call rather than becoming a per-item problem: both are addressing errors
    /// in the request rather than facts about stored data.
    ///
    /// A **type-less** ref is different: resolving it is a lookup against
    /// stored data, so a miss or an ambiguity is that one ref's problem and is
    /// returned alongside the groups for the caller to merge by request index.
    /// Because all three pipelines share this function, they inherit type-less
    /// refs together rather than one at a time.
    async fn group_refs_by_target(
        &self,
        resource_refs: Vec<ResourceRef>,
    ) -> Result<
        (
            Vec<BatchTargetGroup>,
            Vec<BatchResourceProblem<ResourceLookupProblem>>,
        ),
        BatchResourceError,
    > {
        let account_refs = distinct_account_refs(&resource_refs);
        let resolved_accounts = self
            .resource_account_resolver
            .resolve_target_accounts(&account_refs)
            .await?;

        let descriptors = self.list_resource_type_descriptors();

        let mut entries = Vec::new();
        let mut problems = Vec::new();

        for (request_index, resource_ref) in resource_refs.into_iter().enumerate() {
            let account_position = account_refs
                .iter()
                .position(|account| account == &resource_ref.account)
                .expect("every ref's account is in the deduplicated list");
            let account = resolved_accounts[account_position].clone();

            match self
                .resolve_ref_schema(&descriptors, &account.did, &resource_ref)
                .await?
            {
                Ok(schema) => entries.push((request_index, resource_ref, account, schema)),
                Err(error) => problems.push(BatchResourceProblem {
                    request_index,
                    error,
                }),
            }
        }

        Ok((group_by_account_and_schema(entries), problems))
    }

    /// Converts snapshots into summary views, rendering each one's typed list
    /// columns.
    ///
    /// The results may span several types, so presentation dispatchers are
    /// looked up **once per distinct schema** rather than per row — otherwise a
    /// page of N results would construct N dispatchers.
    fn summary_views_with_columns(
        &self,
        snapshots: Vec<ResourceSnapshot>,
    ) -> Result<Vec<ResourceSummaryView>, InternalError> {
        let mut dispatchers: HashMap<TypeUri, Arc<dyn ResourcePresentationDispatcher>> =
            HashMap::new();

        for builder in self
            .catalog
            .builders_for::<dyn ResourcePresentationDispatcher>()
        {
            let dispatcher = builder.get(&self.catalog).int_err()?;
            dispatchers
                .entry(dispatcher.schema().clone())
                .or_insert(dispatcher);
        }

        snapshots
            .into_iter()
            .map(|snapshot| {
                // A schema with no presentation dispatcher is not selectable in
                // the first place — the descriptor list this scope was resolved
                // against is built from exactly this registry — so a miss means
                // stored data of a type that is no longer registered. Render it
                // without typed columns rather than failing the whole listing.
                let list_values = match dispatchers.get(&snapshot.schema) {
                    Some(dispatcher) => dispatcher.list_column_values_for_snapshot(&snapshot)?,
                    None => Vec::new(),
                };

                let mut view = ResourceSummaryView::from(snapshot);
                view.list_values = list_values;
                Ok(view)
            })
            .collect()
    }

    /// Resolves a request's selectors into a repository scope, each carrying
    /// its own resolved label pairs.
    ///
    /// Each type keeps its own query *and* its own filter through resolution,
    /// so a multi-type scope like `vs/a-% ss/b-%` stays intact and two
    /// selectors filtering by different labels stay apart.
    async fn resolve_scope(
        &self,
        selectors: Vec<ResourceSelector>,
    ) -> Result<ResourceScope, ListResourcesError> {
        // Before the fast path below: a selector narrowed *only* by a field the
        // facade cannot resolve reads as unnarrowed there and would widen into
        // "every resource". The trait is public, so this is the boundary that
        // has to hold — not the GraphQL adapter alone.
        for selector in &selectors {
            validate_selector(selector)?;
        }

        // A lone type-less, unnarrowed, account-less, unlabelled selector needs
        // no schemas at all, so it can answer before touching the catalog or
        // resolving any account.
        if let [selector] = selectors.as_slice()
            && selector.r#type.is_none()
            && selector.id.is_none()
            && selector.name.is_none()
            && selector.account.is_none()
            && selector.labels.is_none()
        {
            return Ok(ResourceScope::any_type());
        }

        // Resolved in one batch, deduplicated by spelling, with the permission
        // check applied per distinct account. Any denial fails the whole call.
        //
        // `None` stays `None` rather than resolving to the caller's own
        // account: the repository takes the call-level account as the default
        // for exactly those rows, so resolving here would be redundant work and
        // would lose the "unset" distinction the scope relies on.
        let account_refs = selectors
            .iter()
            .map(|selector| selector.account.clone())
            .collect::<Vec<_>>();
        let account_ids = if account_refs.iter().all(Option::is_none) {
            vec![None; account_refs.len()]
        } else {
            self.resource_account_resolver
                .resolve_target_accounts(&account_refs)
                .await?
                .into_iter()
                .zip(account_refs.iter())
                .map(|(handle, requested)| requested.as_ref().map(|_| handle.did))
                .collect()
        };

        // Built once: constructing it instantiates every registered dispatcher,
        // so every selector resolves against this one list.
        let descriptors = self.list_resource_type_descriptors();

        // Every registered schema, the fallback a type-less selector resolves
        // its labels against.
        let all_schema_ids = descriptors
            .iter()
            .map(|descriptor| ResourceSchemaId::try_from(&descriptor.schema).int_err())
            .collect::<Result<Vec<_>, _>>()?;

        let mut applicable = Vec::with_capacity(selectors.len());

        for (selector, account_id) in selectors.into_iter().zip(account_ids) {
            let schema = selector
                .r#type
                .as_ref()
                .map(|r#type| {
                    resolve_schema_in_descriptors::<ListResourcesError>(&descriptors, r#type)
                })
                .transpose()?;

            // A typed selector resolves its labels against its own schema; a
            // type-less one against every registered schema.
            let candidate_schema_ids = match &schema {
                Some(schema) => vec![ResourceSchemaId::try_from(schema).int_err()?],
                None => all_schema_ids.clone(),
            };

            // Resolution can drop schemas that cannot carry this selector's
            // labels. With a single candidate that is an error rather than a
            // silent miss — the `n = 1` contract of
            // `resolve_label_filter_for_schemas`.
            let (applicable_schema_ids, resolved_label_filter) = resolve_label_filter_for_schemas(
                &self.resource_extension_schema_resolver,
                selector.labels.clone(),
                &candidate_schema_ids,
            )?;

            // This selector's schema cannot carry its labels, so it can never
            // match. Drop it rather than widening the scope.
            if applicable_schema_ids.is_empty() {
                continue;
            }

            // Flattening is what rejects `$not`/`$or`: the repository evaluates
            // a conjunction of pairs, nothing richer.
            let label_pairs =
                ResourceLabelFilterPredicate::flatten_conjunction(&resolved_label_filter)
                    .map_err(|err| {
                        ListResourcesError::InvalidLabelFilter(
                            ResourceInvalidLabelFilterError::unsupported_expression(err),
                        )
                    })?
                    .into_iter()
                    .map(|(key, value)| (key.clone(), value.to_owned()))
                    .collect::<Vec<_>>();

            applicable.push(ResolvedSelector {
                schema,
                id: selector.id,
                // Authored selector names are `LIKE` patterns by definition;
                // an exact name arrives as a `ResourceRef`, not here.
                name: None,
                name_pattern: selector.name,
                account_id,
                label_pairs,
            });
        }

        // `None` means nothing can match, which the repository expresses as an
        // empty type list.
        let scope = coalesce_selectors(applicable)?.unwrap_or(ResourceScope::Types(Vec::new()));

        Ok(scope)
    }

    async fn resolve_multiple_resource_views(
        &self,
        resource_refs: Vec<ResourceRef>,
    ) -> Result<
        (
            Vec<IndexedResource<Resource>>,
            Vec<BatchResourceProblem<ResourceLookupProblem>>,
        ),
        BatchResourceError,
    > {
        // An empty batch names nothing, so it yields an empty result.
        let mut indexed_resources = Vec::new();
        let (groups, mut problems) = self.group_refs_by_target(resource_refs).await?;

        for group in groups {
            let (indexes, refs): (Vec<_>, Vec<_>) = group.entries.into_iter().unzip();

            // Batch refs name their targets explicitly.
            let grouped = group_batch_resource_refs(refs);

            let resolution_response = resolve_batch_ids(
                self.generic_resource_query_service.as_ref(),
                &group.account.did,
                &group.schema,
                grouped,
            )
            .await?;

            problems.extend(resolution_response.problems.into_iter().map(|problem| {
                BatchResourceProblem {
                    request_index: indexes[problem.request_index],
                    error: problem.error,
                }
            }));

            let ids = resolution_response
                .id_entries
                .iter()
                .map(|(_, _, id)| *id)
                .collect::<Vec<_>>();

            let snapshots_by_id = self
                .generic_resource_query_service
                .find_snapshots_by_ids(&group.account.did, &ids)
                .await?
                .into_iter()
                .map(|snapshot| (snapshot.id, snapshot))
                .collect::<HashMap<_, _>>();

            for (request_index, resource_ref, id) in resolution_response.id_entries {
                // Only a ref that supplied its own id asserts a name to verify:
                // for a name-keyed ref the name *is* how the id was found.
                let expected_name = if resource_ref.id.is_some() {
                    resource_ref.name.clone()
                } else {
                    None
                };

                match snapshots_by_id
                    .get(&id)
                    .cloned()
                    .ok_or(ResourceLookupProblem::IDNotFound(ResourceIDNotFoundError(
                        id,
                    )))
                    .and_then(|snapshot| {
                        ensure_schema_matches::<ResourceLookupProblem>(
                            snapshot.id,
                            &group.schema,
                            snapshot.schema.as_str(),
                        )?;
                        // The other half of the `id` + `name` consistency
                        // assertion; without it the id silently wins and the
                        // caller reads a resource they did not name.
                        if let Some(expected_name) = expected_name
                            && expected_name != snapshot.headers.name
                        {
                            return Err(ResourceLookupProblem::NameMismatch(
                                ResourceNameMismatchError {
                                    id: snapshot.id,
                                    expected_name,
                                    actual_name: snapshot.headers.name.clone(),
                                },
                            ));
                        }
                        Ok(snapshot)
                    }) {
                    Ok(snapshot) => {
                        let resource = Resource {
                            schema: snapshot.schema,
                            headers: kamu_resources::ResourceHeaders {
                                account: group.account.clone(),
                                ..snapshot.headers
                            },
                            spec: snapshot.spec,
                            status: snapshot.status.unwrap_or_else(new_pending_resource_status),
                        };

                        indexed_resources.push(IndexedResource {
                            request_index: indexes[request_index],
                            item: resource,
                        });
                    }
                    Err(error) => {
                        problems.push(BatchResourceProblem {
                            request_index: indexes[request_index],
                            error,
                        });
                    }
                }
            }
        }

        indexed_resources.sort_by_key(|resource| resource.request_index);
        problems.sort_by_key(|problem| problem.request_index);

        Ok((indexed_resources, problems))
    }

    async fn resolve_id_handle_groups(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        id_entries: BatchIdEntries,
        mut problems: Vec<BatchResourceProblem<ResourceLookupProblem>>,
    ) -> Result<
        (
            Vec<IndexedResource<ResourceHandle>>,
            Vec<BatchResourceProblem<ResourceLookupProblem>>,
        ),
        BatchResourceError,
    > {
        let ids = id_entries.iter().map(|(_, _, id)| *id).collect::<Vec<_>>();

        let rows_by_id = self
            .generic_resource_query_service
            .find_resource_handles_by_ids(account_id, &ids)
            .await?
            .into_iter()
            .map(|row| (row.id, row))
            .collect::<HashMap<_, _>>();

        let mut handles = Vec::new();
        for (request_index, resource_ref, id) in id_entries {
            // Report misses in the terms the caller selected by. A ref carrying
            // both is resolved by its id, so it reports as an id miss.
            let not_found = || match (&resource_ref.id, &resource_ref.name) {
                (None, Some(name)) => resource_type_name(schema)
                    .map(|type_name| {
                        ResourceLookupProblem::NameNotFound(ResourceNameNotFoundError {
                            type_name,
                            name: name.clone(),
                        })
                    })
                    .unwrap_or_else(|_| id_not_found(id)),
                _ => id_not_found(id),
            };

            // Only a ref that supplied its own id asserts a name to verify: for
            // a name-keyed ref the name *is* how the id was found.
            let expected_name = if resource_ref.id.is_some() {
                resource_ref.name.as_ref()
            } else {
                None
            };

            let row_result = rows_by_id.get(id.as_ref()).cloned().ok_or_else(not_found);
            match row_result.and_then(|row| {
                validate_handle_row(
                    row,
                    schema,
                    expected_name,
                    ensure_schema_matches::<ResourceLookupProblem>,
                )
            }) {
                Ok(row) => {
                    let handle = resource_handle_from_row(row);
                    handles.push(IndexedResource {
                        request_index,
                        item: handle,
                    });
                }
                Err(error) => problems.push(BatchResourceProblem {
                    request_index,
                    error,
                }),
            }
        }

        handles.sort_by_key(|handle| handle.request_index);
        problems.sort_by_key(|problem| problem.request_index);

        Ok((handles, problems))
    }

    async fn find_account_snapshot(
        &self,
        account_id: &odf::AccountID,
        id: ResourceID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let Some(snapshot) = self
            .generic_resource_query_service
            .get_snapshot_by_id(&id)
            .await?
        else {
            return Ok(None);
        };

        if snapshot.headers.account.did != *account_id {
            return Ok(None);
        }

        Ok(Some(snapshot))
    }

    async fn ensure_manifest_id_is_accessible(
        &self,
        schema: &TypeUri,
        account_id: &odf::AccountID,
        maybe_id: Option<ResourceID>,
    ) -> Result<(), ApplyManifestError> {
        let Some(id) = maybe_id else {
            return Ok(());
        };

        let Some(snapshot) = self.find_account_snapshot(account_id, id).await? else {
            return Err(ResourceIDNotFoundError(id).into());
        };

        if snapshot.schema != *schema {
            return Err(ResourceTypeMismatchError::new(id, schema.clone(), snapshot.schema).into());
        }

        Ok(())
    }

    fn apply_spec_view_mode_batch<E>(
        &self,
        resources: &mut [IndexedResource<Resource>],
        spec_view: SpecViewOpts,
    ) -> Result<(), E>
    where
        E: From<InternalError>,
    {
        // A batch can span several schemas, and the dispatcher is schema-specific
        // — a schema may register one or none. Resolving from the first item
        // alone would apply its answer to every other schema, either skipping a
        // transformation the item needs or handing a dispatcher a spec it cannot
        // parse.
        //
        // Cached per schema so a single-schema batch still costs one lookup.
        let mut dispatchers: HashMap<TypeUri, Option<Arc<dyn ResourceSpecViewDispatcher>>> =
            HashMap::new();

        for resource in resources.iter_mut() {
            let maybe_dispatcher = dispatchers
                .entry(resource.item.schema.clone())
                .or_insert_with(|| {
                    self.try_resolve_spec_view_dispatcher(&resource.item.schema, spec_view)
                });

            if let Some(d) = maybe_dispatcher {
                let spec = std::mem::replace(&mut resource.item.spec, serde_json::Value::Null);
                resource.item.spec = d.reveal_spec(spec).map_err(E::from)?;
            }
        }

        Ok(())
    }

    async fn prepare_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<PreparedApplyManifest, ApplyManifestError> {
        let manifest = parse_manifest(request.format, &request.manifest)?;

        let target_account = self
            .resource_account_resolver
            .resolve_target_account(manifest.headers.account.as_ref())
            .await?;

        let dispatcher = get_resource_crud_dispatcher::<ApplyManifestError>(
            &self.catalog,
            manifest.schema.as_str(),
        )?;

        let canonical_labels = self
            .resource_extension_schema_resolver
            .canonicalize_entries(
                ResourceExtensionKind::Label,
                manifest.headers.labels.clone(),
                &manifest.schema,
            )?;
        let canonical_annotations = self
            .resource_extension_schema_resolver
            .canonicalize_entries(
                ResourceExtensionKind::Annotation,
                manifest.headers.annotations.clone(),
                &manifest.schema,
            )?;

        let mut header_warnings = collect_manifest_header_warnings(&canonical_annotations.entries);
        header_warnings.extend(collect_non_indexable_label_warnings(
            &canonical_labels.entries,
        ));
        header_warnings.extend(canonical_labels.warnings);
        header_warnings.extend(canonical_annotations.warnings);

        let headers = make_headers_input(
            manifest.headers.name.as_str(),
            &target_account,
            canonical_labels.entries,
            canonical_annotations.entries,
        )?;

        self.ensure_manifest_id_is_accessible(
            manifest.schema.typ(),
            &target_account.did,
            manifest.headers.id,
        )
        .await?;

        Ok(PreparedApplyManifest {
            dispatcher,
            id: manifest.headers.id,
            header: headers,
            header_warnings,
            target_account,
            spec: manifest.spec,
        })
    }

    fn try_resolve_spec_view_dispatcher(
        &self,
        schema: &TypeUri,
        spec_view: SpecViewOpts,
    ) -> Option<Arc<dyn ResourceSpecViewDispatcher>> {
        if spec_view.revealed {
            get_resource_spec_view_dispatcher_from_catalog(&self.catalog, schema)
        } else {
            None
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PreparedApplyManifest {
    dispatcher: Arc<dyn ResourceCrudDispatcher>,
    id: Option<ResourceID>,
    header: ResourceHeadersInput,
    header_warnings: Vec<ResourceWarning>,
    target_account: odf::AccountHandle,
    spec: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct IndexedResource<T> {
    request_index: usize,
    item: T,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
