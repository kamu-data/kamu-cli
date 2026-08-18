// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::{ResourceApplyOutcome, ResourceApplyParseManifestProblem};
use crate::prelude::*;
use crate::queries::{
    BatchResourceProblem,
    ResourceAccountResolutionProblem,
    ResourceManifestFormat,
    ResourceRefInput,
    ResourceUnsupportedSelectorProblem,
    into_resource_refs,
    map_account_access_error,
    map_unsupported_descriptor_problem,
    map_unsupported_selector_problem,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Clone)]
pub struct ApplyManifestInput {
    pub manifest: String,
    pub format: ResourceManifestFormat,
}

impl From<ApplyManifestInput> for kamu_resources_facade::ApplyManifestRequest {
    fn from(value: ApplyManifestInput) -> Self {
        Self {
            format: value.format.into(),
            manifest: value.manifest,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourcesMut;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl ResourcesMut {
    #[tracing::instrument(level = "info", name = ResourcesMut_apply_manifest, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn apply_manifest(
        &self,
        ctx: &Context<'_>,
        manifest: String,
        format: ResourceManifestFormat,
        dry_run: Option<bool>,
    ) -> Result<ResourceApplyOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let request = kamu_resources_facade::ApplyManifestRequest {
            format: format.into(),
            manifest,
        };

        let outcome_result = if dry_run.unwrap_or(false) {
            resource_facade
                .plan_apply_manifest(request)
                .await
                .map(ResourceApplyOutcome::from)
        } else {
            resource_facade
                .apply_manifest(request)
                .await
                .map(ResourceApplyOutcome::from)
        };

        match outcome_result {
            Ok(outcome) => Ok(outcome),
            Err(err) => map_apply_resource_error(err),
        }
    }

    #[tracing::instrument(level = "info", name = ResourcesMut_apply_manifests, skip_all, fields(item_count = manifests.len()))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn apply_manifests(
        &self,
        ctx: &Context<'_>,
        manifests: Vec<ApplyManifestInput>,
        dry_run: Option<bool>,
    ) -> Result<ResourceApplyManifestsResult> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let request = kamu_resources_facade::ApplyManifestBatchRequest {
            items: manifests.into_iter().map(Into::into).collect(),
        };

        if dry_run.unwrap_or(false) {
            let batch_response = resource_facade
                .plan_apply_manifests(request)
                .await
                .map_err(map_batch_apply_resource_error)?;
            let summary =
                build_apply_manifest_summary(&batch_response.items, |decision| match decision {
                    kamu_resources::ApplyManifestPlanningDecision::Planned(_) => None,
                    kamu_resources::ApplyManifestPlanningDecision::Rejected(rejection) => {
                        Some(rejection)
                    }
                });
            if !summary.fully_succeeded() {
                return Err(rollback_error(&summary));
            }
            // Only ever reached once the batch is already known to have
            // fully succeeded — never short-circuits into the rollback path
            // above.
            build_apply_manifests_data(batch_response.items)
        } else {
            let batch_response = resource_facade
                .apply_manifests(request)
                .await
                .map_err(map_batch_apply_resource_error)?;
            let summary =
                build_apply_manifest_summary(&batch_response.items, |decision| match decision {
                    kamu_resources::ApplyManifestApplicationDecision::Applied(_) => None,
                    kamu_resources::ApplyManifestApplicationDecision::Rejected(rejection) => {
                        Some(rejection)
                    }
                });
            if !summary.fully_succeeded() {
                return Err(rollback_error(&summary));
            }
            build_apply_manifests_data(batch_response.items)
        }
    }

    #[tracing::instrument(level = "info", name = ResourcesMut_delete, skip_all, fields(selector_count = resource_refs.len()))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn delete(
        &self,
        ctx: &Context<'_>,
        resource_refs: Vec<ResourceRefInput>,
    ) -> Result<ResourceDeleteOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        match resource_facade
            .delete(into_resource_refs(resource_refs)?)
            .await
        {
            Ok(response) => Ok(ResourceDeleteOutcome::Success(response.into())),
            Err(kamu_resources_facade::BatchResourceError::UnsupportedSelector(e)) => Ok(
                ResourceDeleteOutcome::UnsupportedSelector(map_unsupported_selector_problem(e)),
            ),
            Err(kamu_resources_facade::BatchResourceError::AccountResolution(e)) => {
                Ok(ResourceDeleteOutcome::AccountResolution(e.into()))
            }
            Err(e) => Err(map_batch_delete_resource_error(e)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApplyManifestsResult {
    pub items: Vec<ResourceApplyManifestItemResult>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApplyManifestItemResult {
    pub request_index: usize,
    pub outcome: ResourceApplyOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Attached to the rollback path's `extensions` payload, sufficient to
/// reconstruct `ApplyManifestBatchResponse<D>` positionally on the remote
/// facade — never the full `Resource`, whose GraphQL/domain representations
/// have no `serde::Serialize` (that object graph is largely auto-generated
/// from ODF schemas). Rollback is batch transaction metadata, not a per-item
/// error: an item that was individually accepted before the batch rolled
/// back is not listed in `items` at all; its index appears in
/// `rolled_back_successes` instead.
/// Built directly from the facade's
/// `Result<D, ApplyManifestError>` per item — deliberately never routed
/// through `map_apply_resource_error`, which raises an immediate
/// transport-level `GqlError` for several variants (`IDNotFound`,
/// `TypeMismatch`, `ConcurrentModification`, `RemoteRequest`, `Internal`) and
/// would short-circuit before this summary could be built at all, defeating
/// the entire point of a summary that exists to survive those errors.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct ApplyManifestBatchSummary {
    pub items: Vec<ApplyManifestItemSummary>,
    pub rolled_back_successes: Vec<usize>,
}

impl ApplyManifestBatchSummary {
    fn fully_succeeded(&self) -> bool {
        self.items.is_empty()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ApplyManifestItemSummary {
    pub request_index: usize,
    pub outcome: ApplyManifestItemOutcomeSummary,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind")]
pub(crate) enum ApplyManifestItemOutcomeSummary {
    Rejected {
        category: kamu_resources::ApplyResourceRejectionCategory,
        message: String,
    },
    ParseManifest {
        message: String,
    },
    UnsupportedDescriptor {
        schema: kamu_resources::TypeUri,
    },
    AccountResolution {
        code: kamu_resources_facade::ResourceAccountResolutionProblemCode,
        message: String,
    },
    InvalidHeaders {
        code: kamu_resources_facade::ResourceHeadersValidationProblemCode,
        message: String,
    },
    InvalidSpec {
        schema: kamu_resources::TypeUri,
        message: String,
    },
    IDNotFound {
        id: kamu_resources::ResourceID,
    },
    TypeMismatch {
        id: kamu_resources::ResourceID,
        expected_schema: kamu_resources::TypeUri,
        actual_schema: kamu_resources::TypeUri,
    },
    ConcurrentModification,
    Failed {
        message: String,
    },
}

/// Summarizes an
/// [`ApplyManifestError`](kamu_resources_facade::ApplyManifestError) for the
/// error side of a batch item, independent of `map_apply_resource_error` —
/// see [`ApplyManifestBatchSummary`] for why.
fn summarize_apply_error(
    error: &kamu_resources_facade::ApplyManifestError,
) -> ApplyManifestItemOutcomeSummary {
    use kamu_resources_facade::ApplyManifestError as E;

    match error {
        E::ParseManifest(error) => ApplyManifestItemOutcomeSummary::ParseManifest {
            message: error.message.clone(),
        },
        E::UnsupportedDescriptor(error) => match error {
            kamu_resources::UnsupportedResourceDescriptorError::NotFound { schema } => {
                ApplyManifestItemOutcomeSummary::UnsupportedDescriptor {
                    schema: schema.clone(),
                }
            }
        },
        E::AccountResolution(error) => ApplyManifestItemOutcomeSummary::AccountResolution {
            code: error.code,
            message: error.message.clone(),
        },
        E::InvalidHeaders(error) => ApplyManifestItemOutcomeSummary::InvalidHeaders {
            code: error.code,
            message: error.message.clone(),
        },
        E::InvalidSpec(error) => ApplyManifestItemOutcomeSummary::InvalidSpec {
            schema: error.schema.clone(),
            message: error.message.clone(),
        },
        E::IDNotFound(error) => ApplyManifestItemOutcomeSummary::IDNotFound { id: error.0 },
        E::TypeMismatch(error) => ApplyManifestItemOutcomeSummary::TypeMismatch {
            id: error.id,
            expected_schema: error.expected_schema.clone(),
            actual_schema: error.actual_schema.clone(),
        },
        E::ConcurrentModification(_) => ApplyManifestItemOutcomeSummary::ConcurrentModification,
        // Account-access denials join the opaque failures here: the rollback
        // summary carries codes only for the user-facing problems.
        E::AccountAccess(_) | E::RemoteRequest(_) | E::Internal(_) => {
            ApplyManifestItemOutcomeSummary::Failed {
                message: error.to_string(),
            }
        }
    }
}

/// Builds the lightweight, always-computable batch summary directly from the
/// facade's `Result<D, ApplyManifestError>` per item, given a way to detect
/// whether a successful decision was in fact a business rejection. Accepted
/// items land in `rolled_back_successes` unconditionally at this point — it's
/// this summary's caller's job to only *report* it once the batch as a whole
/// is known not to have fully succeeded, since `rolled_back_successes` is
/// only meaningful in that case (see `apply_manifests`).
fn build_apply_manifest_summary<D>(
    items: &[kamu_resources_facade::ApplyManifestBatchItemResult<D>],
    decision_rejection: impl Fn(&D) -> Option<&kamu_resources::ApplyManifestRejection>,
) -> ApplyManifestBatchSummary {
    let mut summary = ApplyManifestBatchSummary::default();

    for item in items {
        match &item.outcome {
            Ok(decision) => match decision_rejection(decision) {
                None => summary.rolled_back_successes.push(item.request_index),
                Some(rejection) => summary.items.push(ApplyManifestItemSummary {
                    request_index: item.request_index,
                    outcome: ApplyManifestItemOutcomeSummary::Rejected {
                        category: rejection.category,
                        message: rejection.message.clone(),
                    },
                }),
            },
            Err(err) => summary.items.push(ApplyManifestItemSummary {
                request_index: item.request_index,
                outcome: summarize_apply_error(err),
            }),
        }
    }

    summary
}

/// Forces a transport-level GraphQL error so the whole request's database
/// transaction rolls back. The transaction itself is opened by the
/// `#[transactional_handler]`-annotated `graphql_handler` in
/// `app/cli/src/explore/graphql_handler.rs`, which wraps the *entire*
/// `schema.execute()` call and rolls back on any error in the response — not
/// something local to this resolver. Because that transaction is
/// request-scoped rather than field-scoped, a client that bundles
/// `applyManifests` with an unrelated top-level mutation in the same GraphQL
/// request would roll back that mutation too if this batch fails.
///
/// Rollback is batch-transaction metadata, not a per-item error, so it is
/// carried entirely via `extensions` (as `ApplyManifestBatchSummary`) rather
/// than folded into any per-item outcome. The rich per-item `Resource`
/// GraphQL/domain types have no `serde::Serialize` (much of that graph is
/// auto-generated), so the rollback payload intentionally carries only a
/// lightweight summary per item, rather than the full resource.
fn rollback_error(summary: &ApplyManifestBatchSummary) -> GqlError {
    let summary_json = serde_json::to_value(summary).unwrap_or_default();
    let summary_value =
        async_graphql::Value::from_json(summary_json).unwrap_or(async_graphql::Value::Null);

    GqlError::gql_extended("Batch apply did not fully succeed", |eev| {
        eev.set("batch", summary_value);
    })
}

/// Builds the full, `data`-path result — only ever called once the batch is
/// already known to have fully succeeded (every item `Ok` with no
/// rejection), so `map_apply_resource_error`'s `?` can never actually fire
/// here; it's only present because the compiler can't see that invariant.
fn build_apply_manifests_data<D>(
    items: Vec<kamu_resources_facade::ApplyManifestBatchItemResult<D>>,
) -> Result<ResourceApplyManifestsResult, GqlError>
where
    ResourceApplyOutcome: From<D>,
{
    let items = items
        .into_iter()
        .map(|item| {
            let outcome = match item.outcome {
                Ok(decision) => ResourceApplyOutcome::from(decision),
                Err(err) => map_apply_resource_error(err)?,
            };
            Ok(ResourceApplyManifestItemResult {
                request_index: item.request_index,
                outcome,
            })
        })
        .collect::<Result<Vec<_>, GqlError>>()?;

    Ok(ResourceApplyManifestsResult { items })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourceDeleteOutcome {
    Success(ResourceDeleteResult),
    UnsupportedSelector(ResourceUnsupportedSelectorProblem),
    AccountResolution(ResourceAccountResolutionProblem),
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceDeleteResult {
    pub resources: Vec<ResourceDeleteSuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

type BatchDeleteResourcesResponse = kamu_resources_facade::BatchResourceResponse<
    kamu_resources::ResourceID,
    kamu_resources_facade::ResourceLookupProblem,
>;

impl From<BatchDeleteResourcesResponse> for ResourceDeleteResult {
    fn from(value: BatchDeleteResourcesResponse) -> Self {
        Self {
            resources: value
                .successes
                .into_iter()
                .map(|success| ResourceDeleteSuccess {
                    request_index: success.request_index,
                    resource_id: success.item.into(),
                })
                .collect(),
            problems: value.problems.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceDeleteSuccess {
    pub request_index: usize,
    pub resource_id: ResourceID<'static>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_apply_resource_error(
    error: kamu_resources_facade::ApplyManifestError,
) -> Result<ResourceApplyOutcome, GqlError> {
    use kamu_resources_facade::ApplyManifestError as E;

    match error {
        E::ParseManifest(e) => Ok(ResourceApplyOutcome::ParseManifest(
            ResourceApplyParseManifestProblem {
                message: e.to_string(),
            },
        )),
        E::UnsupportedDescriptor(e) => Ok(ResourceApplyOutcome::UnsupportedDescriptor(
            map_unsupported_descriptor_problem(e),
        )),
        E::AccountResolution(e) => Ok(ResourceApplyOutcome::AccountResolution(e.into())),
        E::AccountAccess(e) => Err(map_account_access_error(e)),
        E::InvalidHeaders(e) => Ok(ResourceApplyOutcome::InvalidHeader(e.into())),
        E::InvalidSpec(e) => Ok(ResourceApplyOutcome::InvalidSpec(e.into())),
        E::IDNotFound(error) => Err(GqlError::gql(error.to_string())),
        E::TypeMismatch(error) => Err(GqlError::gql(error.to_string())),
        E::ConcurrentModification(error) => {
            tracing::error!(error = ?error, "Resource apply_manifest concurrent modification");
            Err(GqlError::gql("Resource was modified concurrently"))
        }
        E::RemoteRequest(error) => Err(error.int_err().into()),
        E::Internal(error) => Err(error.into()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_batch_delete_resource_error(error: kamu_resources_facade::BatchResourceError) -> GqlError {
    use kamu_resources_facade::BatchResourceError as E;

    match error {
        E::UnsupportedSelector(_) => GqlError::gql("Unsupported resource type selector"),
        E::AccountResolution(error) => GqlError::gql(error.to_string()),
        E::AccountAccess(error) => map_account_access_error(error),
        E::InvalidLabelFilter(error) => GqlError::gql(error.to_string()),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_batch_apply_resource_error(error: kamu_resources_facade::BatchResourceError) -> GqlError {
    use kamu_resources_facade::BatchResourceError as E;

    match error {
        E::UnsupportedSelector(_) => GqlError::gql("Unsupported resource type selector"),
        E::AccountResolution(error) => GqlError::gql(error.to_string()),
        E::AccountAccess(error) => map_account_access_error(error),
        E::InvalidLabelFilter(error) => GqlError::gql(error.to_string()),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
