// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::LoadError;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_resources::{
    ApplyManifestApplicationDecision,
    ApplyManifestDocumentSource,
    ApplyManifestPlan,
    ApplyManifestPlanningDecision,
    ApplyManifestResult,
    ApplyResourceApplicationDecision,
    ApplyResourceCrudDispatcherError,
    ApplyResourceParams,
    ApplyResourcePlanningDecision,
    DeclarativeResource,
    DeclarativeResourceState,
    EventSourcedResource,
    Resource,
    ResourceAccountOperation,
    ResourceAggregateLoader,
    ResourceCrudDispatcherApplyRequest,
    ResourceID,
    ResourceIDNotFoundError,
    ResourceOperation,
    ResourceOperationError,
    ResourceOperationGuard,
    ResourceOperationRequest,
    ResourceSchemaProvider,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn decode_apply_resource_params<R>(
    request: ResourceCrudDispatcherApplyRequest,
) -> Result<ApplyResourceParams<R>, ApplyResourceCrudDispatcherError>
where
    R: EventSourcedResource + ResourceSchemaProvider,
    R::SpecInput: DeserializeOwned,
{
    let ResourceCrudDispatcherApplyRequest {
        id,
        headers,
        account_source,
        access_check,
        spec,
        provenance,
    } = request;

    let spec = serde_json::from_value(spec).map_err(|e| {
        ApplyResourceCrudDispatcherError::InvalidSpec {
            schema: R::type_uri().clone(),
            message: e.to_string(),
        }
    })?;

    Ok(ApplyResourceParams {
        id,
        headers,
        account_source,
        access_check,
        spec,
        provenance,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn map_apply_resource_planning_decision<R>(
    decision: ApplyResourcePlanningDecision<R>,
) -> Result<ApplyManifestPlanningDecision, InternalError>
where
    R: ResourceSchemaProvider + DeclarativeResource,
    R::Spec: Serialize,
{
    Ok(match decision {
        ApplyResourcePlanningDecision::Planned(plan) => {
            let kamu_resources::ApplyResourcePlan {
                state,
                action,
                reconciliation_required,
                executable,
                warnings,
                previous_state,
                name_pending,
                ..
            } = plan;

            let resource = typed_resource_state_to_resource::<R>(state)?;
            // Comes from the aggregate the planner already loaded — no extra read.
            let previous_resource = previous_state
                .map(typed_resource_state_to_resource::<R>)
                .transpose()?;

            ApplyManifestPlanningDecision::Planned(ApplyManifestPlan {
                resource,
                outcome: action.into(),
                reconciliation_required,
                executable,
                warnings,
                name_pending,
                // Deliberately not canonicalized here: the facade corrects
                // `headers.account` after this dispatcher returns, and the
                // account is part of the canonical manifest.
                documents: ApplyManifestDocumentSource::Pair {
                    previous: previous_resource,
                },
            })
        }
        ApplyResourcePlanningDecision::Rejected(rejection) => {
            ApplyManifestPlanningDecision::Rejected(rejection.into())
        }
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn map_apply_resource_application_decision<R>(
    decision: ApplyResourceApplicationDecision<R>,
) -> Result<ApplyManifestApplicationDecision, InternalError>
where
    R: ResourceSchemaProvider + DeclarativeResource,
    R::Spec: Serialize,
{
    Ok(match decision {
        ApplyResourceApplicationDecision::Applied(result) => {
            let kamu_resources::ApplyResourceResult {
                state,
                outcome,
                warnings,
                previous_state,
                ..
            } = result;
            let resource = typed_resource_state_to_resource::<R>(state)?;
            // Captured by the planner before the write — see `previous_state`.
            let previous_resource = previous_state
                .map(typed_resource_state_to_resource::<R>)
                .transpose()?;

            ApplyManifestApplicationDecision::Applied(ApplyManifestResult {
                resource,
                outcome,
                warnings,
                // See the planning path: canonicalized only after the facade's
                // `headers.account` fixup.
                documents: ApplyManifestDocumentSource::Pair {
                    previous: previous_resource,
                },
            })
        }
        ApplyResourceApplicationDecision::Rejected(rejection) => {
            ApplyManifestApplicationDecision::Rejected(rejection.into())
        }
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn typed_resource_state_to_resource<R>(
    state: R::ResourceState,
) -> Result<Resource, InternalError>
where
    R: ResourceSchemaProvider + DeclarativeResource,
    R::Spec: Serialize,
{
    let (_id, headers, spec, status) = state.into_parts();

    Ok(Resource {
        schema: R::type_uri().cheap_clone(),
        headers,
        spec: serde_json::to_value(spec).int_err()?,
        status,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One result per ID, in ID order. A resource deleted since its ID was
/// resolved is not found
pub async fn check_resource_operations<R>(
    resource_aggregate_loader: &dyn ResourceAggregateLoader<R>,
    operation_guard: &dyn ResourceOperationGuard<R>,
    principal: &odf::metadata::auth::PrincipalHandle,
    account: &odf::ResourceOwnerHandle,
    operation: ResourceAccountOperation,
    ids: &[ResourceID],
) -> Result<Vec<Result<(), ResourceOperationError>>, InternalError>
where
    R: EventSourcedResource,
{
    let mut loaded = Vec::with_capacity(ids.len());
    for (id, result) in ids
        .iter()
        .zip(resource_aggregate_loader.load_many(ids).await.int_err()?)
    {
        match result {
            Ok(resource) => loaded.push(Ok(resource)),
            Err(LoadError::NotFound(_)) => loaded.push(Err(ResourceOperationError::NotFound(
                ResourceIDNotFoundError(*id),
            ))),
            Err(err @ (LoadError::ProjectionError(_) | LoadError::Internal(_))) => {
                return Err(err.int_err());
            }
        }
    }

    let requests = loaded
        .iter()
        .filter_map(|result| result.as_ref().ok())
        .map(|resource| ResourceOperationRequest {
            account,
            operation: match operation {
                ResourceAccountOperation::Read => ResourceOperation::Read { resource },
                ResourceAccountOperation::Delete => ResourceOperation::Delete { resource },
            },
        })
        .collect::<Vec<_>>();

    let mut checked = operation_guard
        .check_operations(principal, &requests)
        .await?
        .into_iter();

    Ok(loaded
        .into_iter()
        .map(|result| match result {
            Ok(_) => checked.next().expect("one result per request"),
            Err(err) => Err(err),
        })
        .collect())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
