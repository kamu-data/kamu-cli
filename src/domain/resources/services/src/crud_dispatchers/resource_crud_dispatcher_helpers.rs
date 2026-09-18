// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{
    ApplyManifestApplicationDecision,
    ApplyManifestDocumentSource,
    ApplyManifestPlan,
    ApplyManifestPlanningDecision,
    ApplyManifestResult,
    ApplyResourceApplicationDecision,
    ApplyResourceCrudDispatcherError,
    ApplyResourcePlanningDecision,
    DeclarativeResource,
    DeclarativeResourceState,
    ReconcilableEventSourcedResource,
    Resource,
    ResourceSchemaProvider,
    TypeUri,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn decode_resource_spec<R>(
    schema: &TypeUri,
    spec: serde_json::Value,
) -> Result<R::SpecInput, ApplyResourceCrudDispatcherError>
where
    R: ReconcilableEventSourcedResource,
    R::SpecInput: DeserializeOwned,
{
    serde_json::from_value(spec).map_err(|e| ApplyResourceCrudDispatcherError::InvalidSpec {
        schema: schema.clone(),
        message: e.to_string(),
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
        schema: R::schema().clone(),
        headers,
        spec: serde_json::to_value(spec).int_err()?,
        status,
    })
}
