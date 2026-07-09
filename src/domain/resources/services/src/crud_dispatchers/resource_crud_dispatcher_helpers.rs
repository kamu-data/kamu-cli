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
    ApplyManifestPlan,
    ApplyManifestPlanningDecision,
    ApplyManifestResult,
    ApplyResourceApplicationDecision,
    ApplyResourceCrudDispatcherError,
    ApplyResourcePlanningDecision,
    DeclarativeResource,
    DeclarativeResourceState,
    GenericResourceQueryService,
    ReconcilableEventSourcedResource,
    Resource,
    ResourceConditionStatus,
    ResourcePresentation,
    ResourceSchemaProvider,
    ResourceSnapshot,
    ResourceStatus,
    ResourceStatusExt,
    ResourceStatusSummaryView,
    ResourceSummaryView,
    TypeUri,
    new_pending_resource_status,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{load_previous_resource, make_apply_manifest_changes};

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

pub async fn map_apply_resource_planning_decision<R>(
    decision: ApplyResourcePlanningDecision<R>,
    generic_resource_query_service: &dyn GenericResourceQueryService,
) -> Result<ApplyManifestPlanningDecision, InternalError>
where
    R: ResourceSchemaProvider + DeclarativeResource,
    R::Spec: Serialize,
{
    Ok(match decision {
        ApplyResourcePlanningDecision::Planned(plan) => {
            let kamu_resources::ApplyResourcePlan {
                id,
                state,
                action,
                reconciliation_required,
                executable,
                warnings,
            } = plan;

            let resource = typed_resource_state_to_resource::<R>(state)?;
            let previous_resource =
                load_previous_resource(action, id, generic_resource_query_service).await?;
            let changes = make_apply_manifest_changes(previous_resource.as_ref(), &resource)?;

            ApplyManifestPlanningDecision::Planned(ApplyManifestPlan {
                resource,
                outcome: action.into(),
                reconciliation_required,
                executable,
                changes,
                warnings,
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
                ..
            } = result;
            let resource = typed_resource_state_to_resource::<R>(state)?;

            ApplyManifestApplicationDecision::Applied(ApplyManifestResult {
                resource,
                outcome,
                warnings,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn typed_resource_state_to_summary_view<R>(state: &R::ResourceState) -> ResourceSummaryView
where
    R: ResourceSchemaProvider + DeclarativeResource + ResourcePresentation,
{
    ResourceSummaryView {
        schema: R::schema().clone(),
        id: *state.id(),
        name: state.headers().name.clone(),
        description: state.headers().description.clone(),
        generation: state.headers().generation,
        created_at: state.headers().created_at,
        updated_at: state.headers().updated_at,
        status: Some(resource_status_summary_view(state.status())),
        list_values: R::list_column_values(state),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resource_snapshot_to_resource(snapshot: ResourceSnapshot) -> Resource {
    let ResourceSnapshot {
        schema,
        headers,
        spec,
        status,
        ..
    } = snapshot;

    Resource {
        schema,
        headers,
        spec,
        status: status.unwrap_or_else(new_pending_resource_status),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn resource_status_summary_view(status: &ResourceStatus) -> ResourceStatusSummaryView {
    let ready = status.ready_condition_status().map(|status| match status {
        ResourceConditionStatus::True => true,
        ResourceConditionStatus::False | ResourceConditionStatus::Unknown => false,
    });

    ResourceStatusSummaryView {
        phase: Some(status.phase),
        observed_generation: status.observed_generation,
        ready,
    }
}
