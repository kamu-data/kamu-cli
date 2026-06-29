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
    ResourceConditionStatus,
    ResourceConditionType,
    ResourceDescriptorProvider,
    ResourcePresentation,
    ResourceSnapshot,
    ResourceStatusLike,
    ResourceStatusSummaryView,
    ResourceSummaryView,
    ResourceView,
    ResourceViewHeaders,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{load_previous_resource_view, make_apply_manifest_changes};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn decode_resource_spec<R>(
    schema: &str,
    spec: serde_json::Value,
) -> Result<R::Spec, ApplyResourceCrudDispatcherError>
where
    R: ReconcilableEventSourcedResource,
    R::Spec: DeserializeOwned,
{
    serde_json::from_value(spec).map_err(|e| ApplyResourceCrudDispatcherError::InvalidSpec {
        schema: schema.to_string(),
        message: e.to_string(),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn map_apply_resource_planning_decision<R>(
    decision: ApplyResourcePlanningDecision<R>,
    generic_resource_query_service: &dyn GenericResourceQueryService,
) -> Result<ApplyManifestPlanningDecision, InternalError>
where
    R: ResourceDescriptorProvider + DeclarativeResource,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
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

            let resource = typed_resource_state_to_view::<R>(state)?;
            let previous_resource =
                load_previous_resource_view(action, id, generic_resource_query_service).await?;
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
    R: ResourceDescriptorProvider + DeclarativeResource,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    Ok(match decision {
        ApplyResourceApplicationDecision::Applied(result) => {
            let kamu_resources::ApplyResourceResult {
                state,
                outcome,
                warnings,
                ..
            } = result;
            let resource = typed_resource_state_to_view::<R>(state)?;

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

pub fn typed_resource_state_to_view<R>(
    state: R::ResourceState,
) -> Result<ResourceView, InternalError>
where
    R: ResourceDescriptorProvider + DeclarativeResource,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    let (id, headers, spec, status) = state.into_parts();

    Ok(ResourceView {
        schema: R::DESCRIPTOR.schema.to_string(),
        headers: ResourceViewHeaders::from_owned(id, headers),
        last_reconciled_at: status.resource_status().last_reconciled_at(),
        spec: serde_json::to_value(spec).int_err()?,
        status: Some(serde_json::to_value(status).int_err()?),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn typed_resource_state_to_summary_view<R>(state: &R::ResourceState) -> ResourceSummaryView
where
    R: ResourceDescriptorProvider + DeclarativeResource + ResourcePresentation,
    R::Status: ResourceStatusLike,
{
    ResourceSummaryView {
        schema: R::DESCRIPTOR.schema.to_string(),
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

pub(crate) fn resource_snapshot_to_view(snapshot: ResourceSnapshot) -> ResourceView {
    let ResourceSnapshot {
        id,
        schema,
        headers,
        spec,
        status,
        last_reconciled_at,
        ..
    } = snapshot;

    ResourceView {
        schema,
        headers: ResourceViewHeaders::from_owned(id, headers),
        last_reconciled_at,
        spec,
        status,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn resource_status_summary_view(status: &impl ResourceStatusLike) -> ResourceStatusSummaryView {
    let resource_status = status.resource_status();
    let ready = resource_status
        .conditions
        .iter()
        .find(|condition| condition.type_ == ResourceConditionType::Ready)
        .map(|condition| match condition.status {
            ResourceConditionStatus::True => true,
            ResourceConditionStatus::False | ResourceConditionStatus::Unknown => false,
        });

    ResourceStatusSummaryView {
        phase: Some(resource_status.phase),
        observed_generation: Some(resource_status.observed_generation),
        ready,
    }
}
