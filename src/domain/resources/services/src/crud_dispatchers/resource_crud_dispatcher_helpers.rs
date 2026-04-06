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
    ResourceLinterSpec,
    ResourceSnapshot,
    ResourceStatusLike,
    ResourceStatusSummaryView,
    ResourceSummaryView,
    ResourceValidateSpec,
    ResourceView,
    ResourceViewAccount,
    ResourceViewMetadata,
    ResourceWarning,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn decode_resource_spec<R>(
    kind: &str,
    api_version: &str,
    spec: serde_json::Value,
) -> Result<DecodedResourceSpec<R::Spec>, ApplyResourceCrudDispatcherError>
where
    R: ReconcilableEventSourcedResource,
    R::Spec: DeserializeOwned + ResourceValidateSpec + ResourceLinterSpec,
    <R::Spec as ResourceValidateSpec>::ValidationError: std::fmt::Display,
{
    let spec: R::Spec = serde_json::from_value(spec).map_err(|e| {
        ApplyResourceCrudDispatcherError::InvalidSpec {
            kind: kind.to_string(),
            api_version: api_version.to_string(),
            message: e.to_string(),
        }
    })?;

    spec.validate()
        .map_err(|e| ApplyResourceCrudDispatcherError::InvalidSpec {
            kind: kind.to_string(),
            api_version: api_version.to_string(),
            message: e.to_string(),
        })?;

    let warnings = spec.lint_warnings();

    Ok(DecodedResourceSpec { spec, warnings })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DecodedResourceSpec<T> {
    pub spec: T,
    pub warnings: Vec<ResourceWarning>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn map_apply_resource_planning_decision<R>(
    decision: ApplyResourcePlanningDecision<R>,
    generic_resource_query_service: &dyn GenericResourceQueryService,
    warnings: Vec<ResourceWarning>,
) -> Result<ApplyManifestPlanningDecision, InternalError>
where
    R: ResourceDescriptorProvider + DeclarativeResource,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    Ok(match decision {
        ApplyResourcePlanningDecision::Planned(plan) => {
            let kamu_resources::ApplyResourcePlan {
                uid,
                state,
                action,
                reconciliation_required,
                executable,
            } = plan;

            let resource = typed_resource_state_to_view::<R>(state)?;
            let previous_resource =
                super::resource_crud_dispatcher_diff_helpers::load_previous_resource_view(
                    action,
                    uid,
                    generic_resource_query_service,
                )
                .await?;
            let changes =
                super::resource_crud_dispatcher_diff_helpers::make_apply_manifest_changes(
                    previous_resource.as_ref(),
                    &resource,
                )?;

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
    warnings: Vec<ResourceWarning>,
) -> Result<ApplyManifestApplicationDecision, InternalError>
where
    R: ResourceDescriptorProvider + DeclarativeResource,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    Ok(match decision {
        ApplyResourceApplicationDecision::Applied(result) => {
            let kamu_resources::ApplyResourceResult { state, outcome, .. } = result;
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
    let (uid, metadata, spec, status) = state.into_parts();

    Ok(ResourceView {
        kind: R::DESCRIPTOR.resource_type.to_string(),
        api_version: R::DESCRIPTOR.api_version.to_string(),
        account: ResourceViewAccount {
            id: metadata.account.clone(),
            name: None,
        },
        metadata: ResourceViewMetadata::from_owned(uid, metadata),
        last_reconciled_at: status.resource_status().last_reconciled_at(),
        spec: serde_json::to_value(spec).int_err()?,
        status: Some(serde_json::to_value(status).int_err()?),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn typed_resource_state_to_summary_view<R>(state: &R::ResourceState) -> ResourceSummaryView
where
    R: ResourceDescriptorProvider + DeclarativeResource,
    R::Status: ResourceStatusLike,
{
    ResourceSummaryView {
        kind: R::DESCRIPTOR.resource_type.to_string(),
        api_version: R::DESCRIPTOR.api_version.to_string(),
        uid: *state.uid(),
        name: state.metadata().name.clone(),
        description: state.metadata().description.clone(),
        generation: state.metadata().generation,
        created_at: state.metadata().created_at,
        updated_at: state.metadata().updated_at,
        status: Some(resource_status_summary_view(state.status())),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resource_snapshot_to_view(snapshot: ResourceSnapshot) -> ResourceView {
    let ResourceSnapshot {
        uid,
        kind,
        api_version,
        metadata,
        spec,
        status,
        last_reconciled_at,
        ..
    } = snapshot;

    ResourceView {
        kind,
        api_version,
        account: ResourceViewAccount {
            id: metadata.account.clone(),
            name: None,
        },
        metadata: ResourceViewMetadata::from_owned(uid, metadata),
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
        phase: Some(format!("{:?}", resource_status.phase)),
        observed_generation: Some(resource_status.observed_generation),
        ready,
    }
}
