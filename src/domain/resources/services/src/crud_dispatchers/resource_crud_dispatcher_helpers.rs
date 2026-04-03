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
    ApplyResourceCrudDispatcherError,
    DeclarativeResource,
    DeclarativeResourceState,
    ReconcilableEventSourcedResource,
    ResourceConditionStatus,
    ResourceConditionType,
    ResourceDescriptorProvider,
    ResourceStatusLike,
    ResourceStatusSummaryView,
    ResourceSummaryView,
    ResourceValidateSpec,
    ResourceView,
    ResourceViewAccount,
    ResourceViewMetadata,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn decode_resource_spec<R>(
    kind: &str,
    api_version: &str,
    spec: serde_json::Value,
) -> Result<R::Spec, ApplyResourceCrudDispatcherError>
where
    R: ReconcilableEventSourcedResource,
    R::Spec: DeserializeOwned + ResourceValidateSpec,
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

    Ok(spec)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn typed_resource_state_to_view<R>(
    state: &R::ResourceState,
) -> Result<ResourceView, InternalError>
where
    R: ResourceDescriptorProvider + DeclarativeResource,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    Ok(ResourceView {
        kind: R::DESCRIPTOR.resource_type.to_string(),
        api_version: R::DESCRIPTOR.api_version.to_string(),
        account: ResourceViewAccount {
            id: state.metadata().account.clone(),
            name: None,
        },
        metadata: ResourceViewMetadata {
            uid: *state.uid(),
            name: state.metadata().name.clone(),
            description: state.metadata().description.clone(),
            labels: state.metadata().labels.clone(),
            annotations: state.metadata().annotations.clone(),
            generation: state.metadata().generation,
            created_at: state.metadata().created_at,
            updated_at: state.metadata().updated_at,
            deleted_at: state.metadata().deleted_at,
        },
        last_reconciled_at: state.status().resource_status().last_reconciled_at(),
        spec: serde_json::to_value(state.spec()).int_err()?,
        status: Some(serde_json::to_value(state.status()).int_err()?),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
