// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::*;
use kamu_resources::{
    DeclarativeResource,
    DeclarativeResourceState,
    ResourceListColumnDataType,
    ResourceListColumnDefinition,
    ResourceListColumnValue,
    ResourceListColumnValueView,
    ResourceListColumnVisibility,
    ResourcePresentation,
    ResourcePresentationDefinition,
    ResourceSchemaProvider,
};

use crate::{SecretSetEventStore, SecretSetSpec, SecretSetState, SecretSetStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct SecretSetResource(pub(crate) Aggregate<SecretSetState, dyn SecretSetEventStore>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSetResource {
    pub const SCHEMA: &'static str = "https://opendatafabric.org/schemas/config/v1alpha1/SecretSet";
    pub const RESOURCE_NAME: &'static str = "secretsets";
    pub const RESOURCE_SHORT_NAMES: &'static [&'static str] = &["ss"];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSchemaProvider for SecretSetResource {
    const SCHEMA: &'static str = Self::SCHEMA;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for SecretSetResource {
    type Spec = SecretSetSpec;
    type Status = SecretSetStatus;
    type ResourceState = SecretSetState;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourcePresentation for SecretSetResource {
    const PRESENTATION: ResourcePresentationDefinition = ResourcePresentationDefinition::new(
        Self::RESOURCE_NAME,
        Self::RESOURCE_SHORT_NAMES,
        &[ResourceListColumnDefinition {
            key: "secrets",
            header: "Secrets",
            data_type: ResourceListColumnDataType::UInt64,
            visibility: ResourceListColumnVisibility::Default,
        }],
    );

    fn list_column_values(state: &Self::ResourceState) -> Vec<ResourceListColumnValueView> {
        vec![ResourceListColumnValueView {
            key: "secrets".to_string(),
            value: ResourceListColumnValue::UInt64(
                u64::try_from(state.status().stats.total_secrets).unwrap(),
            ),
        }]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
