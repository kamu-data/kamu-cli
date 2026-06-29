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

use crate::{VariableSetEventStore, VariableSetSpec, VariableSetState, VariableSetStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct VariableSetResource(pub(crate) Aggregate<VariableSetState, dyn VariableSetEventStore>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl VariableSetResource {
    pub const SCHEMA: &'static str =
        "https://opendatafabric.org/schemas/config/v1alpha1/VariableSet";
    pub const RESOURCE_NAME: &'static str = "variablesets";
    pub const RESOURCE_SHORT_NAMES: &'static [&'static str] = &["vs"];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSchemaProvider for VariableSetResource {
    const SCHEMA: &'static str = Self::SCHEMA;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for VariableSetResource {
    type Spec = VariableSetSpec;
    type Status = VariableSetStatus;
    type ResourceState = VariableSetState;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourcePresentation for VariableSetResource {
    const PRESENTATION: ResourcePresentationDefinition = ResourcePresentationDefinition::new(
        Self::RESOURCE_NAME,
        Self::RESOURCE_SHORT_NAMES,
        &[ResourceListColumnDefinition {
            key: "variables",
            header: "Variables",
            data_type: ResourceListColumnDataType::UInt64,
            visibility: ResourceListColumnVisibility::Default,
        }],
    );

    fn list_column_values(state: &Self::ResourceState) -> Vec<ResourceListColumnValueView> {
        vec![ResourceListColumnValueView {
            key: "variables".to_string(),
            value: ResourceListColumnValue::UInt64(
                u64::try_from(state.status().stats.total_variables).unwrap(),
            ),
        }]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
