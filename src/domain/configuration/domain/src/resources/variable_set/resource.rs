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
    ResourceApiVersion,
    ResourceKindName,
    ResourceKindShortNames,
    ResourceType,
};

use crate::{VariableSetEventStore, VariableSetSpec, VariableSetState, VariableSetStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct VariableSetResource(pub(crate) Aggregate<VariableSetState, dyn VariableSetEventStore>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl VariableSetResource {
    pub const RESOURCE_TYPE: &'static str = "VariableSet";
    pub const RESOURCE_NAME: &'static str = "variablesets";
    pub const RESOURCE_SHORT_NAMES: &'static [&'static str] = &["vs"];
    pub const API_VERSION: &'static str = "kamu.dev/v1alpha1";
}

impl ResourceType for VariableSetResource {
    const RESOURCE_TYPE: &'static str = Self::RESOURCE_TYPE;
}

impl ResourceKindName for VariableSetResource {
    const RESOURCE_NAME: &'static str = Self::RESOURCE_NAME;
}

impl ResourceKindShortNames for VariableSetResource {
    const RESOURCE_SHORT_NAMES: &'static [&'static str] = Self::RESOURCE_SHORT_NAMES;
}

impl ResourceApiVersion for VariableSetResource {
    const API_VERSION: &'static str = Self::API_VERSION;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for VariableSetResource {
    type Spec = VariableSetSpec;
    type Status = VariableSetStatus;
    type ResourceState = VariableSetState;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
