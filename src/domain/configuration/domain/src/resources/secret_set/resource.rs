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

use crate::{SecretSetEventStore, SecretSetSpec, SecretSetState, SecretSetStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct SecretSetResource(pub(crate) Aggregate<SecretSetState, dyn SecretSetEventStore>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSetResource {
    pub const RESOURCE_TYPE: &'static str = "SecretSet";
    pub const RESOURCE_NAME: &'static str = "secretsets";
    pub const RESOURCE_SHORT_NAMES: &'static [&'static str] = &["ss"];
    pub const API_VERSION: &'static str = "kamu.dev/v1alpha1";
}

impl ResourceType for SecretSetResource {
    const RESOURCE_TYPE: &'static str = Self::RESOURCE_TYPE;
}

impl ResourceKindName for SecretSetResource {
    const RESOURCE_NAME: &'static str = Self::RESOURCE_NAME;
}

impl ResourceKindShortNames for SecretSetResource {
    const RESOURCE_SHORT_NAMES: &'static [&'static str] = Self::RESOURCE_SHORT_NAMES;
}

impl ResourceApiVersion for SecretSetResource {
    const API_VERSION: &'static str = Self::API_VERSION;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for SecretSetResource {
    type Spec = SecretSetSpec;
    type Status = SecretSetStatus;
    type ResourceState = SecretSetState;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
