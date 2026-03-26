// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::*;
use kamu_resources::{DeclarativeResource, ResourceApiVersion, ResourceType};

use crate::{SecretSetEventStore, SecretSetSpec, SecretSetState, SecretSetStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct SecretSetResource(pub(crate) Aggregate<SecretSetState, dyn SecretSetEventStore>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSetResource {
    pub const RESOURCE_TYPE: &'static str = "secret_set";
    pub const API_VERSION: &'static str = "v1alpha1";
}

impl ResourceType for SecretSetResource {
    const RESOURCE_TYPE: &'static str = Self::RESOURCE_TYPE;
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
