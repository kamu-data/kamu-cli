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

use crate::{StorageEventStore, StorageSpec, StorageState, StorageStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct StorageResource(pub(crate) Aggregate<StorageState, dyn StorageEventStore>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl StorageResource {
    pub const RESOURCE_TYPE: &'static str = "storage";
    pub const API_VERSION: &'static str = "v1alpha1";
}

impl ResourceType for StorageResource {
    const RESOURCE_TYPE: &'static str = Self::RESOURCE_TYPE;
}

impl ResourceApiVersion for StorageResource {
    const API_VERSION: &'static str = Self::API_VERSION;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for StorageResource {
    type Spec = StorageSpec;
    type Status = StorageStatus;
    type ResourceState = StorageState;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
