// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod secret_set;
mod storage;
mod variable_set;

pub use secret_set::*;
pub use storage::*;
pub use variable_set::*;

use crate::{ResourceDescriptor, ResourceDescriptorProvider};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const RESOURCE_DESCRIPTORS: &[ResourceDescriptor] = &[
    StorageResource::DESCRIPTOR,
    SecretSetResource::DESCRIPTOR,
    VariableSetResource::DESCRIPTOR,
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn find_resource_descriptor(resource_type: &str) -> Option<&'static ResourceDescriptor> {
    RESOURCE_DESCRIPTORS
        .iter()
        .find(|descriptor| descriptor.resource_type == resource_type)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
