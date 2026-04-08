// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceType {
    const RESOURCE_TYPE: &'static str;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceApiVersion {
    const API_VERSION: &'static str;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceDescriptor {
    pub resource_type: &'static str,
    pub api_version: &'static str,
}

impl ResourceDescriptor {
    pub const fn new(resource_type: &'static str, api_version: &'static str) -> Self {
        Self {
            resource_type,
            api_version,
        }
    }

    pub fn matches_snapshot(&self, snapshot: &crate::ResourceSnapshot) -> bool {
        self.resource_type == snapshot.kind && self.api_version == snapshot.api_version
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceDescriptorProvider: ResourceType + ResourceApiVersion {
    const DESCRIPTOR: ResourceDescriptor =
        ResourceDescriptor::new(Self::RESOURCE_TYPE, Self::API_VERSION);
}

impl<T> ResourceDescriptorProvider for T where T: ResourceType + ResourceApiVersion {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
