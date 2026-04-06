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

pub trait ResourceKindName {
    const RESOURCE_NAME: &'static str;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceKindShortNames {
    const RESOURCE_SHORT_NAMES: &'static [&'static str];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceApiVersion {
    const API_VERSION: &'static str;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceDescriptor {
    pub resource_type: &'static str,
    pub resource_name: &'static str,
    pub resource_short_names: &'static [&'static str],
    pub api_version: &'static str,
}

impl ResourceDescriptor {
    pub const fn new(
        resource_type: &'static str,
        resource_name: &'static str,
        resource_short_names: &'static [&'static str],
        api_version: &'static str,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            resource_short_names,
            api_version,
        }
    }

    pub fn matches_snapshot(&self, snapshot: &crate::ResourceSnapshot) -> bool {
        self.resource_type == snapshot.kind && self.api_version == snapshot.api_version
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceDescriptorProvider:
    ResourceType + ResourceKindName + ResourceKindShortNames + ResourceApiVersion
{
    const DESCRIPTOR: ResourceDescriptor = ResourceDescriptor::new(
        Self::RESOURCE_TYPE,
        Self::RESOURCE_NAME,
        Self::RESOURCE_SHORT_NAMES,
        Self::API_VERSION,
    );
}

impl<T> ResourceDescriptorProvider for T where
    T: ResourceType + ResourceKindName + ResourceKindShortNames + ResourceApiVersion
{
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
