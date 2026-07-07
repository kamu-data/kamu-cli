// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Map of condition schema [`crate::TypeRef`] keys to JSON condition payloads
/// added by controllers to describe the observed state of a resource.
pub type ResourceConditions = odf::metadata::resource::ResourceConditions;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn empty_resource_conditions() -> ResourceConditions {
    ResourceConditions {
        entries: std::collections::BTreeMap::new(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
