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

/// Decodes [`ResourceConditions`] from a raw JSON value, going through the YAML
/// shadow proxy since the ODF DTO has no direct `Deserialize` of its own.
pub fn resource_conditions_from_json(value: serde_json::Value) -> ResourceConditions {
    let proxy: odf::metadata::serde::yaml::resource::ResourceConditions =
        serde_json::from_value(value).unwrap();
    proxy.try_into().unwrap()
}

/// Encodes [`ResourceConditions`] into a raw JSON value, going through the YAML
/// shadow proxy since the ODF DTO has no direct `Serialize` of its own.
pub fn resource_conditions_to_json(conditions: &ResourceConditions) -> serde_json::Value {
    let proxy: odf::metadata::serde::yaml::resource::ResourceConditions = conditions.clone().into();
    serde_json::to_value(proxy).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
