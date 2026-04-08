// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

use crate::{DeclarativeResource, ResourceDescriptorProvider};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourcePresentation: ResourceDescriptorProvider + DeclarativeResource {
    const PRESENTATION: ResourcePresentationDefinition;

    fn list_column_values(state: &Self::ResourceState) -> Vec<crate::ResourceListColumnValueView>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourcePresentationDefinition {
    pub resource_name: &'static str,
    pub resource_short_names: &'static [&'static str],
    pub list_columns: &'static [ResourceListColumnDefinition],
}

impl ResourcePresentationDefinition {
    pub const fn new(
        resource_name: &'static str,
        resource_short_names: &'static [&'static str],
        list_columns: &'static [ResourceListColumnDefinition],
    ) -> Self {
        Self {
            resource_name,
            resource_short_names,
            list_columns,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceListColumnDefinition {
    pub key: &'static str,
    pub header: &'static str,
    pub data_type: ResourceListColumnDataType,
    pub visibility: ResourceListColumnVisibility,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Display, EnumString, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum ResourceListColumnDataType {
    String,
    UInt64,
    Bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Display, EnumString, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
pub enum ResourceListColumnVisibility {
    Default,
    WideOnly,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_list_column_data_type_has_identical_serde_and_strum_serialization() {
        let cases = [
            (ResourceListColumnDataType::String, "\"string\"", "string"),
            (ResourceListColumnDataType::UInt64, "\"uint64\"", "uint64"),
            (ResourceListColumnDataType::Bool, "\"bool\"", "bool"),
        ];

        for (value, serde_value, strum_value) in cases {
            assert_eq!(serde_json::to_string(&value).unwrap(), serde_value);
            assert_eq!(value.to_string(), strum_value);
            assert_eq!(
                strum_value.parse::<ResourceListColumnDataType>().unwrap(),
                value
            );
        }
    }

    #[test]
    fn resource_list_column_visibility_has_identical_serde_and_strum_serialization() {
        let cases = [
            (
                ResourceListColumnVisibility::Default,
                "\"default\"",
                "default",
            ),
            (
                ResourceListColumnVisibility::WideOnly,
                "\"wideOnly\"",
                "wideOnly",
            ),
        ];

        for (value, serde_value, strum_value) in cases {
            assert_eq!(serde_json::to_string(&value).unwrap(), serde_value);
            assert_eq!(value.to_string(), strum_value);
            assert_eq!(
                strum_value.parse::<ResourceListColumnVisibility>().unwrap(),
                value
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
