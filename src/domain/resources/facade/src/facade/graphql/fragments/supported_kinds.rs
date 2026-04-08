// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Deserialize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SupportedKindsQueryDataFragment {
    pub resources: SupportedKindsFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SupportedKindsFragment {
    pub supported_kinds: Vec<ResourceKindDescriptorFragment>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceKindDescriptorFragment {
    pub name: String,
    pub short_names: Vec<String>,
    pub kind: ResourceKindFragment,
    pub api_version: String,
    pub list_columns: Vec<ResourceListColumnDescriptorFragment>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResourceKindFragment {
    pub value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceListColumnDescriptorFragment {
    pub key: String,
    pub header: String,
    pub data_type: String,
    pub visibility: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
