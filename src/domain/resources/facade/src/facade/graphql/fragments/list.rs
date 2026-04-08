// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use serde::Deserialize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListByKindQueryDataFragment {
    pub resources: ListByKindResourcesFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListByKindResourcesFragment {
    pub list_by_kind: ResourceConnectionFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminListByKindQueryDataFragment {
    pub admin: AdminListByKindFieldFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminListByKindFieldFragment {
    pub resources: ListByKindResourcesFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListAllQueryDataFragment {
    pub resources: ListAllResourcesFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListAllResourcesFragment {
    pub list_all: ResourceConnectionFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminListAllQueryDataFragment {
    pub admin: AdminListAllFieldFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminListAllFieldFragment {
    pub resources: ListAllResourcesFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceConnectionFragment {
    pub nodes: Vec<ResourceSummaryFragment>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceSummaryFragment {
    pub id: kamu_resources::ResourceUID,
    pub api_version: String,
    pub kind: ResourceKindFragment,
    pub name: String,
    pub description: Option<String>,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: Option<ResourceStatusSummaryFragment>,
    pub list_values: Vec<ResourceListColumnValueViewFragment>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceKindFragment {
    pub value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceStatusSummaryFragment {
    pub phase: Option<String>,
    pub observed_generation: Option<u64>,
    pub ready: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceListColumnValueViewFragment {
    pub key: String,
    pub string_value: Option<String>,
    pub uint64_value: Option<u64>,
    pub bool_value: Option<bool>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
