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
pub(crate) struct GetResourceIdentityQueryDataFragment {
    pub resources: GetResourceIdentityFieldFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetResourceIdentityFieldFragment {
    pub resource_identity: Option<ResourceIdentityFragment>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchGetResourceIdentitiesQueryDataFragment {
    pub resources: BatchGetResourceIdentitiesFieldFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchGetResourceIdentitiesFieldFragment {
    pub resource_identities: BatchResourceIdentitiesResultFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchResourceIdentitiesResultFragment {
    pub identities: Vec<BatchResourceIdentitySuccessFragment>,
    pub problems: Vec<super::BatchResourceProblemFragment>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchResourceIdentitySuccessFragment {
    pub request_index: usize,
    pub identity: ResourceIdentityFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListIdentitiesByKindQueryDataFragment {
    pub resources: ListIdentitiesByKindResourcesFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListIdentitiesByKindResourcesFragment {
    pub list_identities_by_kind: ResourceIdentityConnectionFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListAllIdentitiesQueryDataFragment {
    pub resources: ListAllIdentitiesResourcesFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListAllIdentitiesResourcesFragment {
    pub list_all_identities: ResourceIdentityConnectionFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SearchIdentitiesQueryDataFragment {
    pub resources: SearchIdentitiesResourcesFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SearchIdentitiesResourcesFragment {
    pub search_identities: ResourceIdentityConnectionFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceIdentityConnectionFragment {
    pub nodes: Vec<ResourceIdentityFragment>,
    pub total_count: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceIdentityFragment {
    pub id: kamu_resources::ResourceUID,
    pub api_version: String,
    pub kind: ResourceIdentityKindFragment,
    pub canonical_kind_name: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceIdentityKindFragment {
    pub value: String,
}

impl From<ResourceIdentityFragment> for kamu_resources::ResourceIdentityView {
    fn from(value: ResourceIdentityFragment) -> Self {
        Self {
            kind: value.kind.value,
            api_version: value.api_version,
            canonical_kind_name: value.canonical_kind_name,
            uid: value.id,
            name: value.name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
