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
pub(crate) struct AdminGetResourceIdentityQueryDataFragment {
    pub admin: AdminGetResourceIdentityRootFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminGetResourceIdentityRootFragment {
    pub resources: GetResourceIdentityFieldFragment,
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
pub(crate) struct AdminListIdentitiesByKindQueryDataFragment {
    pub admin: AdminListIdentitiesByKindFieldFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminListIdentitiesByKindFieldFragment {
    pub resources: ListIdentitiesByKindResourcesFragment,
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
pub(crate) struct AdminListAllIdentitiesQueryDataFragment {
    pub admin: AdminListAllIdentitiesFieldFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminListAllIdentitiesFieldFragment {
    pub resources: ListAllIdentitiesResourcesFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceIdentityConnectionFragment {
    pub nodes: Vec<ResourceIdentityFragment>,
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
