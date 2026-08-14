// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources as domain;

use crate::facade::graphql::cynic_api::fragments::ResourceManifestFormat;
use crate::facade::graphql::cynic_api::scalars::AccountName;
use crate::facade::graphql::cynic_api::schema;
use crate::{SearchResourceHandlesRequest, SearchResourcesRequest};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceTypeSelectorInput")]
pub(crate) struct ResourceTypeSelectorInput {
    pub selector: domain::ResourceTypeSelectorRaw,
}

impl ResourceTypeSelectorInput {
    /// The server resolves canonical selectors, aliases, ODF type names, and
    /// schema URIs through one lookup, so a `TypeRef` passes through as-is.
    pub(crate) fn from_type_ref(type_ref: &domain::TypeRef) -> Self {
        Self {
            selector: domain::ResourceTypeSelectorRaw::new_unchecked(type_ref.as_str()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "AccountRefInput")]
pub(crate) struct AccountRefInput {
    pub id: Option<kamu_resources::ResourceID>,
    pub did: Option<odf::AccountID>,
    pub name: Option<AccountName>,
}

impl From<&domain::ResourceAccountRef> for AccountRefInput {
    fn from(value: &domain::ResourceAccountRef) -> Self {
        Self {
            id: value.id,
            did: value.did.clone(),
            name: value
                .name
                .as_ref()
                .map(|name| AccountName(name.to_string())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Mirrors the server's `ResourceRefInput`: a plain input object, not a
/// `oneOf`, so `id` and `name` can travel together as ODF's consistency
/// assertion.
#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceRefInput")]
pub(crate) struct ResourceRefInput {
    pub account: Option<AccountRefInput>,

    #[cynic(rename = "type")]
    pub type_: ResourceTypeSelectorInput,

    pub id: Option<domain::ResourceID>,
    pub name: Option<domain::ResourceName>,
}

impl From<&domain::ResourceRef> for ResourceRefInput {
    fn from(value: &domain::ResourceRef) -> Self {
        Self {
            account: value.account.as_ref().map(Into::into),
            type_: ResourceTypeSelectorInput::from_type_ref(&value.r#type),
            id: value.id,
            name: value.name.clone(),
            // `did` is deliberately absent: the facade rejects a populated one
            // before a request is ever built, so it can never reach the wire.
        }
    }
}

pub(crate) fn resource_ref_inputs(resource_refs: &[domain::ResourceRef]) -> Vec<ResourceRefInput> {
    resource_refs.iter().map(Into::into).collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceLabelFilterEntryInput")]
pub(crate) struct ResourceLabelFilterEntryInput {
    pub key: String,
    pub value: serde_json::Value,
}

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceLabelFilterInput")]
pub(crate) struct ResourceLabelFilterInput {
    pub entries: Vec<ResourceLabelFilterEntryInput>,
}

impl From<&domain::ResourceLabelFilterInput> for ResourceLabelFilterInput {
    fn from(value: &domain::ResourceLabelFilterInput) -> Self {
        Self {
            entries: value
                .entries
                .iter()
                .map(|(key, value)| ResourceLabelFilterEntryInput {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ApplyManifestInput")]
pub(crate) struct ApplyManifestInput {
    pub manifest: String,
    pub format: ResourceManifestFormat,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Mirrors the server's `ResourceSelectorInput`: the ODF selector shape, with
/// every field optional so one selector can span every type.
#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceSelectorInput")]
pub(crate) struct ResourceSelectorInput {
    pub account: Option<AccountRefInput>,

    #[cynic(rename = "type")]
    pub type_: Option<ResourceTypeSelectorInput>,

    pub id: Option<domain::ResourceID>,
    pub name: Option<String>,
    pub labels: Option<ResourceLabelFilterInput>,
}

impl From<&domain::ResourceSelector> for ResourceSelectorInput {
    fn from(value: &domain::ResourceSelector) -> Self {
        Self {
            account: value.account.as_ref().map(Into::into),
            type_: value
                .r#type
                .as_ref()
                .map(ResourceTypeSelectorInput::from_type_ref),
            id: value.id,
            name: value.name.clone(),
            labels: None,
        }
    }
}

pub(crate) fn resource_selector_inputs(
    selectors: &[domain::ResourceSelector],
) -> Vec<ResourceSelectorInput> {
    selectors.iter().map(Into::into).collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "SearchResourceHandlesInput")]
pub(crate) struct SearchResourceHandlesInput {
    pub selectors: Vec<ResourceSelectorInput>,
    pub account: Option<AccountRefInput>,
    pub label_filter: Option<ResourceLabelFilterInput>,
}

impl TryFrom<&SearchResourceHandlesRequest> for SearchResourceHandlesInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &SearchResourceHandlesRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            selectors: resource_selector_inputs(&value.selectors),
            account: value.account.as_ref().map(Into::into),
            label_filter: value.label_filter.as_ref().map(Into::into),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "SearchResourcesInput")]
pub(crate) struct SearchResourcesInput {
    pub selectors: Option<Vec<ResourceSelectorInput>>,
    pub account: Option<AccountRefInput>,
    pub label_filter: Option<ResourceLabelFilterInput>,
}

impl TryFrom<&SearchResourcesRequest> for SearchResourcesInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &SearchResourcesRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            // Always sent explicitly. The field is nullable so that omitting it
            // spans every type, but the facade has already resolved that to a
            // concrete selector list by this point.
            selectors: Some(resource_selector_inputs(&value.selectors)),
            account: value.account.as_ref().map(Into::into),
            label_filter: value.label_filter.as_ref().map(Into::into),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
