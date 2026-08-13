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
use crate::{ResourceBatchSelector, ResourceRef, ResourceSelector, SearchResourceHandlesRequest};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceTypeSelectorInput")]
pub(crate) struct ResourceTypeSelectorInput {
    pub selector: domain::ResourceTypeSelectorRaw,
}

impl ResourceTypeSelectorInput {
    pub(crate) fn from_resource_type(resource_type: &domain::ResourceTypeSelectorRaw) -> Self {
        Self {
            selector: resource_type.clone(),
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

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceByNameSelectorInput")]
pub(crate) struct ResourceByNameSelectorInput {
    pub name: domain::ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceRefInput")]
pub(crate) enum ResourceRefInput {
    ById(domain::ResourceID),
    ByName(ResourceByNameSelectorInput),
}

impl From<&ResourceRef> for ResourceRefInput {
    fn from(value: &ResourceRef) -> Self {
        match value {
            ResourceRef::ById(id) => Self::ById(*id),
            ResourceRef::ByName(name) => {
                Self::ByName(ResourceByNameSelectorInput { name: name.clone() })
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceSelectorInput")]
pub(crate) struct ResourceSelectorInput {
    pub resource_type: ResourceTypeSelectorInput,

    #[cynic(rename = "ref")]
    pub ref_: ResourceRefInput,

    pub account: Option<AccountRefInput>,
}

impl TryFrom<&ResourceSelector> for ResourceSelectorInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &ResourceSelector) -> Result<Self, Self::Error> {
        Ok(Self {
            resource_type: ResourceTypeSelectorInput::from_resource_type(&value.resource_type),
            ref_: (&value.resource_ref).into(),
            account: value.account.as_ref().map(Into::into),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceBatchSelectorInput")]
pub(crate) struct ResourceBatchSelectorInput {
    pub resource_type: ResourceTypeSelectorInput,
    pub refs: Vec<ResourceRefInput>,
    pub account: Option<AccountRefInput>,
}

impl TryFrom<&ResourceBatchSelector> for ResourceBatchSelectorInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &ResourceBatchSelector) -> Result<Self, Self::Error> {
        Ok(Self {
            resource_type: ResourceTypeSelectorInput::from_resource_type(&value.resource_type),
            refs: value.resource_refs.iter().map(Into::into).collect(),
            account: value.account.as_ref().map(Into::into),
        })
    }
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

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceQueryInput")]
pub(crate) struct ResourceQueryInput {
    pub exact_names: Option<Vec<domain::ResourceName>>,
    pub exact_ids: Option<Vec<domain::ResourceID>>,
    pub name_pattern: Option<String>,
}

impl From<&domain::ResourceQuery> for ResourceQueryInput {
    fn from(value: &domain::ResourceQuery) -> Self {
        match value {
            domain::ResourceQuery::ExactNames(names) => Self {
                exact_names: Some(names.clone()),
                exact_ids: None,
                name_pattern: None,
            },
            domain::ResourceQuery::ExactIds(ids) => Self {
                exact_names: None,
                exact_ids: Some(ids.clone()),
                name_pattern: None,
            },
            domain::ResourceQuery::NamePattern(pattern) => Self {
                exact_names: None,
                exact_ids: None,
                name_pattern: Some(pattern.clone()),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceTypeQueryInput")]
pub(crate) struct ResourceTypeQueryInput {
    pub resource_type: ResourceTypeSelectorInput,
    pub query: Option<ResourceQueryInput>,
}

impl From<&crate::RawResourceTypeQuery> for ResourceTypeQueryInput {
    fn from(value: &crate::RawResourceTypeQuery) -> Self {
        Self {
            resource_type: ResourceTypeSelectorInput::from_resource_type(&value.raw_type_selector),
            query: value.query.as_ref().map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceScopeInput")]
pub(crate) struct ResourceScopeInput {
    pub any_type: Option<ResourceAnyTypeScopeInput>,
    pub types: Option<Vec<ResourceTypeQueryInput>>,
}

impl From<&crate::RawResourceScope> for ResourceScopeInput {
    fn from(value: &crate::RawResourceScope) -> Self {
        match value {
            crate::RawResourceScope::AnyType(query) => Self {
                any_type: Some(ResourceAnyTypeScopeInput {
                    query: query.as_ref().map(Into::into),
                }),
                types: None,
            },
            crate::RawResourceScope::Types(type_queries) => Self {
                any_type: None,
                types: Some(type_queries.iter().map(Into::into).collect()),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceAnyTypeScopeInput")]
pub(crate) struct ResourceAnyTypeScopeInput {
    pub query: Option<ResourceQueryInput>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "SearchResourceHandlesInput")]
pub(crate) struct SearchResourceHandlesInput {
    pub scope: ResourceScopeInput,
    pub account: Option<AccountRefInput>,
    pub label_filter: Option<ResourceLabelFilterInput>,
}

impl TryFrom<&SearchResourceHandlesRequest> for SearchResourceHandlesInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &SearchResourceHandlesRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            scope: (&value.scope).into(),
            account: value.account.as_ref().map(Into::into),
            label_filter: value.label_filter.as_ref().map(Into::into),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
