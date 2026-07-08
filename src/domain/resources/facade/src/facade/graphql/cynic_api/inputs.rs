// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources as domain;

use crate::facade::graphql::cynic_api::scalars::AccountName;
use crate::facade::graphql::cynic_api::schema;
use crate::{
    ResourceBatchSelector,
    ResourceRef,
    ResourceSelector,
    SearchResourceIdentitiesRequest,
};

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
#[cynic(graphql_type = "AccountHandleInput")]
pub(crate) struct AccountHandleInput {
    pub id: odf::AccountID,
    pub name: AccountName,
}

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceAccountSelectorInput")]
pub(crate) enum ResourceAccountSelectorInput {
    ById(odf::AccountID),
    ByName(AccountName),
    Handle(AccountHandleInput),
}

impl From<&domain::ResourceAccountRef> for ResourceAccountSelectorInput {
    fn from(value: &domain::ResourceAccountRef) -> Self {
        match value {
            domain::ResourceAccountRef::Id(id) => Self::ById(id.clone()),
            domain::ResourceAccountRef::Name(name) => Self::ByName(AccountName(name.to_string())),
            domain::ResourceAccountRef::Handle(odf::metadata::auth::AccountHandle { id, name }) => {
                Self::Handle(AccountHandleInput {
                    id: id.clone(),
                    name: AccountName(name.to_string()),
                })
            }
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

    pub account: Option<ResourceAccountSelectorInput>,
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
    pub account: Option<ResourceAccountSelectorInput>,
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
#[cynic(graphql_type = "SearchResourceIdentitiesInput")]
pub(crate) struct SearchResourceIdentitiesInput {
    pub resource_types: Vec<ResourceTypeSelectorInput>,
    pub names: Option<Vec<domain::ResourceName>>,
    pub name_pattern: Option<String>,
    pub account: Option<ResourceAccountSelectorInput>,
}

impl TryFrom<&SearchResourceIdentitiesRequest> for SearchResourceIdentitiesInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &SearchResourceIdentitiesRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            resource_types: value
                .raw_type_selectors
                .iter()
                .map(ResourceTypeSelectorInput::from_resource_type)
                .collect(),
            names: value.exact_names.clone(),
            name_pattern: value.name_pattern.clone(),
            account: value.account.as_ref().map(Into::into),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
