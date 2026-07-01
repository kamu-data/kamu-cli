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
#[cynic(graphql_type = "ResourceKindInput")]
pub(crate) struct ResourceKindInput {
    pub kind: String,
}

impl ResourceKindInput {
    pub(crate) fn from_kind(kind: &str) -> Self {
        Self {
            kind: kind.to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceAccountSelectorInput")]
pub(crate) struct ResourceAccountSelectorInput {
    pub by_id: Option<odf::AccountID>,
    pub by_name: Option<AccountName>,
}

impl TryFrom<&domain::ResourceAccountRef> for ResourceAccountSelectorInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &domain::ResourceAccountRef) -> Result<Self, Self::Error> {
        Ok(Self {
            by_id: value.id.clone(),
            by_name: value.name.clone().map(AccountName),
        })
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
    pub kind: ResourceKindInput,

    #[cynic(rename = "ref")]
    pub ref_: ResourceRefInput,

    pub account: Option<ResourceAccountSelectorInput>,
}

impl TryFrom<&ResourceSelector> for ResourceSelectorInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &ResourceSelector) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: ResourceKindInput::from_kind(&value.kind),
            ref_: (&value.resource_ref).into(),
            account: value.account.as_ref().map(TryInto::try_into).transpose()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceBatchSelectorInput")]
pub(crate) struct ResourceBatchSelectorInput {
    pub kind: ResourceKindInput,
    pub refs: Vec<ResourceRefInput>,
    pub account: Option<ResourceAccountSelectorInput>,
}

impl TryFrom<&ResourceBatchSelector> for ResourceBatchSelectorInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &ResourceBatchSelector) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: ResourceKindInput::from_kind(&value.kind),
            refs: value.resource_refs.iter().map(Into::into).collect(),
            account: value.account.as_ref().map(TryInto::try_into).transpose()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "SearchResourceIdentitiesInput")]
pub(crate) struct SearchResourceIdentitiesInput {
    pub kinds: Vec<ResourceKindInput>,
    pub names: Option<Vec<domain::ResourceName>>,
    pub name_pattern: Option<String>,
    pub account: Option<ResourceAccountSelectorInput>,
}

impl TryFrom<&SearchResourceIdentitiesRequest> for SearchResourceIdentitiesInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &SearchResourceIdentitiesRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            kinds: value
                .kinds
                .iter()
                .map(|k| ResourceKindInput::from_kind(k))
                .collect(),
            names: value.exact_names.clone(),
            name_pattern: value.name_pattern.clone(),
            account: value.account.as_ref().map(TryInto::try_into).transpose()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
