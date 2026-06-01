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

#[derive(cynic::Enum, Debug, Clone, Copy)]
pub(crate) enum ResourceBuiltinKind {
    SecretSet,
    VariableSet,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceKindInput")]
#[allow(dead_code)]
pub(crate) enum ResourceKindInput {
    Builtin(ResourceBuiltinKind),
    Custom(String),
}

impl ResourceKindInput {
    pub(crate) fn custom(kind: String) -> Self {
        Self::Custom(kind)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceAccountSelectorInput")]
pub(crate) enum ResourceAccountSelectorInput {
    ById(odf::AccountID),
    ByName(AccountName),
}

impl TryFrom<&domain::ResourceManifestAccount> for ResourceAccountSelectorInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &domain::ResourceManifestAccount) -> Result<Self, Self::Error> {
        if let Some(id) = &value.id {
            Ok(Self::ById(id.clone()))
        } else if let Some(name) = &value.name {
            Ok(Self::ByName(AccountName(name.clone())))
        } else {
            Err(internal_error::InternalError::new(
                "Remote resource request account selector must contain either id or name",
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceByNameSelectorInput")]
pub(crate) struct ResourceByNameSelectorInput {
    pub name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InputObject, Debug, Clone)]
#[cynic(graphql_type = "ResourceRefInput")]
pub(crate) enum ResourceRefInput {
    ById(domain::ResourceUID),
    ByName(ResourceByNameSelectorInput),
}

impl From<&ResourceRef> for ResourceRefInput {
    fn from(value: &ResourceRef) -> Self {
        match value {
            ResourceRef::ById(uid) => Self::ById(*uid),
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
    pub api_version: Option<String>,

    #[cynic(rename = "ref")]
    pub ref_: ResourceRefInput,

    pub account: Option<ResourceAccountSelectorInput>,
}

impl TryFrom<&ResourceSelector> for ResourceSelectorInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &ResourceSelector) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: ResourceKindInput::custom(value.kind.clone()),
            api_version: value.api_version.clone(),
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
    pub api_version: Option<String>,
    pub refs: Vec<ResourceRefInput>,
    pub account: Option<ResourceAccountSelectorInput>,
}

impl TryFrom<&ResourceBatchSelector> for ResourceBatchSelectorInput {
    type Error = internal_error::InternalError;

    fn try_from(value: &ResourceBatchSelector) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: ResourceKindInput::custom(value.kind.clone()),
            api_version: value.api_version.clone(),
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
    pub names: Option<Vec<String>>,
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
                .map(|k| ResourceKindInput::custom(k.clone()))
                .collect(),
            names: value.exact_names.clone(),
            name_pattern: value.name_pattern.clone(),
            account: value.account.as_ref().map(TryInto::try_into).transpose()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
