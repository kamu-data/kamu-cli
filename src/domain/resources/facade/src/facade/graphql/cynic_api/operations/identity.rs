// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cynic::QueryBuilder;
use internal_error::InternalError;

use crate::facade::graphql::cynic_api::fragments::{BatchResourceProblem, ResourceIdentity};
use crate::facade::graphql::cynic_api::inputs::{
    ResourceBatchSelectorInput,
    ResourceSelectorInput,
};
use crate::facade::graphql::cynic_api::schema;
use crate::{ResourceBatchSelector, ResourceSelector};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Query",
    variables = "ResourceIdentitySelectorVariables"
)]
pub(crate) struct GetResourceIdentityQuery {
    pub resources: ResourceIdentityResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Resources",
    variables = "ResourceIdentitySelectorVariables"
)]
pub(crate) struct ResourceIdentityResources {
    #[arguments(selector: $selector)]
    pub resource_identity: Option<ResourceIdentity>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Query",
    variables = "ResourceIdentityBatchSelectorVariables"
)]
pub(crate) struct GetResourceIdentitiesQuery {
    pub resources: ResourceIdentitiesResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    graphql_type = "Resources",
    variables = "ResourceIdentityBatchSelectorVariables"
)]
pub(crate) struct ResourceIdentitiesResources {
    #[arguments(selector: $selector)]
    pub resource_identities: BatchResourceIdentitiesResult,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourceIdentitiesResult {
    pub identities: Vec<BatchResourceIdentitySuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct BatchResourceIdentitySuccess {
    pub request_index: i32,
    pub identity: ResourceIdentity,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ResourceIdentitySelectorVariables {
    pub selector: ResourceSelectorInput,
}

impl ResourceIdentitySelectorVariables {
    pub(crate) fn new(selector: &ResourceSelector) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ResourceIdentityBatchSelectorVariables {
    pub selector: ResourceBatchSelectorInput,
}

impl ResourceIdentityBatchSelectorVariables {
    pub(crate) fn new(selector: &ResourceBatchSelector) -> Result<Self, InternalError> {
        Ok(Self {
            selector: selector.try_into()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_identity_operation(
    variables: ResourceIdentitySelectorVariables,
) -> cynic::Operation<GetResourceIdentityQuery, ResourceIdentitySelectorVariables> {
    GetResourceIdentityQuery::build(variables)
}

pub(crate) fn build_identities_operation(
    variables: ResourceIdentityBatchSelectorVariables,
) -> cynic::Operation<GetResourceIdentitiesQuery, ResourceIdentityBatchSelectorVariables> {
    GetResourceIdentitiesQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
