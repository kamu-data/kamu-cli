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

use crate::ResourcesSummaryRequest;
use crate::facade::graphql::cynic_api::fragments::{ResourceBadAccountProblem, ResourcesSummary};
use crate::facade::graphql::cynic_api::inputs::ResourceAccountSelectorInput;
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query", variables = "SummaryVariables")]
pub(crate) struct SummaryQuery {
    pub resources: SummaryResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources", variables = "SummaryVariables")]
pub(crate) struct SummaryResources {
    #[arguments(account: $account)]
    pub summary: ResourcesSummaryOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
pub(crate) enum ResourcesSummaryOutcome {
    ResourcesSummary(ResourcesSummary),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct SummaryVariables {
    pub account: Option<ResourceAccountSelectorInput>,
}

impl SummaryVariables {
    pub(crate) fn new(request: &ResourcesSummaryRequest) -> Result<Self, InternalError> {
        Ok(Self {
            account: request
                .account
                .as_ref()
                .map(TryInto::try_into)
                .transpose()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_operation(
    variables: SummaryVariables,
) -> cynic::Operation<SummaryQuery, SummaryVariables> {
    SummaryQuery::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
