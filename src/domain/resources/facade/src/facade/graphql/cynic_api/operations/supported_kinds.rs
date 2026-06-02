// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cynic::QueryBuilder;

use crate::facade::graphql::cynic_api::fragments::ResourceKind;
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Query")]
pub(crate) struct SupportedKindsQuery {
    pub resources: SupportedKindsResources,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Resources")]
pub(crate) struct SupportedKindsResources {
    pub supported_kinds: Vec<ResourceKindDescriptor>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceKindDescriptor {
    pub name: String,
    pub short_names: Vec<String>,
    pub kind: ResourceKind,
    pub api_version: String,
    pub list_columns: Vec<ResourceListColumnDescriptor>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceListColumnDescriptor {
    pub key: String,
    pub header: String,
    pub data_type: String,
    pub visibility: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_operation() -> cynic::Operation<SupportedKindsQuery, ()> {
    SupportedKindsQuery::build(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
