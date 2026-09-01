// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cynic::MutationBuilder;

use crate::ApplyManifestBatchRequest;
use crate::facade::graphql::cynic_api::inputs::ApplyManifestInput;
use crate::facade::graphql::cynic_api::operations::apply::ResourceApplyOutcome;
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Mutation", variables = "ApplyManifestsVariables")]
pub(crate) struct ApplyManifestsMutation {
    pub resources: ApplyManifestsResourcesMut,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "ResourcesMut", variables = "ApplyManifestsVariables")]
pub(crate) struct ApplyManifestsResourcesMut {
    #[arguments(manifests: $manifests, dryRun: $dry_run)]
    pub apply_manifests: ResourceApplyManifestsResult,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceApplyManifestsResult {
    pub items: Vec<ResourceApplyManifestItemResult>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceApplyManifestItemResult {
    pub request_index: i32,
    pub outcome: ResourceApplyOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ApplyManifestsVariables {
    pub manifests: Vec<ApplyManifestInput>,
    pub dry_run: bool,
}

impl ApplyManifestsVariables {
    pub(crate) fn new(request: ApplyManifestBatchRequest, dry_run: bool) -> Self {
        Self {
            manifests: request
                .items
                .into_iter()
                .map(|item| ApplyManifestInput {
                    manifest: item.manifest,
                    format: item.format.into(),
                })
                .collect(),
            dry_run,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_operation(
    variables: ApplyManifestsVariables,
) -> cynic::Operation<ApplyManifestsMutation, ApplyManifestsVariables> {
    ApplyManifestsMutation::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
