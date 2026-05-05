// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Deserialize;

use super::ResourceFragment;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetResourceQueryDataFragment {
    pub resources: GetResourceFieldFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetResourceFieldFragment {
    pub resource: Option<ResourceFragment>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminGetResourceQueryDataFragment {
    pub admin: AdminGetResourceRootFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminGetResourceRootFragment {
    pub resources: GetResourceFieldFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchGetResourcesQueryDataFragment {
    pub resources: BatchGetResourcesFieldFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchGetResourcesFieldFragment {
    pub resources: BatchResourcesResultFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminBatchGetResourcesQueryDataFragment {
    pub admin: AdminBatchGetResourcesRootFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminBatchGetResourcesRootFragment {
    pub resources: BatchGetResourcesFieldFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchResourcesResultFragment {
    pub resources: Vec<BatchResourceSuccessFragment>,
    pub problems: Vec<BatchResourceProblemFragment>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchResourceSuccessFragment {
    pub request_index: usize,
    pub resource: ResourceFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchResourceProblemFragment {
    pub request_index: usize,
    pub code: BatchResourceProblemCodeFragment,
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum BatchResourceProblemCodeFragment {
    UidNotFound,
    NameNotFound,
    ApiVersionMismatch,
    KindMismatch,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
