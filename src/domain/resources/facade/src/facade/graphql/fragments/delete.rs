// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources as domain;
use serde::Deserialize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeleteMutationDataFragment {
    pub resources: ResourcesDeleteMutFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourcesDeleteMutFragment {
    pub delete: ResourceDeleteResultFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeleteManyMutationDataFragment {
    pub resources: ResourcesDeleteManyMutFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourcesDeleteManyMutFragment {
    pub delete_many: BatchDeleteResourcesResultFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceDeleteResultFragment {
    pub resource_id: domain::ResourceUID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchDeleteResourcesResultFragment {
    pub resources: Vec<BatchDeleteResourceSuccessFragment>,
    pub problems: Vec<super::BatchResourceProblemFragment>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchDeleteResourceSuccessFragment {
    pub request_index: usize,
    pub resource_id: domain::ResourceUID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
