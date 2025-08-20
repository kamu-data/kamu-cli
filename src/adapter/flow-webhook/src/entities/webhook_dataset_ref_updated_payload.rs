// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const WEBHOOK_DATASET_REF_UPDATED_VERSION: u32 = 2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
pub(crate) struct WebhookDatasetRefUpdatedPayload {
    pub version: u32,
    pub dataset_id: String,
    pub owner_account_id: String,
    pub block_ref: String,
    pub new_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_hash: Option<String>,
    pub is_breaking_change: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
