// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait WebhookPayloadBuilder: Send + Sync {
    async fn build_dataset_ref_updated_payload(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        new_block_hash: &odf::Multihash,
        maybe_prev_block_hash: Option<&odf::Multihash>,
    ) -> Result<serde_json::Value, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
