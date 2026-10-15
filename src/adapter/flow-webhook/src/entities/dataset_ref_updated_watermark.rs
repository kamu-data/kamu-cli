// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_webhooks::WebhookWatermark;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Watermark of the dataset reference update events: the dataset head the
/// receiver already knows
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatasetRefUpdatedWatermark {
    pub head: odf::Multihash,
}

impl TryFrom<WebhookWatermark> for DatasetRefUpdatedWatermark {
    type Error = InternalError;

    fn try_from(watermark: WebhookWatermark) -> Result<Self, Self::Error> {
        serde_json::from_value(serde_json::Value::from(watermark)).int_err()
    }
}

impl From<DatasetRefUpdatedWatermark> for WebhookWatermark {
    fn from(watermark: DatasetRefUpdatedWatermark) -> Self {
        Self::from(serde_json::json!({ "head": watermark.head }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
