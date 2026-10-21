// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Position in a stream of events that the receiver of a subscription has
/// acknowledged. Opaque to the webhooks core: the producer of the event type
/// defines its contents
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct WebhookWatermark(serde_json::Value);

impl From<serde_json::Value> for WebhookWatermark {
    fn from(value: serde_json::Value) -> Self {
        Self(value)
    }
}

impl From<WebhookWatermark> for serde_json::Value {
    fn from(watermark: WebhookWatermark) -> Self {
        watermark.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
