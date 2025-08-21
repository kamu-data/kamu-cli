// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_webhooks::{
    WebhookSendConnectionTimeoutError,
    WebhookSendFailedToConnectError,
    WebhookUnsuccessfulResponseError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_error_enum! {
    pub enum TaskErrorWebhookDelivery {
        FailedToConnect(WebhookSendFailedToConnectError),
        ConnectionTimeout(WebhookSendConnectionTimeoutError),
        UnsuccessfulResponse(WebhookUnsuccessfulResponseError),
    }
    => "WebhookDeliveryError"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TaskErrorWebhookDelivery {
    pub fn target_url(&self) -> &url::Url {
        match self {
            TaskErrorWebhookDelivery::FailedToConnect(e) => &e.target_url,
            TaskErrorWebhookDelivery::ConnectionTimeout(e) => &e.target_url,
            TaskErrorWebhookDelivery::UnsuccessfulResponse(e) => &e.target_url,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
