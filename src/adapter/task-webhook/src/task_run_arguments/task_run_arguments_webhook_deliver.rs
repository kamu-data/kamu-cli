// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_webhooks::WebhookEventType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_run_arguments_struct! {
    pub struct TaskRunArgumentsWebhookDeliver {
        pub event_type: WebhookEventType,
        pub payload: serde_json::Value,
    }
    => "WebhookDeliverArguments"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
