// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system as fs;
use kamu_webhooks::WebhookSubscriptionID;

use crate::{FLOW_TYPE_WEBHOOK_DELIVER, FlowScopeSubscription};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[inline]
pub fn webhook_deliver_binding(
    subscription_id: WebhookSubscriptionID,
    dataset_id: Option<&odf::DatasetID>,
) -> fs::FlowBinding {
    fs::FlowBinding::new(
        FLOW_TYPE_WEBHOOK_DELIVER,
        FlowScopeSubscription::make_scope(subscription_id, dataset_id),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
