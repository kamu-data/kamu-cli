// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::Object;

use crate::mutations::WebhookSubscriptionsMut;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhooksMut;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl WebhooksMut {
    /// Access to the mutable webhook subscriptions methods
    async fn subscriptions(&self) -> WebhookSubscriptionsMut {
        WebhookSubscriptionsMut
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
