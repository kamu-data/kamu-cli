// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::Object;

use crate::queries::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Webhooks;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl Webhooks {
    /// List of supported event types
    async fn event_types(&self) -> Vec<String> {
        kamu_webhooks::WebhookEventTypeCatalog::all_non_test()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// Access to the webhook subscriptions methods
    async fn subscriptions(&self) -> WebhookSubscriptions {
        WebhookSubscriptions
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
