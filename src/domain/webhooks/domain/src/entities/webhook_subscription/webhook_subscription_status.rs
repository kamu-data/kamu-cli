// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "sqlx",
    derive(sqlx::Type),
    sqlx(type_name = "webhook_subscription_status", rename_all = "snake_case")
)]
pub enum WebhookSubscriptionStatus {
    Unverified,
    Enabled,
    Paused,
    Unreachable,
    Removed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
