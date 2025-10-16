// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use chrono::Duration;

pub const DEFAULT_MAX_WEBHOOK_CONSECUTIVE_FAILURES: u32 = 5;
pub const DEFAULT_WEBHOOK_DELIVERY_TIMEOUT: u32 = 10;
pub const SAMPLE_WEBHOOK_SUBSCRIPTION_SECRET_KEY_ENCRYPTION_KEY: &str =
    "QfnEDcnUtGSW2pwVXaFPvZOwxyFm2BOC";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct WebhooksConfig {
    pub max_consecutive_failures: u32,
    pub delivery_timeout: Duration,
    pub secret_encryption_key: Option<String>,
}

impl WebhooksConfig {
    pub fn new(
        max_consecutive_failures: u32,
        delivery_timeout: Duration,
        secret_encryption_key: Option<String>,
    ) -> Self {
        Self {
            max_consecutive_failures,
            delivery_timeout,
            secret_encryption_key,
        }
    }

    pub fn sample() -> Self {
        Self {
            secret_encryption_key: Some(
                SAMPLE_WEBHOOK_SUBSCRIPTION_SECRET_KEY_ENCRYPTION_KEY.to_string(),
            ),
            ..Default::default()
        }
    }
}

impl Default for WebhooksConfig {
    fn default() -> Self {
        Self {
            max_consecutive_failures: DEFAULT_MAX_WEBHOOK_CONSECUTIVE_FAILURES,
            delivery_timeout: Duration::seconds(i64::from(DEFAULT_WEBHOOK_DELIVERY_TIMEOUT)),
            secret_encryption_key: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
