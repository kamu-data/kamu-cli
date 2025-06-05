// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

simple_scalar!(WebhookSubscriptionID, kamu_webhooks::WebhookSubscriptionID);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_webhooks::WebhookSubscriptionStatus")]
pub enum WebhookSubscriptionStatus {
    Unverified,
    Enabled,
    Paused,
    Unreachable,
    Removed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookEventType(pub String);

impl FromStr for WebhookEventType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let allowed_types = kamu_webhooks::WebhookEventTypeCatalog::all_non_test_as_str();

        if allowed_types.contains(&s) {
            Ok(WebhookEventType(s.to_string()))
        } else {
            Err(format!(
                "Invalid value '{}'. Must be one of: {:?}",
                s,
                allowed_types.join(", ")
            ))
        }
    }
}

#[Scalar()]
impl ScalarType for WebhookEventType {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(s) = &value {
            WebhookEventType::from_str(s).map_err(InputValueError::custom)
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookSubscriptionlabel(pub kamu_webhooks::WebhookSubscriptionLabel);

impl FromStr for WebhookSubscriptionlabel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        kamu_webhooks::WebhookSubscriptionLabel::try_new(s)
            .map(WebhookSubscriptionlabel)
            .map_err(|e| e.to_string())
    }
}

#[Scalar()]
impl ScalarType for WebhookSubscriptionlabel {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(s) = &value {
            WebhookSubscriptionlabel::from_str(s).map_err(InputValueError::custom)
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
