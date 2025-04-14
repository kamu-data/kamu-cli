// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::DeviceCode;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum DeviceToken {
    DeviceCodeCreated(DeviceTokenCreated),
    DeviceCodeWithIssuedToken(DeviceCodeWithIssuedToken),
}

impl DeviceToken {
    pub fn with_token_params_part(self, token_params_part: DeviceTokenParamsPart) -> DeviceToken {
        match self {
            DeviceToken::DeviceCodeCreated(base) => {
                DeviceToken::DeviceCodeWithIssuedToken(DeviceCodeWithIssuedToken {
                    base,
                    token_params_part,
                    token_last_used_at: None,
                })
            }
            _ => panic!("Cannot add token params part to a token that already has one"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DeviceTokenCreated {
    pub device_code: DeviceCode,
    pub created_at: DateTime<Utc>,
    pub device_code_expires_in_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct DeviceCodeWithIssuedToken {
    pub base: DeviceTokenCreated,
    pub token_params_part: DeviceTokenParamsPart,
    pub token_last_used_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DeviceTokenParamsPart {
    pub iat: usize,
    pub exp: usize,
    pub sub: String,
}

impl DeviceTokenParamsPart {
    pub fn expires_in(&self) -> usize {
        self.exp - self.iat
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
