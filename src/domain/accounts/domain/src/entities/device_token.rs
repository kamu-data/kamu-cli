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
    DeviceCodeCreated {
        device_code: DeviceCode,
        created_at: DateTime<Utc>,
    },
    DeviceCodeWithIssuedToken {
        device_code: DeviceCode,
        created_at: DateTime<Utc>,
        token_params_part: DeviceTokenParamsPart,
        token_last_used_at: Option<DateTime<Utc>>,
    },
}

impl DeviceToken {
    pub fn into_parts(self) -> (DeviceCode, DateTime<Utc>) {
        match self {
            DeviceToken::DeviceCodeWithIssuedToken {
                device_code,
                created_at,
                ..
            }
            | DeviceToken::DeviceCodeCreated {
                device_code,
                created_at,
            } => (device_code, created_at),
        }
    }
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
