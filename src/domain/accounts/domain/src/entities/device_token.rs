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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeviceToken {
    DeviceCodeCreated(DeviceTokenCreated),
    DeviceCodeWithIssuedToken(DeviceCodeWithIssuedToken),
}

impl DeviceToken {
    pub fn with_token_params_part(self, token_params_part: DeviceTokenParamsPart) -> DeviceToken {
        match self {
            DeviceToken::DeviceCodeCreated(d) => {
                DeviceToken::DeviceCodeWithIssuedToken(DeviceCodeWithIssuedToken {
                    device_code: d.device_code,
                    created_at: d.created_at,
                    expires_at: d.expires_at,
                    token_params_part,
                    token_last_used_at: None,
                })
            }
            _ => panic!("Cannot add token params part to a token that already has one"),
        }
    }

    pub fn device_code(&self) -> &DeviceCode {
        match self {
            DeviceToken::DeviceCodeCreated(d) => &d.device_code,
            DeviceToken::DeviceCodeWithIssuedToken(d) => &d.device_code,
        }
    }

    pub fn device_code_expire_at(&self) -> DateTime<Utc> {
        match self {
            DeviceToken::DeviceCodeCreated(d) => d.expires_at,
            DeviceToken::DeviceCodeWithIssuedToken(d) => d.expires_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[reusable::reusable(device_token)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceTokenCreated {
    pub device_code: DeviceCode,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl From<DeviceTokenCreated> for DeviceToken {
    fn from(v: DeviceTokenCreated) -> Self {
        DeviceToken::DeviceCodeCreated(v)
    }
}

#[reusable::reuse(device_token)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceCodeWithIssuedToken {
    pub token_params_part: DeviceTokenParamsPart,
    pub token_last_used_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceTokenParamsPart {
    pub iat: usize,
    pub exp: usize,
    pub account_id: odf::AccountID,
}

impl DeviceTokenParamsPart {
    pub fn expires_in(&self) -> usize {
        self.exp - self.iat
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct DeviceTokenRowModel {
    pub device_code: uuid::Uuid,
    pub device_code_created_at: DateTime<Utc>,
    pub device_code_expires_at: DateTime<Utc>,
    pub token_iat: Option<i64>,
    pub token_exp: Option<i64>,
    pub token_last_used_at: Option<DateTime<Utc>>,
    pub account_id: Option<String>,
}

#[cfg(feature = "sqlx")]
impl TryFrom<DeviceTokenRowModel> for DeviceToken {
    type Error = internal_error::InternalError;

    fn try_from(v: DeviceTokenRowModel) -> Result<Self, Self::Error> {
        use internal_error::ResultIntoInternal;

        let res = match (v.token_iat, v.token_exp, v.account_id) {
            (Some(token_iat), Some(token_exp), Some(account_id)) => {
                DeviceToken::DeviceCodeWithIssuedToken(DeviceCodeWithIssuedToken {
                    device_code: DeviceCode::new(v.device_code),
                    created_at: v.device_code_created_at,
                    expires_at: v.device_code_expires_at,
                    token_params_part: DeviceTokenParamsPart {
                        iat: token_iat.try_into().int_err()?,
                        exp: token_exp.try_into().int_err()?,
                        account_id: odf::AccountID::from_did_str(&account_id).int_err()?,
                    },
                    token_last_used_at: None,
                })
            }
            (_, _, _) => DeviceToken::DeviceCodeCreated(DeviceTokenCreated {
                device_code: DeviceCode::new(v.device_code),
                created_at: v.device_code_created_at,
                expires_at: v.device_code_expires_at,
            }),
        };
        Ok(res)
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
