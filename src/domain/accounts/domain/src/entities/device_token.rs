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

pub enum DeviceToken {
    DeviceCodeCreated(DeviceCodeCreated),
    DeviceCodeWithIssuedToken(DeviceCodeWithIssuedToken),
    DeviceCodeWithUsedIssuedToken(DeviceCodeWithUsedIssuedToken),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DeviceCodeCreated {
    pub device_code: CreatedDeviceCode,
}

pub struct DeviceCodeWithIssuedToken {
    pub device_code: DeviceCodeCreated,
    pub token_params_part: TokenParamsPart,
}

pub struct DeviceCodeWithUsedIssuedToken {
    pub device_code: CreatedDeviceCode,
    pub token_params_part: TokenParamsPart,
    pub token_last_used_at: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CreatedDeviceCode {
    pub device_code: DeviceCode,
    pub created_at: DateTime<Utc>,
}

pub struct TokenParamsPart {
    pub iat: u64,
    pub exp: u64,
    pub sub: odf::AccountID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
