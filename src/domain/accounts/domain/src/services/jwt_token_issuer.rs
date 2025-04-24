// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use nutype::nutype;

use crate::DeviceTokenParamsPart;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait JwtTokenIssuer: Sync + Send {
    fn make_access_token_from_account_id(
        &self,
        account_id: &odf::AccountID,
        expiration_time_sec: usize,
    ) -> Result<JwtAccessToken, InternalError>;

    fn make_access_token_from_device_token_params_part(
        &self,
        device_token_params_part: DeviceTokenParamsPart,
    ) -> Result<JwtAccessToken, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype(sanitize(trim), validate(not_empty), derive(AsRef, Display))]
pub struct JwtAccessToken(String);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
