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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const OAUTH_DEVICE_ACCESS_TOKEN_GRANT_TYPE: &str =
    "urn:ietf:params:oauth:grant-type:device_code";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DeviceCodeService: Sync + Send {
    fn create_device_code(&self, client_id: &DeviceClientId) -> DeviceCode;

    async fn create_device_access_token(
        &self,
        account_id: &odf::AccountID,
        device_code: &DeviceCode,
    ) -> Result<JwtAccessToken, InternalError>;

    // TODO: Device Flow: return an entity (JwtAccessToken as its field)
    async fn find_access_token_by_device_code(
        &self,
        device_code: &DeviceCode,
    ) -> Result<Option<JwtAccessToken>, InternalError>;

    async fn cleanup_expired_device_codes(&self) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype(sanitize(trim), validate(not_empty), derive(AsRef))]
pub struct DeviceClientId(String);

#[nutype(sanitize(trim), validate(not_empty), derive(AsRef, Debug, Display))]
pub struct DeviceCode(String);

// TODO: Device Flow: move to AuthenticationService scope
#[nutype(sanitize(trim), validate(not_empty), derive(AsRef))]
pub struct JwtAccessToken(String);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
