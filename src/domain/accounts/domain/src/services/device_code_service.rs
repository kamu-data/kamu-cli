// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use nutype::nutype;
use uuid::Uuid;

use crate::{
    CleanupExpiredDeviceCodesError,
    CreateDeviceCodeError,
    DeviceToken,
    DeviceTokenCreated,
    DeviceTokenParamsPart,
    FindDeviceTokenByDeviceCodeError,
    UpdateDeviceCodeWithTokenParamsPartError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const OAUTH_DEVICE_ACCESS_TOKEN_GRANT_TYPE: &str =
    "urn:ietf:params:oauth:grant-type:device_code";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DeviceCodeService: Sync + Send {
    async fn create_device_code(
        &self,
        client_id: &DeviceClientId,
    ) -> Result<DeviceTokenCreated, CreateDeviceCodeError>;

    async fn update_device_token_with_token_params_part(
        &self,
        device_code: &DeviceCode,
        token_params_part: &DeviceTokenParamsPart,
    ) -> Result<(), UpdateDeviceCodeWithTokenParamsPartError>;

    async fn find_device_token_by_device_code(
        &self,
        device_code: &DeviceCode,
    ) -> Result<DeviceToken, FindDeviceTokenByDeviceCodeError>;

    async fn cleanup_expired_device_codes(&self) -> Result<(), CleanupExpiredDeviceCodesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DeviceCodeServiceExt: DeviceCodeService {
    async fn try_find_device_token_by_device_code(
        &self,
        device_code: &DeviceCode,
    ) -> Result<Option<DeviceToken>, InternalError>;
}

#[async_trait::async_trait]
impl<T> DeviceCodeServiceExt for T
where
    T: DeviceCodeService,
    T: ?Sized,
{
    async fn try_find_device_token_by_device_code(
        &self,
        device_code: &DeviceCode,
    ) -> Result<Option<DeviceToken>, InternalError> {
        use FindDeviceTokenByDeviceCodeError as E;

        match self.find_device_token_by_device_code(device_code).await {
            Ok(device_token) => Ok(Some(device_token)),
            Err(E::NotFound(_)) => Ok(None),
            Err(E::Internal(e)) => Err(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype(sanitize(trim), validate(not_empty), derive(AsRef))]
pub struct DeviceClientId(String);

#[nutype(derive(AsRef, Debug, Display, Clone, Hash, Eq, PartialEq))]
pub struct DeviceCode(Uuid);

impl DeviceCode {
    pub fn try_new(raw: &str) -> Result<Self, InternalError> {
        let uuid = Uuid::parse_str(raw).int_err()?;
        Ok(Self::new(uuid))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
