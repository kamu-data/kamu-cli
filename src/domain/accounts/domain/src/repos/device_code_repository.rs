// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::{DeviceCode, DeviceToken, DeviceTokenCreated, DeviceTokenParamsPart};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DeviceCodeRepository: Send + Sync {
    async fn create_device_code(
        &self,
        device_code_created: &DeviceTokenCreated,
    ) -> Result<(), CreateDeviceCodeError>;

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
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CreateDeviceCodeError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum UpdateDeviceCodeWithTokenParamsPartError {
    #[error(transparent)]
    NotFound(#[from] DeviceTokenFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum FindDeviceTokenByDeviceCodeError {
    #[error(transparent)]
    NotFound(#[from] DeviceTokenFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Dataset token for device_code '{device_code}' not found")]
pub struct DeviceTokenFoundError {
    pub device_code: DeviceCode,
}

#[derive(Error, Debug)]
pub enum CleanupExpiredDeviceCodesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
