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

use crate::{DeviceCode, DeviceCodeCreated, DeviceCodeWithIssuedToken, DeviceToken};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DeviceCodeRepository: Send + Sync {
    async fn create_device_code(
        &self,
        device_code_created: &DeviceCodeCreated,
    ) -> Result<(), CreateDeviceTokenError>;

    async fn update_device_code_with_issued_token(
        &self,
        device_code_with_issued_token: &DeviceCodeWithIssuedToken,
    ) -> Result<(), UpdateDeviceCodeWithIssuedTokenError>;

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
pub enum CreateDeviceTokenError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum UpdateDeviceCodeWithIssuedTokenError {
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
    device_code: DeviceCode,
}

#[derive(Error, Debug)]
pub enum CleanupExpiredDeviceCodesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
