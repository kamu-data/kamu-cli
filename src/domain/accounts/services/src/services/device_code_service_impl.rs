// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::{
    CleanupExpiredDeviceCodesError,
    CreateDeviceCodeError,
    DeviceClientId,
    DeviceCode,
    DeviceCodeRepository,
    DeviceCodeService,
    DeviceToken,
    DeviceTokenCreated,
    DeviceTokenParamsPart,
    FindDeviceTokenByDeviceCodeError,
    UpdateDeviceCodeWithTokenParamsPartError,
};
use time_source::SystemTimeSource;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DEVICE_CODE_EXPIRES_IN_SECONDS: u64 = 600; /* 5 minutes */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn DeviceCodeService)]
pub struct DeviceCodeServiceImpl {
    device_code_repo: Arc<dyn DeviceCodeRepository>,
    time_source: Arc<dyn SystemTimeSource>,
}

#[async_trait::async_trait]
impl DeviceCodeService for DeviceCodeServiceImpl {
    async fn create_device_code(
        &self,
        _client_id: &DeviceClientId,
    ) -> Result<DeviceTokenCreated, CreateDeviceCodeError> {
        let device_code = DeviceCode::try_new(Uuid::new_v4().to_string()).unwrap();
        let device_token_created = DeviceTokenCreated {
            device_code,
            created_at: self.time_source.now(),
            device_code_expires_in_seconds: DEVICE_CODE_EXPIRES_IN_SECONDS,
        };

        self.device_code_repo
            .create_device_code(&device_token_created)
            .await?;

        Ok(device_token_created)
    }

    async fn update_device_code_with_token_params_part(
        &self,
        device_code: &DeviceCode,
        token_params_part: &DeviceTokenParamsPart,
    ) -> Result<(), UpdateDeviceCodeWithTokenParamsPartError> {
        self.device_code_repo
            .update_device_code_with_token_params_part(device_code, token_params_part)
            .await
    }

    async fn find_device_token_by_device_code(
        &self,
        device_code: &DeviceCode,
    ) -> Result<DeviceToken, FindDeviceTokenByDeviceCodeError> {
        self.device_code_repo
            .find_device_token_by_device_code(device_code)
            .await
    }

    async fn cleanup_expired_device_codes(&self) -> Result<(), CleanupExpiredDeviceCodesError> {
        self.device_code_repo.cleanup_expired_device_codes().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
