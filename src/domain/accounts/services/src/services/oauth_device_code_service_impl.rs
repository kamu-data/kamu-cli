// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Duration;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{
    CleanupExpiredDeviceCodesError,
    CreateDeviceCodeError,
    DeviceClientId,
    DeviceCode,
    DeviceToken,
    DeviceTokenCreated,
    DeviceTokenParamsPart,
    FindDeviceTokenByDeviceCodeError,
    OAuthDeviceCodeRepository,
    OAuthDeviceCodeService,
    UpdateDeviceCodeWithTokenParamsPartError,
    JOB_KAMU_ACCOUNTS_DEVICE_CODE_SERVICE,
};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DEVICE_CODE_EXPIRES_IN_5_MINUTES: Duration = Duration::minutes(5);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn OAuthDeviceCodeService)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: JOB_KAMU_ACCOUNTS_DEVICE_CODE_SERVICE,
    depends_on: &[],
    requires_transaction: true,
})]
pub struct OAuthDeviceCodeServiceImpl {
    oauth_device_code_repo: Arc<dyn OAuthDeviceCodeRepository>,
    time_source: Arc<dyn SystemTimeSource>,
}

#[async_trait::async_trait]
impl OAuthDeviceCodeService for OAuthDeviceCodeServiceImpl {
    async fn create_device_code(
        &self,
        _client_id: &DeviceClientId,
    ) -> Result<DeviceTokenCreated, CreateDeviceCodeError> {
        let device_token_created = {
            let device_code = DeviceCode::new_uuid_v4();
            let created_at = self.time_source.now();
            let expires_at = created_at + DEVICE_CODE_EXPIRES_IN_5_MINUTES;

            DeviceTokenCreated {
                device_code,
                created_at,
                expires_at,
            }
        };

        self.oauth_device_code_repo
            .save_device_code(&device_token_created)
            .await?;

        Ok(device_token_created)
    }

    async fn update_device_token_with_token_params_part(
        &self,
        device_code: &DeviceCode,
        token_params_part: &DeviceTokenParamsPart,
    ) -> Result<(), UpdateDeviceCodeWithTokenParamsPartError> {
        self.oauth_device_code_repo
            .update_device_token_with_token_params_part(device_code, token_params_part)
            .await
    }

    async fn find_device_token_by_device_code(
        &self,
        device_code: &DeviceCode,
    ) -> Result<DeviceToken, FindDeviceTokenByDeviceCodeError> {
        self.oauth_device_code_repo
            .find_device_token_by_device_code(device_code)
            .await
    }

    async fn cleanup_expired_device_codes(&self) -> Result<(), CleanupExpiredDeviceCodesError> {
        let now = self.time_source.now();

        self.oauth_device_code_repo
            .cleanup_expired_device_codes(now)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl InitOnStartup for OAuthDeviceCodeServiceImpl {
    #[tracing::instrument(level = "debug", skip_all, name = OAuthDeviceCodeServiceImpl_run_initialization)]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        self.cleanup_expired_device_codes().await.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
