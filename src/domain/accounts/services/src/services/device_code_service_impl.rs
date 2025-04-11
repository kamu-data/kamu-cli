// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_accounts::{DeviceClientId, DeviceCode, DeviceCodeService, JwtAccessToken};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn DeviceCodeService)]
pub struct DeviceCodeServiceImpl {}

#[async_trait::async_trait]
impl DeviceCodeService for DeviceCodeServiceImpl {
    fn create_device_code(&self, _client_id: &DeviceClientId) -> DeviceCode {
        DeviceCode::try_new(Uuid::new_v4().to_string()).unwrap()
    }

    async fn create_device_access_token(
        &self,
        _account_id: &odf::AccountID,
        _device_code: &DeviceCode,
    ) -> Result<JwtAccessToken, InternalError> {
        todo!("TODO: Device Flow: implement")
    }

    async fn find_access_token_by_device_code(
        &self,
        _device_code: &DeviceCode,
    ) -> Result<Option<JwtAccessToken>, InternalError> {
        // TODO: Device Flow: implement
        Ok(None)
    }

    async fn cleanup_expired_device_codes(&self) -> Result<(), InternalError> {
        // TODO: Device Flow: implement
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
