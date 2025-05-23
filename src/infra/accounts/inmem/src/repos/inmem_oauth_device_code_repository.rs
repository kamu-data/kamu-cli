// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    device_token_by_device_code: HashMap<DeviceCode, DeviceToken>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryOAuthDeviceCodeRepository {
    state: Arc<RwLock<State>>,
}

#[dill::component(pub)]
#[dill::interface(dyn OAuthDeviceCodeRepository)]
#[dill::scope(dill::Singleton)]
impl InMemoryOAuthDeviceCodeRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl OAuthDeviceCodeRepository for InMemoryOAuthDeviceCodeRepository {
    async fn save_device_code(
        &self,
        device_code_created: &DeviceTokenCreated,
    ) -> Result<(), CreateDeviceCodeError> {
        let mut writable_state = self.state.write().await;

        if writable_state
            .device_token_by_device_code
            .contains_key(&device_code_created.device_code)
        {
            return Err(DeviceCodeDuplicateError {
                device_code: device_code_created.device_code.clone(),
            }
            .into());
        }

        let device_token = DeviceToken::from(device_code_created.clone());

        writable_state
            .device_token_by_device_code
            .insert(device_code_created.device_code.clone(), device_token);

        Ok(())
    }

    async fn update_device_token_with_token_params_part(
        &self,
        device_code: &DeviceCode,
        token_params_part: &DeviceTokenParamsPart,
    ) -> Result<(), UpdateDeviceCodeWithTokenParamsPartError> {
        let mut writable_state = self.state.write().await;

        let maybe_created_token = writable_state
            .device_token_by_device_code
            .remove(device_code);

        let Some(created_token) = maybe_created_token else {
            return Err(DeviceTokenNotFoundError::new(device_code.clone()).into());
        };

        let token = created_token.with_token_params_part(token_params_part.clone());

        writable_state
            .device_token_by_device_code
            .insert(device_code.clone(), token);

        Ok(())
    }

    async fn find_device_token_by_device_code(
        &self,
        device_code: &DeviceCode,
    ) -> Result<DeviceToken, FindDeviceTokenByDeviceCodeError> {
        let readable_state = self.state.read().await;

        let maybe_device_token = readable_state.device_token_by_device_code.get(device_code);

        if let Some(device_token) = maybe_device_token {
            Ok(device_token.clone())
        } else {
            Err(DeviceTokenNotFoundError::new(device_code.clone()).into())
        }
    }

    async fn cleanup_expired_device_codes(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), CleanupExpiredDeviceCodesError> {
        let mut writable_state = self.state.write().await;

        writable_state
            .device_token_by_device_code
            .retain(|_, device_token| device_token.device_code_expire_at() >= now);

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
