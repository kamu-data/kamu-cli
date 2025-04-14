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

use chrono::Utc;
use tokio::sync::RwLock;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    device_token_by_device_code: HashMap<DeviceCode, DeviceToken>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDeviceCodeRepository {
    state: Arc<RwLock<State>>,
}

#[dill::component(pub)]
#[dill::interface(dyn DeviceCodeRepository)]
#[dill::scope(dill::Singleton)]
impl InMemoryDeviceCodeRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DeviceCodeRepository for InMemoryDeviceCodeRepository {
    async fn create_device_code(
        &self,
        device_code: &DeviceCode,
    ) -> Result<(), CreateDeviceCodeError> {
        let mut writable_state = self.state.write().await;

        let device_token = DeviceToken::DeviceCodeCreated {
            device_code: device_code.clone(),
            created_at: Utc::now(),
        };

        writable_state
            .device_token_by_device_code
            .insert(device_code.clone(), device_token);

        Ok(())
    }

    async fn update_device_code_with_token_params_part(
        &self,
        device_code: &DeviceCode,
        token_params_part: &DeviceTokenParamsPart,
    ) -> Result<(), UpdateDeviceCodeWithTokenParamsPartError> {
        let mut writable_state = self.state.write().await;

        let created_token = writable_state
            .device_token_by_device_code
            .remove(device_code)
            .expect("Device code must exist");
        let (created_device_code, created_at) = created_token.into_parts();
        let device_token = DeviceToken::DeviceCodeWithIssuedToken {
            device_code: created_device_code,
            created_at,
            token_params_part: token_params_part.clone(),
            token_last_used_at: None,
        };

        writable_state
            .device_token_by_device_code
            .insert(device_code.clone(), device_token);

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
            Err(DeviceTokenFoundError {
                device_code: device_code.clone(),
            }
            .into())
        }
    }

    async fn cleanup_expired_device_codes(&self) -> Result<(), CleanupExpiredDeviceCodesError> {
        // A server restart is already a cleanup
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
