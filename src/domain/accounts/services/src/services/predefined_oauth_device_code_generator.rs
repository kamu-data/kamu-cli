// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{DeviceClientId, DeviceCode, OAuthDeviceCodeGenerator};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const PREDEFINED_DEVICE_CODE_UUID: Uuid = Uuid::from_bytes(*b"kamu            ");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn OAuthDeviceCodeGenerator)]
pub struct PredefinedOAuthDeviceCodeGenerator;

#[async_trait::async_trait]
impl OAuthDeviceCodeGenerator for PredefinedOAuthDeviceCodeGenerator {
    fn generate_device_code(&self, _client_id: &DeviceClientId) -> DeviceCode {
        DeviceCode::new(PREDEFINED_DEVICE_CODE_UUID)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
