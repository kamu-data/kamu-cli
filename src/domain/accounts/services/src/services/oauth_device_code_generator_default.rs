// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{DeviceClientId, DeviceCode, OAuthDeviceCodeGenerator};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn OAuthDeviceCodeGenerator)]
pub struct OAuthDeviceCodeGeneratorDefault;

#[async_trait::async_trait]
impl OAuthDeviceCodeGenerator for OAuthDeviceCodeGeneratorDefault {
    fn generate_device_code(&self, _client_id: &DeviceClientId) -> DeviceCode {
        DeviceCode::new_uuid_v4()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
