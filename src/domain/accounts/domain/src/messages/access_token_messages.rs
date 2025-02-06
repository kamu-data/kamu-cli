// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use messaging_outbox::Message;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MESSAGE_PRODUCER_KAMU_ACCESS_TOKEN_SERVICE: &str =
    "dev.kamu.domain.accounts.AccessTokenService";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ACCESS_TOKEN_LIFECYCLE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessTokenLifecycleMessage {
    Created(AccessTokenLifecycleMessageCreated),
}

impl AccessTokenLifecycleMessage {
    pub fn created(composed_token: String) -> Self {
        Self::Created(AccessTokenLifecycleMessageCreated { composed_token })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Message for AccessTokenLifecycleMessage {
    fn version() -> u32 {
        ACCESS_TOKEN_LIFECYCLE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessTokenLifecycleMessageCreated {
    pub composed_token: String,
}
