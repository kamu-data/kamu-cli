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

const ACCESS_TOKEN_LIFECYCLE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the lifecycle of an access token
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessTokenLifecycleMessage {
    /// Message indicating that an access token has been created
    Created(AccessTokenLifecycleMessageCreated),
}

impl AccessTokenLifecycleMessage {
    pub fn created(token_name: String, owner_id: odf::AccountID) -> Self {
        Self::Created(AccessTokenLifecycleMessageCreated {
            token_name,
            owner_id,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Message for AccessTokenLifecycleMessage {
    fn version() -> u32 {
        ACCESS_TOKEN_LIFECYCLE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a newly created access token
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessTokenLifecycleMessageCreated {
    /// The name of the access token
    pub token_name: String,

    /// The unique identifier of the account that owns the token
    pub owner_id: odf::AccountID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
