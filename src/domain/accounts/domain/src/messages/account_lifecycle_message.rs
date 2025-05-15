// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use email_utils::Email;
use messaging_outbox::Message;
use serde::{Deserialize, Serialize};

use crate::AccountDisplayName;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ACCOUNT_LIFECYCLE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the lifecycle of an account
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountLifecycleMessage {
    /// Message indicating that an account has been created
    Created(AccountLifecycleMessageCreated),

    /// Message indicating that an account has been deleted
    Deleted(AccountLifecycleMessageDeleted),
}

impl AccountLifecycleMessage {
    pub fn created(
        account_id: odf::AccountID,
        email: Email,
        display_name: AccountDisplayName,
    ) -> Self {
        Self::Created(AccountLifecycleMessageCreated {
            account_id,
            email,
            display_name,
        })
    }

    pub fn deleted(account_id: odf::AccountID, display_name: AccountDisplayName) -> Self {
        Self::Deleted(AccountLifecycleMessageDeleted {
            account_id,
            display_name,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Message for AccountLifecycleMessage {
    fn version() -> u32 {
        ACCOUNT_LIFECYCLE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a newly created account
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountLifecycleMessageCreated {
    /// The unique identifier of the account
    pub account_id: odf::AccountID,

    /// The email address associated with the account
    pub email: Email,

    /// The display name of the account
    pub display_name: AccountDisplayName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountLifecycleMessageDeleted {
    /// The unique identifier of the account
    pub account_id: odf::AccountID,

    /// The display name of the account
    pub display_name: AccountDisplayName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
