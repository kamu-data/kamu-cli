// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use email_utils::Email;
use messaging_outbox::Message;
use serde::{Deserialize, Serialize};

use crate::AccountDisplayName;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ACCOUNT_LIFECYCLE_OUTBOX_VERSION: u32 = 2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the lifecycle of an account
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountLifecycleMessage {
    /// Message indicating that an account has been created
    Created(AccountLifecycleMessageCreated),

    /// Message indicating that an account has been updated
    Updated(AccountLifecycleMessageUpdated),

    /// Message indicating that the account password has been changed.
    /// Only for the password account provider.
    PasswordChanged(AccountLifecycleMessagePasswordChanged),

    /// Message indicating that an account has been deleted
    Deleted(AccountLifecycleMessageDeleted),
}

impl AccountLifecycleMessage {
    pub fn created(
        event_time: DateTime<Utc>,
        account_id: odf::AccountID,
        email: Email,
        account_name: odf::AccountName,
        display_name: AccountDisplayName,
    ) -> Self {
        Self::Created(AccountLifecycleMessageCreated {
            event_time,
            account_id,
            email,
            account_name,
            display_name,
        })
    }

    pub fn updated(
        event_time: DateTime<Utc>,
        account_id: odf::AccountID,
        old_email: Email,
        new_email: Email,
        old_account_name: odf::AccountName,
        new_account_name: odf::AccountName,
        old_display_name: AccountDisplayName,
        new_display_name: AccountDisplayName,
    ) -> Self {
        Self::Updated(AccountLifecycleMessageUpdated {
            event_time,
            account_id,
            old_email,
            new_email,
            old_account_name,
            new_account_name,
            old_display_name,
            new_display_name,
        })
    }

    pub fn password_changed(
        event_time: DateTime<Utc>,
        account_id: odf::AccountID,
        email: Email,
        display_name: AccountDisplayName,
    ) -> Self {
        Self::PasswordChanged(AccountLifecycleMessagePasswordChanged {
            event_time,
            account_id,
            email,
            display_name,
        })
    }

    pub fn deleted(
        event_time: DateTime<Utc>,
        account_id: odf::AccountID,
        email: Email,
        display_name: AccountDisplayName,
    ) -> Self {
        Self::Deleted(AccountLifecycleMessageDeleted {
            event_time,
            account_id,
            email,
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
    /// Event timestamp
    pub event_time: DateTime<Utc>,

    /// The unique identifier of the account
    pub account_id: odf::AccountID,

    /// The email address associated with the account
    pub email: Email,

    /// The name of the account
    pub account_name: odf::AccountName,

    /// The display name of the account
    pub display_name: AccountDisplayName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountLifecycleMessageUpdated {
    /// Event timestamp
    pub event_time: DateTime<Utc>,

    /// The unique identifier of the account
    pub account_id: odf::AccountID,

    /// The email address associated with the account (before, after)
    pub old_email: Email,
    pub new_email: Email,

    /// The name of the account (before, after)
    pub old_account_name: odf::AccountName,
    pub new_account_name: odf::AccountName,

    /// The display name of the account (before, after)
    pub old_display_name: AccountDisplayName,
    pub new_display_name: AccountDisplayName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountLifecycleMessagePasswordChanged {
    /// Event timestamp
    pub event_time: DateTime<Utc>,

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
    /// Event timestamp
    pub event_time: DateTime<Utc>,

    /// The unique identifier of the account
    pub account_id: odf::AccountID,

    /// The email address associated with the account
    pub email: Email,

    /// The display name of the account
    pub display_name: AccountDisplayName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
