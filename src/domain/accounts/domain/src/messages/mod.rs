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

pub const MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE: &str = "dev.kamu.domain.accounts.AccountsService";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ACCOUNT_LIFECYCLE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountLifecycleMessage {
    Created(AccountLifecycleMessageCreated),
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Message for AccountLifecycleMessage {
    fn version() -> u32 {
        ACCOUNT_LIFECYCLE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountLifecycleMessageCreated {
    pub account_id: odf::AccountID,
    pub email: Email,
    pub display_name: AccountDisplayName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
