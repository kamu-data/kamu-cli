// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_accounts::{
    Account,
    AccountLifecycleMessage,
    AccountProvider,
    AccountService,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    UpdateAccountError,
};
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Updates an account with no authorization: the use case checks the
/// principal first, the predefined accounts registrator runs as the system
#[derive(Clone)]
pub(crate) struct UpdateAccountUseCaseHelper {
    account_service: Arc<dyn AccountService>,
    outbox: Arc<dyn Outbox>,
    time_source: Arc<dyn SystemTimeSource>,
}

impl UpdateAccountUseCaseHelper {
    pub(crate) fn new(
        account_service: Arc<dyn AccountService>,
        outbox: Arc<dyn Outbox>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            account_service,
            outbox,
            time_source,
        }
    }

    /// Stores `account` over `original_account` and notifies about the change
    /// when a name, display name or email differs
    pub(crate) async fn update(
        &self,
        account: &Account,
        original_account: &Account,
    ) -> Result<(), UpdateAccountError> {
        let mut account_to_update = account.clone();
        if original_account.account_name != account_to_update.account_name
            && AccountProvider::is_password(&account.provider)
        {
            account_to_update.provider_identity_key = account_to_update.account_name.to_string();
        }

        self.account_service
            .update_account(&account_to_update)
            .await?;

        self.notify_account_updated(&account_to_update, original_account)
            .await?;

        Ok(())
    }

    async fn notify_account_updated(
        &self,
        updated_account: &Account,
        original_account: &Account,
    ) -> Result<(), InternalError> {
        if original_account.account_name != updated_account.account_name
            || original_account.display_name != updated_account.display_name
            || original_account.email != updated_account.email
        {
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
                    AccountLifecycleMessage::updated(
                        self.time_source.now(),
                        updated_account.id.clone(),
                        original_account.email.clone(),
                        updated_account.email.clone(),
                        original_account.account_name.clone(),
                        updated_account.account_name.clone(),
                        original_account.display_name.clone(),
                        updated_account.display_name.clone(),
                    ),
                )
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
