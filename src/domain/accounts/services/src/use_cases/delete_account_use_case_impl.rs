// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn DeleteAccountUseCase)]
pub struct DeleteAccountUseCaseImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    rebac_service: Arc<dyn kamu_auth_rebac::RebacService>,
    account_service: Arc<dyn AccountService>,
    outbox: Arc<dyn messaging_outbox::Outbox>,
}

#[async_trait::async_trait]
impl DeleteAccountUseCase for DeleteAccountUseCaseImpl {
    async fn execute(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), DeleteAccountByNameError> {
        if self.current_account_subject.account_name() == account_name {
            return Err(DeleteAccountByNameError::SelfDeletion);
        }

        let is_admin = {
            use internal_error::ResultIntoInternal;
            use kamu_auth_rebac::RebacServiceExt;

            self.rebac_service
                .is_account_admin(self.current_account_subject.account_id())
                .await
                .int_err()?
        };

        if !is_admin {
            return Err(DeleteAccountByNameError::Access(
                odf::AccessError::Unauthenticated(
                    AccountDeletionNotAuthorizedError {
                        subject_account: self.current_account_subject.account_name().clone(),
                        object_account: account_name.clone(),
                    }
                    .into(),
                ),
            ));
        }

        let deleted_account = self
            .account_service
            .delete_account_by_name(account_name)
            .await?;

        use messaging_outbox::OutboxExt;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
                AccountLifecycleMessage::deleted(
                    deleted_account.id,
                    deleted_account.email,
                    deleted_account.display_name,
                ),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
