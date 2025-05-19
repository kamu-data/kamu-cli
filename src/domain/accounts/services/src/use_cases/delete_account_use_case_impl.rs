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

impl DeleteAccountUseCaseImpl {
    async fn unauthenticated(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<bool, InternalError> {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(l) if l.account_name == *account_name => Ok(true),
            CurrentAccountSubject::Logged(l) => {
                use internal_error::ResultIntoInternal;
                use kamu_auth_rebac::RebacServiceExt;

                let is_admin = self
                    .rebac_service
                    .is_account_admin(&l.account_id)
                    .await
                    .int_err()?;

                Ok(is_admin)
            }
            CurrentAccountSubject::Anonymous(_) => Ok(false),
        }
    }
}

#[async_trait::async_trait]
impl DeleteAccountUseCase for DeleteAccountUseCaseImpl {
    async fn execute(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), DeleteAccountByNameError> {
        if !self.unauthenticated(account_name).await? {
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
