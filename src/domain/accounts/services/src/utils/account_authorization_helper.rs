// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError};
use kamu_accounts::{CurrentAccountSubject, DeleteAccountByNameError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// NOTE: If you need to extend this utility,
//       please consider implementing it through OSO instead
//       (similar to OsoDatasetAuthorizer).

#[dill::component(pub)]
pub struct AccountAuthorizationHelper {
    current_account_subject: Arc<CurrentAccountSubject>,
    rebac_service: Arc<dyn kamu_auth_rebac::RebacService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl AccountAuthorizationHelper {
    pub async fn can_delete_account(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<bool, InternalError> {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => Ok(false),
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
        }
    }

    pub async fn ensure_account_can_be_deleted(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), EnsureAccountCanBeDeletedError> {
        if !self.can_delete_account(account_name).await? {
            return Err(EnsureAccountCanBeDeletedError::Access(
                odf::AccessError::Unauthenticated(
                    AccountDeletionNotAuthorizedError {
                        subject_account: self.current_account_subject.maybe_account_name().cloned(),
                        object_account: account_name.clone(),
                    }
                    .into(),
                ),
            ));
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum EnsureAccountCanBeDeletedError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub struct AccountDeletionNotAuthorizedError {
    pub subject_account: Option<odf::AccountName>,
    pub object_account: odf::AccountName,
}

impl std::fmt::Display for AccountDeletionNotAuthorizedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let object_account = &self.object_account;

        if let Some(subject_account) = &self.subject_account {
            write!(
                f,
                "Account '{subject_account}' is not authorized to delete account \
                 '{object_account}'"
            )?;
        } else {
            write!(
                f,
                "Anonymous is not authorized to delete account '{object_account}'"
            )?;
        }

        Ok(())
    }
}

impl From<EnsureAccountCanBeDeletedError> for DeleteAccountByNameError {
    fn from(e: EnsureAccountCanBeDeletedError) -> Self {
        use EnsureAccountCanBeDeletedError as E;

        match e {
            E::Access(e) => Self::Access(e),
            e @ E::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
