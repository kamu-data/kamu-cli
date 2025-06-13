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
use kamu_accounts::{
    CurrentAccountSubject,
    CurrentAccountSubjectExt,
    DeleteAccountError,
    LoggedAccountExt,
    ModifyAccountPasswordError,
    RenameAccountError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
#[async_trait::async_trait]
pub trait AccountAuthorizationHelper: Send + Sync {
    async fn is_admin(&self) -> Result<bool, InternalError>;

    async fn can_modify_account(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<bool, InternalError>;

    async fn ensure_account_password_can_be_modified(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), EnsureAccountPasswordCanBeModifiedError>;

    async fn ensure_account_can_be_deleted(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), EnsureAccountCanBeDeletedError>;

    async fn ensure_account_can_be_renamed(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), EnsureAccountCanBeRenamedError>;
}

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
impl MockAccountAuthorizationHelper {
    pub fn allowing() -> Self {
        let mut mock = Self::new();

        mock.expect_is_admin().returning(|| Ok(true));
        mock.expect_can_modify_account().returning(|_| Ok(true));
        mock.expect_ensure_account_password_can_be_modified()
            .returning(|_| Ok(()));
        mock.expect_ensure_account_can_be_deleted()
            .returning(|_| Ok(()));
        mock.expect_ensure_account_can_be_renamed()
            .returning(|_| Ok(()));

        mock
    }

    pub fn disallowing() -> Self {
        let subject_account = odf::AccountName::new_unchecked("not-admin");

        let mut mock = Self::new();

        mock.expect_is_admin().returning(|| Ok(false));
        mock.expect_can_modify_account().returning(|_| Ok(false));

        let subject_account_clone = subject_account.clone();
        mock.expect_ensure_account_password_can_be_modified()
            .returning(move |account_name| {
                Err(EnsureAccountPasswordCanBeModifiedError::Access(
                    odf::AccessError::Unauthenticated(
                        NotAuthorizedToModifyAccountPasswordError {
                            subject_account: Some(subject_account_clone.clone()),
                            object_account: account_name.clone(),
                        }
                        .into(),
                    ),
                ))
            });

        let subject_account_clone = subject_account.clone();
        mock.expect_ensure_account_can_be_deleted()
            .returning(move |account_name| {
                Err(EnsureAccountCanBeDeletedError::Access(
                    odf::AccessError::Unauthenticated(
                        AccountDeletionNotAuthorizedError {
                            subject_account: Some(subject_account_clone.clone()),
                            object_account: account_name.clone(),
                        }
                        .into(),
                    ),
                ))
            });

        mock.expect_ensure_account_can_be_renamed()
            .returning(move |account_name| {
                Err(EnsureAccountCanBeRenamedError::Access(
                    odf::AccessError::Unauthenticated(
                        AccountRenameNotAuthorizedError {
                            subject_account: Some(subject_account.clone()),
                            object_account: account_name.clone(),
                        }
                        .into(),
                    ),
                ))
            });

        mock
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// NOTE: If you need to extend this utility,
//       please consider implementing it through OSO instead
//       (similar to OsoDatasetAuthorizer).

#[dill::component(pub)]
#[dill::interface(dyn AccountAuthorizationHelper)]
pub struct AccountAuthorizationHelperImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    rebac_service: Arc<dyn kamu_auth_rebac::RebacService>,
}

#[async_trait::async_trait]
impl AccountAuthorizationHelper for AccountAuthorizationHelperImpl {
    async fn is_admin(&self) -> Result<bool, InternalError> {
        self.current_account_subject
            .is_admin(self.rebac_service.as_ref())
            .await
    }

    async fn can_modify_account(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<bool, InternalError> {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => Ok(false),
            CurrentAccountSubject::Logged(l) if l.account_name == *account_name => Ok(true),
            CurrentAccountSubject::Logged(l) => l.is_admin(self.rebac_service.as_ref()).await,
        }
    }

    async fn ensure_account_password_can_be_modified(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), EnsureAccountPasswordCanBeModifiedError> {
        if !self.is_admin().await? {
            return Err(EnsureAccountPasswordCanBeModifiedError::Access(
                odf::AccessError::Unauthenticated(
                    NotAuthorizedToModifyAccountPasswordError {
                        subject_account: self.current_account_subject.maybe_account_name().cloned(),
                        object_account: account_name.clone(),
                    }
                    .into(),
                ),
            ));
        }

        Ok(())
    }

    async fn ensure_account_can_be_deleted(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), EnsureAccountCanBeDeletedError> {
        if !self.can_modify_account(account_name).await? {
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

    async fn ensure_account_can_be_renamed(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), EnsureAccountCanBeRenamedError> {
        if !self.can_modify_account(account_name).await? {
            return Err(EnsureAccountCanBeRenamedError::Access(
                odf::AccessError::Unauthenticated(
                    AccountRenameNotAuthorizedError {
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
pub enum EnsureAccountPasswordCanBeModifiedError {
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
pub struct NotAuthorizedToModifyAccountPasswordError {
    pub subject_account: Option<odf::AccountName>,
    pub object_account: odf::AccountName,
}

impl std::fmt::Display for NotAuthorizedToModifyAccountPasswordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let object_account = &self.object_account;

        if let Some(subject_account) = &self.subject_account {
            write!(
                f,
                "Account '{subject_account}' is not authorized to modify account's password \
                 '{object_account}'"
            )?;
        } else {
            write!(
                f,
                "Anonymous is not authorized to delete modify account's password \
                 '{object_account}'"
            )?;
        }

        Ok(())
    }
}

impl From<EnsureAccountPasswordCanBeModifiedError> for ModifyAccountPasswordError {
    fn from(e: EnsureAccountPasswordCanBeModifiedError) -> Self {
        use EnsureAccountPasswordCanBeModifiedError as E;

        match e {
            E::Access(e) => Self::Access(e),
            e @ E::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

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

impl From<EnsureAccountCanBeDeletedError> for DeleteAccountError {
    fn from(e: EnsureAccountCanBeDeletedError) -> Self {
        use EnsureAccountCanBeDeletedError as E;

        match e {
            E::Access(e) => Self::Access(e),
            e @ E::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum EnsureAccountCanBeRenamedError {
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
pub struct AccountRenameNotAuthorizedError {
    pub subject_account: Option<odf::AccountName>,
    pub object_account: odf::AccountName,
}

impl std::fmt::Display for AccountRenameNotAuthorizedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let object_account = &self.object_account;

        if let Some(subject_account) = &self.subject_account {
            write!(
                f,
                "Account '{subject_account}' is not authorized to rename account \
                 '{object_account}'"
            )?;
        } else {
            write!(
                f,
                "Anonymous is not authorized to rename account '{object_account}'"
            )?;
        }

        Ok(())
    }
}

impl From<EnsureAccountCanBeRenamedError> for RenameAccountError {
    fn from(e: EnsureAccountCanBeRenamedError) -> Self {
        use EnsureAccountCanBeRenamedError as E;

        match e {
            E::Access(e) => Self::Access(e),
            e @ E::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
