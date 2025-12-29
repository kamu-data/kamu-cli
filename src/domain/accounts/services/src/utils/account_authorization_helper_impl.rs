// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::{
    CurrentAccountSubject,
    DeleteAccountError,
    ModifyAccountPasswordError,
    ModifyAccountPasswordWithConfirmationError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// NOTE: If you need to extend this utility,
//       please consider implementing it through OSO instead
//       (similar to OsoDatasetAuthorizer).

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
    ) -> Result<(), EnsureNotAuthorizedError>;

    async fn ensure_account_password_with_confirmation_can_be_modified(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), EnsureNotAuthorizedError>;

    async fn ensure_account_can_be_deleted(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), EnsureNotAuthorizedError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(any(feature = "testing", test))]
#[derive(Default)]
pub enum AccountAuthorizationHelperTestProvider {
    #[default]
    Default,

    Mock(MockAccountAuthorizationHelper),
}

#[cfg(any(feature = "testing", test))]
impl AccountAuthorizationHelperTestProvider {
    pub fn embed_into_catalog(self, target_catalog_builder: &mut dill::CatalogBuilder) {
        match self {
            AccountAuthorizationHelperTestProvider::Default => {
                target_catalog_builder.add::<AccountAuthorizationHelperImpl>();
            }
            AccountAuthorizationHelperTestProvider::Mock(mock) => {
                target_catalog_builder
                    .add_value(mock)
                    .bind::<dyn AccountAuthorizationHelper, MockAccountAuthorizationHelper>();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn AccountAuthorizationHelper)]
pub struct AccountAuthorizationHelperImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    rebac_service: Arc<dyn kamu_auth_rebac::RebacService>,
}

#[async_trait::async_trait]
impl AccountAuthorizationHelper for AccountAuthorizationHelperImpl {
    async fn is_admin(&self) -> Result<bool, InternalError> {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => Ok(false),
            CurrentAccountSubject::Logged(l) => {
                use kamu_auth_rebac::RebacServiceExt;

                self.rebac_service
                    .is_account_admin(&l.account_id)
                    .await
                    .int_err()
            }
        }
    }

    async fn can_modify_account(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<bool, InternalError> {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => Ok(false),
            CurrentAccountSubject::Logged(l) if l.account_name == *account_name => Ok(true),
            CurrentAccountSubject::Logged(l) => {
                use kamu_auth_rebac::RebacServiceExt;

                self.rebac_service
                    .is_account_admin(&l.account_id)
                    .await
                    .int_err()
            }
        }
    }

    async fn ensure_account_password_can_be_modified(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), EnsureNotAuthorizedError> {
        if !self.can_modify_account(account_name).await? {
            return Err(EnsureNotAuthorizedError::Access(
                odf::AccessError::Unauthenticated(
                    NotAuthorizedError::modify_account_password(
                        self.current_account_subject.maybe_account_name().cloned(),
                        account_name.clone(),
                    )
                    .into(),
                ),
            ));
        }

        Ok(())
    }

    async fn ensure_account_password_with_confirmation_can_be_modified(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), EnsureNotAuthorizedError> {
        if !self.can_modify_account(account_name).await? {
            return Err(EnsureNotAuthorizedError::Access(
                odf::AccessError::Unauthenticated(
                    NotAuthorizedError::modify_account_password_with_confirmation(
                        self.current_account_subject.maybe_account_name().cloned(),
                        account_name.clone(),
                    )
                    .into(),
                ),
            ));
        }

        Ok(())
    }

    async fn ensure_account_can_be_deleted(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), EnsureNotAuthorizedError> {
        if !self.can_modify_account(account_name).await? {
            return Err(EnsureNotAuthorizedError::Access(
                odf::AccessError::Unauthenticated(
                    NotAuthorizedError::delete_account(
                        self.current_account_subject.maybe_account_name().cloned(),
                        account_name.clone(),
                    )
                    .into(),
                ),
            ));
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(any(feature = "testing", test))]
impl MockAccountAuthorizationHelper {
    pub fn allowing() -> Self {
        let mut mock = Self::new();

        mock.expect_is_admin().returning(|| Ok(true));
        mock.expect_can_modify_account().returning(|_| Ok(true));
        mock.expect_ensure_account_password_can_be_modified()
            .returning(|_| Ok(()));
        mock.expect_ensure_account_password_with_confirmation_can_be_modified()
            .returning(|_| Ok(()));
        mock.expect_ensure_account_can_be_deleted()
            .returning(|_| Ok(()));

        mock
    }

    pub fn disallowing() -> Self {
        let subject_account = odf::AccountName::new_unchecked("user-without-access");

        let mut mock = Self::new();

        mock.expect_is_admin().returning(|| Ok(false));
        mock.expect_can_modify_account().returning(|_| Ok(false));

        let subject_account_clone = subject_account.clone();
        mock.expect_ensure_account_password_can_be_modified()
            .returning(move |account_name| {
                Err(EnsureNotAuthorizedError::Access(
                    odf::AccessError::Unauthenticated(
                        NotAuthorizedError::modify_account_password(
                            Some(subject_account_clone.clone()),
                            account_name.clone(),
                        )
                        .into(),
                    ),
                ))
            });

        let subject_account_clone = subject_account.clone();
        mock.expect_ensure_account_password_with_confirmation_can_be_modified()
            .returning(move |account_name| {
                Err(EnsureNotAuthorizedError::Access(
                    odf::AccessError::Unauthenticated(
                        NotAuthorizedError::modify_account_password_with_confirmation(
                            Some(subject_account_clone.clone()),
                            account_name.clone(),
                        )
                        .into(),
                    ),
                ))
            });

        let subject_account_clone = subject_account.clone();
        mock.expect_ensure_account_can_be_deleted()
            .returning(move |account_name| {
                Err(EnsureNotAuthorizedError::Access(
                    odf::AccessError::Unauthenticated(
                        NotAuthorizedError::delete_account(
                            Some(subject_account_clone.clone()),
                            account_name.clone(),
                        )
                        .into(),
                    ),
                ))
            });

        mock
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum EnsureNotAuthorizedError {
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
pub struct NotAuthorizedError {
    kind: NotAuthorizedErrorKind,
    subject_account: Option<odf::AccountName>,
    object_account: odf::AccountName,
}

impl NotAuthorizedError {
    fn modify_account_password(
        subject_account: Option<odf::AccountName>,
        object_account: odf::AccountName,
    ) -> Self {
        Self {
            kind: NotAuthorizedErrorKind::ModifyAccountPassword,
            subject_account,
            object_account,
        }
    }

    fn modify_account_password_with_confirmation(
        subject_account: Option<odf::AccountName>,
        object_account: odf::AccountName,
    ) -> Self {
        Self {
            kind: NotAuthorizedErrorKind::ModifyAccountPasswordWithConfirmation,
            subject_account,
            object_account,
        }
    }

    fn delete_account(
        subject_account: Option<odf::AccountName>,
        object_account: odf::AccountName,
    ) -> Self {
        Self {
            kind: NotAuthorizedErrorKind::DeleteAccount,
            subject_account,
            object_account,
        }
    }
}

#[derive(Debug)]
pub enum NotAuthorizedErrorKind {
    ModifyAccountPassword,
    ModifyAccountPasswordWithConfirmation,
    DeleteAccount,
}

impl NotAuthorizedErrorKind {
    fn as_action(&self) -> &'static str {
        match self {
            Self::ModifyAccountPassword => "modify account's password",
            Self::ModifyAccountPasswordWithConfirmation => {
                "modify account's password with confirmation"
            }
            Self::DeleteAccount => "delete account",
        }
    }
}

impl std::fmt::Display for NotAuthorizedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let action = self.kind.as_action();
        let object_account = &self.object_account;
        if let Some(subject_account) = &self.subject_account {
            write!(
                f,
                "Account '{subject_account}' is not authorized to {action} '{object_account}'"
            )?;
        } else {
            write!(
                f,
                "Anonymous is not authorized to {action} '{object_account}'",
            )?;
        }

        Ok(())
    }
}

impl From<EnsureNotAuthorizedError> for DeleteAccountError {
    fn from(e: EnsureNotAuthorizedError) -> Self {
        use EnsureNotAuthorizedError as E;

        match e {
            E::Access(e) => Self::Access(e),
            e @ E::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

impl From<EnsureNotAuthorizedError> for ModifyAccountPasswordError {
    fn from(e: EnsureNotAuthorizedError) -> Self {
        use EnsureNotAuthorizedError as E;

        match e {
            E::Access(e) => Self::Access(e),
            e @ E::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

impl From<EnsureNotAuthorizedError> for ModifyAccountPasswordWithConfirmationError {
    fn from(e: EnsureNotAuthorizedError) -> Self {
        use EnsureNotAuthorizedError as E;

        match e {
            E::Access(e) => Self::Access(e),
            e @ E::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
