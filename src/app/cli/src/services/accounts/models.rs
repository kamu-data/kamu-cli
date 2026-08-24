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
use kamu_accounts::{Account, AccountService, CurrentAccountSubject};
use thiserror::Error;

use crate::WorkspaceStatus;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct RelatedAccountIndication {
    pub target_account: TargetAccountSelection,
}

impl RelatedAccountIndication {
    pub fn new(target_account: TargetAccountSelection) -> Self {
        Self { target_account }
    }

    pub fn is_explicit(&self) -> bool {
        self.target_account != TargetAccountSelection::Current
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TargetAccountSelection {
    Current,
    Specific { account_name: String },
    AllUsers,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct CurrentAccountIndication {
    pub account_name: odf::AccountName,
    pub user_name: String,
    pub specified_explicitly: bool,
}

impl CurrentAccountIndication {
    pub fn new<A, U>(
        account_name: A,
        user_name: U,
        specified_explicitly: bool,
    ) -> Result<Self, odf::metadata::ParseError<odf::AccountName>>
    where
        A: Into<String>,
        U: Into<String>,
    {
        let account_name = odf::AccountName::try_from(account_name.into())?;

        Ok(Self {
            account_name,
            user_name: user_name.into(),
            specified_explicitly,
        })
    }

    pub fn is_explicit(&self) -> bool {
        self.specified_explicitly
    }

    pub async fn to_current_account_subject(
        &self,
        workspace_status: WorkspaceStatus,
        account_service: Arc<dyn AccountService>,
    ) -> Result<CurrentAccountSubject, ToCurrentAccountSubjectError> {
        match workspace_status {
            WorkspaceStatus::NoWorkspace | WorkspaceStatus::AboutToBeCreated(_) => {
                // NOTE: At this stage, real accounts do not exist yet.
                let dummy_subject = CurrentAccountSubject::logged(
                    Account::seed_resource_id_from_name(self.account_name.as_str()),
                    odf::AccountID::new_seeded_ed25519(self.account_name.as_bytes()),
                    self.account_name.clone(),
                );

                Ok(dummy_subject)
            }
            WorkspaceStatus::Created(_) => {
                let maybe_account = account_service.account_by_name(&self.account_name).await?;
                let Some(account) = maybe_account else {
                    return Err(ToCurrentAccountSubjectError::NotRegisteredAccount {
                        account: self.account_name.clone(),
                    });
                };

                let subject = CurrentAccountSubject::logged_from_account(&account);

                Ok(subject)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ToCurrentAccountSubjectError {
    #[error("Account '{account}' not registered")]
    NotRegisteredAccount { account: odf::AccountName },

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
