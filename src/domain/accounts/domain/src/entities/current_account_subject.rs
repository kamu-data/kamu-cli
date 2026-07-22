// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(any(feature = "testing", test))]
use crate::DEFAULT_ACCOUNT_ID;
use crate::DEFAULT_ACCOUNT_NAME;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum CurrentAccountSubject {
    Logged(LoggedAccount),
    Anonymous(AnonymousAccountReason),
}

#[derive(Debug, Clone)]
pub struct LoggedAccount {
    pub account_handle: odf::AccountHandle,
}

#[derive(Debug, Copy, Clone)]
pub enum AnonymousAccountReason {
    NoAuthenticationProvided,
    AuthenticationInvalid,
    AuthenticationExpired,
}

impl CurrentAccountSubject {
    pub fn anonymous(reason: AnonymousAccountReason) -> Self {
        Self::Anonymous(reason)
    }

    pub fn logged_from_account(account: &crate::Account) -> Self {
        Self::Logged(LoggedAccount {
            account_handle: account.into(),
        })
    }

    #[cfg(any(feature = "testing", test))]
    pub fn anonymous_no_authentication_provided() -> Self {
        Self::Anonymous(AnonymousAccountReason::NoAuthenticationProvided)
    }

    pub fn logged(
        resource_id: odf::ResourceID,
        account_id: odf::AccountID,
        account_name: odf::AccountName,
    ) -> Self {
        Self::Logged(LoggedAccount {
            account_handle: odf::AccountHandle {
                id: resource_id,
                did: account_id,
                name: account_name,
            },
        })
    }

    #[cfg(any(feature = "testing", test))]
    pub fn new_test() -> Self {
        Self::logged(
            *crate::DEFAULT_ACCOUNT_RESOURCE_ID,
            DEFAULT_ACCOUNT_ID.clone(),
            DEFAULT_ACCOUNT_NAME.clone(),
        )
    }

    #[cfg(any(feature = "testing", test))]
    pub fn new_test_with(account_name: &impl AsRef<str>) -> Self {
        Self::logged(
            crate::Account::seed_resource_id_from_name(account_name.as_ref()),
            odf::metadata::testing::account_id(account_name),
            odf::AccountName::new_unchecked(account_name),
        )
    }

    pub fn account_id(&self) -> &odf::AccountID {
        match self {
            CurrentAccountSubject::Anonymous(_) => {
                panic!("Anonymous account misused");
            }
            CurrentAccountSubject::Logged(l) => &l.account_handle.did,
        }
    }

    pub fn get_maybe_logged_account_id(&self) -> Option<&odf::AccountID> {
        match self {
            CurrentAccountSubject::Anonymous(_) => None,
            CurrentAccountSubject::Logged(logged_account) => {
                Some(&logged_account.account_handle.did)
            }
        }
    }

    pub fn account_name(&self) -> &odf::AccountName {
        match self {
            CurrentAccountSubject::Anonymous(_) => {
                panic!("Anonymous account misused");
            }
            CurrentAccountSubject::Logged(l) => &l.account_handle.name,
        }
    }

    pub fn maybe_account_name(&self) -> Option<&odf::AccountName> {
        match self {
            CurrentAccountSubject::Logged(l) => Some(&l.account_handle.name),
            CurrentAccountSubject::Anonymous(_) => None,
        }
    }

    pub fn account_name_or_default(&self) -> &odf::AccountName {
        match self {
            CurrentAccountSubject::Logged(l) => &l.account_handle.name,
            CurrentAccountSubject::Anonymous(_) => &DEFAULT_ACCOUNT_NAME,
        }
    }

    pub fn resolve_account_name_by_dataset_alias(
        &self,
        dataset_alias: &odf::DatasetAlias,
    ) -> odf::AccountName {
        if dataset_alias.is_multi_tenant() {
            // Safety: In multi-tenant, we have a name.
            dataset_alias.account_name.as_ref().unwrap().clone()
        } else {
            self.account_name_or_default().clone()
        }
    }

    pub fn account_handle(&self) -> &odf::AccountHandle {
        match self {
            CurrentAccountSubject::Anonymous(_) => {
                panic!("Anonymous account misused");
            }
            CurrentAccountSubject::Logged(l) => &l.account_handle,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
