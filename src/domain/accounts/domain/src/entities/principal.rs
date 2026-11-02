// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use crate::{Account, AccountHandle, CurrentAccountSubject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reserved name of the local node (RFC-020, Appendix C)
const NODE_PRINCIPAL_NAME: &str = "node";

static NODE_PRINCIPAL_RESOURCE_ID: LazyLock<odf::ResourceID> =
    LazyLock::new(|| Account::seed_resource_id_from_name(NODE_PRINCIPAL_NAME));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The identity whose permissions are checked (RFC-020, "Principals").
///
/// Usually the authenticated caller, but not always: background work runs as
/// [`Principal::Node`] while its catalog still holds a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    /// An authenticated account of a person
    User(AccountHandle),
    /// The local node, acting in system work; passes every permission check
    Node,
    /// An unauthenticated request
    Anonymous,
}

impl Principal {
    /// The ODF handle of the principal; `None` for [`Principal::Anonymous`],
    /// which carries no identity
    pub fn handle(&self) -> Option<odf::metadata::auth::PrincipalHandle> {
        match self {
            Self::User(account_handle) => Some(odf::metadata::auth::PrincipalHandle {
                r#type: odf::metadata::auth::User::schema().clone(),
                account: None,
                id: account_handle.id,
                did: Some(account_handle.did.clone().into()),
                name: account_handle.name.clone().into(),
            }),
            Self::Node => Some(odf::metadata::auth::PrincipalHandle {
                r#type: odf::metadata::auth::Node::schema().clone(),
                account: None,
                id: *NODE_PRINCIPAL_RESOURCE_ID,
                did: None,
                name: odf::metadata::resources::ResourceName::new_unchecked(NODE_PRINCIPAL_NAME),
            }),
            Self::Anonymous => None,
        }
    }

    /// The account name of a [`Principal::User`]
    pub fn maybe_account_name(&self) -> Option<&odf::AccountName> {
        match self {
            Self::User(account_handle) => Some(&account_handle.name),
            Self::Node | Self::Anonymous => None,
        }
    }

    #[cfg(any(feature = "testing", test))]
    pub fn new_test() -> Self {
        Self::from(&CurrentAccountSubject::new_test())
    }

    #[cfg(any(feature = "testing", test))]
    pub fn new_test_with(account_name: &impl AsRef<str>) -> Self {
        Self::from(&CurrentAccountSubject::new_test_with(account_name))
    }
}

impl From<&CurrentAccountSubject> for Principal {
    fn from(subject: &CurrentAccountSubject) -> Self {
        match subject {
            CurrentAccountSubject::Logged(logged) => Self::User(logged.account_handle.clone()),
            CurrentAccountSubject::Anonymous(_) => Self::Anonymous,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::AnonymousAccountReason;

    #[test]
    fn test_principal_from_subject() {
        let subject = CurrentAccountSubject::new_test_with(&"alice");

        assert_eq!(
            Principal::from(&subject),
            Principal::User(subject.account_handle().clone())
        );
        assert_eq!(
            Principal::from(&CurrentAccountSubject::anonymous(
                AnonymousAccountReason::AuthenticationExpired
            )),
            Principal::Anonymous
        );
    }

    #[test]
    fn test_principal_handle() {
        let subject = CurrentAccountSubject::new_test_with(&"alice");
        let account_handle = subject.account_handle();

        let user = Principal::from(&subject).handle().unwrap();
        assert_eq!(user.r#type, *odf::metadata::auth::User::schema());
        assert_eq!(user.id, account_handle.id);
        assert_eq!(user.did, Some(account_handle.did.clone().into()));
        assert_eq!(user.name.as_str(), "alice");

        let node = Principal::Node.handle().unwrap();
        assert_eq!(node.r#type, *odf::metadata::auth::Node::schema());
        assert_eq!(node.id, Account::seed_resource_id_from_name("node"));
        assert_eq!(node.did, None);
        assert_eq!(node.name.as_str(), "node");

        assert_eq!(Principal::Anonymous.handle(), None);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
