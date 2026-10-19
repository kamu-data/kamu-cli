// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolved identity of an account: the id of its resource, its DID and its
/// name.
///
/// Converts into [`odf::ResourceOwnerHandle`] where the account is named as the
/// owner of a resource. Not every account may own resources, so the
/// conversion marks that role explicitly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountHandle {
    pub id: odf::ResourceID,
    pub did: odf::AccountID,
    pub name: odf::AccountName,
}

impl From<&Account> for AccountHandle {
    fn from(account: &Account) -> Self {
        Self {
            id: account.resource_id,
            did: account.id.clone(),
            name: account.account_name.clone(),
        }
    }
}

impl From<AccountHandle> for odf::ResourceOwnerHandle {
    fn from(value: AccountHandle) -> Self {
        let AccountHandle { id, did, name } = value;
        Self { id, did, name }
    }
}

impl From<&AccountHandle> for odf::ResourceOwnerHandle {
    fn from(value: &AccountHandle) -> Self {
        value.clone().into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
