// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use kamu_accounts::{DEFAULT_ACCOUNT_NAME, DEFAULT_ACCOUNT_NAME_STR, Principal};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
pub enum TenancyConfig {
    #[default]
    SingleTenant,
    MultiTenant,
}

impl TenancyConfig {
    pub fn make_alias(
        &self,
        owner_name: odf::AccountName,
        dataset_name: odf::DatasetName,
    ) -> odf::DatasetAlias {
        match *self {
            TenancyConfig::MultiTenant => odf::DatasetAlias::new(Some(owner_name), dataset_name),
            TenancyConfig::SingleTenant => odf::DatasetAlias::new(None, dataset_name),
        }
    }

    pub fn canonical_alias(&self, alias: &odf::DatasetAlias) -> odf::DatasetAlias {
        match *self {
            TenancyConfig::MultiTenant => {
                assert!(
                    alias.account_name.is_some(),
                    "Multi-tenant tenancy requires dataset alias to have an account name"
                );
                alias.clone()
            }
            TenancyConfig::SingleTenant => {
                match alias.account_name.as_ref() {
                    None => {}
                    Some(name) if name.as_str() == DEFAULT_ACCOUNT_NAME_STR => {}
                    Some(_) => panic!(
                        "Single-tenant tenancy requires dataset alias to not have an account name"
                    ),
                }
                odf::DatasetAlias::new(None, alias.dataset_name.clone())
            }
        }
    }

    pub fn canonical_ref(&self, dataset_ref: &odf::DatasetRef) -> odf::DatasetRef {
        match dataset_ref {
            odf::DatasetRef::ID(id) => id.as_local_ref(),
            odf::DatasetRef::Alias(alias) => self.canonical_alias(alias).into_local_ref(),
            odf::DatasetRef::Handle(hdl) => {
                odf::DatasetHandle::new(hdl.id.clone(), self.canonical_alias(&hdl.alias), hdl.kind)
                    .into_local_ref()
            }
        }
    }

    /// The account a bare dataset alias belongs to when `principal` resolves
    /// it: the default account in a single-tenant workspace; otherwise the
    /// user's own account and the default account for an anonymous request.
    /// `None` for the node in a multi-tenant workspace: it has no account, so
    /// a bare alias names no dataset
    pub fn bare_alias_account_name<'a>(
        &self,
        principal: &'a Principal,
    ) -> Option<&'a odf::AccountName> {
        match (*self, principal) {
            (TenancyConfig::SingleTenant, _)
            | (TenancyConfig::MultiTenant, Principal::Anonymous) => Some(&DEFAULT_ACCOUNT_NAME),
            (TenancyConfig::MultiTenant, Principal::User(account_handle)) => {
                Some(&account_handle.name)
            }
            (TenancyConfig::MultiTenant, Principal::Node) => None,
        }
    }

    pub fn default_dataset_visibility(&self) -> odf::DatasetVisibility {
        match *self {
            TenancyConfig::MultiTenant => odf::DatasetVisibility::Private,
            TenancyConfig::SingleTenant => odf::DatasetVisibility::Public,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use kamu_accounts::{Account, AccountHandle};

    use super::*;

    fn user(name: &str) -> Principal {
        Principal::User(AccountHandle {
            id: Account::seed_resource_id_from_name(name),
            did: odf::metadata::testing::account_id(&name),
            name: odf::AccountName::new_unchecked(name),
        })
    }

    #[test]
    fn test_bare_alias_account_name_single_tenant_is_always_default() {
        for principal in [user("alice"), Principal::Node, Principal::Anonymous] {
            assert_eq!(
                TenancyConfig::SingleTenant.bare_alias_account_name(&principal),
                Some(&*DEFAULT_ACCOUNT_NAME)
            );
        }
    }

    #[test]
    fn test_bare_alias_account_name_multi_tenant_follows_principal() {
        let mt = TenancyConfig::MultiTenant;

        assert_eq!(
            mt.bare_alias_account_name(&user("alice")),
            Some(&odf::AccountName::new_unchecked("alice"))
        );
        assert_eq!(mt.bare_alias_account_name(&Principal::Node), None);
        assert_eq!(
            mt.bare_alias_account_name(&Principal::Anonymous),
            Some(&*DEFAULT_ACCOUNT_NAME)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
