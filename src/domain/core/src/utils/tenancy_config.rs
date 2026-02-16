// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use kamu_accounts::DEFAULT_ACCOUNT_NAME_STR;

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

    pub fn default_dataset_visibility(&self) -> odf::DatasetVisibility {
        match *self {
            TenancyConfig::MultiTenant => odf::DatasetVisibility::Private,
            TenancyConfig::SingleTenant => odf::DatasetVisibility::Public,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
