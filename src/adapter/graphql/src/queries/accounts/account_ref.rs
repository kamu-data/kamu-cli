// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric as odf;

use crate::prelude::*;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRef {
    account_id: AccountID,
    account_name: AccountName,
}

#[Object]
impl AccountRef {
    #[graphql(skip)]
    pub(crate) fn from_account_name(name: &odf::AccountName) -> Self {
        Self {
            account_id: AccountID::from(odf::FAKE_ACCOUNT_ID),
            account_name: name.clone().into(),
        }
    }

    #[graphql(skip)]
    pub(crate) fn from_dataset_alias(ctx: &Context<'_>, alias: &odf::DatasetAlias) -> Self {
        if alias.is_multi_tenant() {
            Self {
                account_id: AccountID::from(odf::FAKE_ACCOUNT_ID),
                account_name: alias.account_name.as_ref().unwrap().clone().into(),
            }
        } else {
            let current_account_subject =
                from_catalog::<kamu_core::CurrentAccountSubject>(ctx).unwrap();
            Self {
                account_id: AccountID::from(odf::FAKE_ACCOUNT_ID),
                account_name: current_account_subject.account_name.clone().into(),
            }
        }
    }

    #[graphql(skip)]
    pub(crate) fn account_name_internal(&self) -> &AccountName {
        &self.account_name
    }

    /// Unique and stable identitfier of this account
    async fn id(&self) -> &AccountID {
        &self.account_id
    }

    /// Symbolic account name
    async fn account_name(&self) -> &AccountName {
        &self.account_name
    }
}

///////////////////////////////////////////////////////////////////////////////
