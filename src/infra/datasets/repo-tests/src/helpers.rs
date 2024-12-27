// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use chrono::{SubsecRound, Utc};
use kamu_accounts::{Account, AccountRepository, AccountType};
use kamu_datasets::DatasetEntry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn new_account_with_name(
    account_repo: &Arc<dyn AccountRepository>,
    account_name: &str,
) -> Account {
    let (_, id) = odf::AccountID::new_generated_ed25519();

    let account = Account {
        id,
        account_name: odf::AccountName::new_unchecked(account_name),
        email: None,
        display_name: String::new(),
        account_type: AccountType::User,
        avatar_url: None,
        registered_at: Default::default(),
        is_admin: false,
        provider: "unit-test-provider".to_string(),
        provider_identity_key: account_name.to_string(),
    };
    let create_res = account_repo.create_account(&account).await;

    assert_matches!(create_res, Ok(_));

    account
}

pub(crate) async fn new_account(account_repo: &Arc<dyn AccountRepository>) -> Account {
    new_account_with_name(account_repo, "unit-test-user").await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn new_dataset_entry_with(owner: &Account, dataset_name: &str) -> DatasetEntry {
    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let owner_id = owner.id.clone();
    let dataset_alias = odf::DatasetName::new_unchecked(dataset_name);
    let created_at = Utc::now().round_subsecs(6);

    DatasetEntry::new(dataset_id, owner_id, dataset_alias, created_at)
}

pub(crate) fn new_dataset_entry(owner: &Account) -> DatasetEntry {
    new_dataset_entry_with(owner, "dataset")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
