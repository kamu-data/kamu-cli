// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{AccountID, AccountName, DatasetAlias, DatasetHandle, DatasetID, DatasetName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn handle(account: &impl AsRef<str>, dataset_name: &impl AsRef<str>) -> DatasetHandle {
    let account_name = AccountName::new_unchecked(account.as_ref());
    let dataset_name = DatasetName::new_unchecked(dataset_name.as_ref());
    let dataset_id = {
        let mut seed = Vec::new();
        seed.extend(account_name.as_bytes());
        seed.extend(dataset_name.as_bytes());
        DatasetID::new_seeded_ed25519(&seed)
    };

    DatasetHandle::new(
        dataset_id,
        DatasetAlias::new(Some(account_name), dataset_name),
    )
}

pub fn alias(account: &impl AsRef<str>, dataset_name: &impl AsRef<str>) -> DatasetAlias {
    DatasetAlias::new(
        Some(AccountName::new_unchecked(account.as_ref())),
        DatasetName::new_unchecked(dataset_name.as_ref()),
    )
}

pub fn account_id(name: &impl AsRef<str>) -> AccountID {
    let name = AccountName::new_unchecked(name.as_ref());
    AccountID::new_seeded_ed25519(name.as_bytes())
}

pub fn account_name(value: &impl AsRef<str>) -> AccountName {
    AccountName::new_unchecked(value.as_ref())
}

pub fn account_id_by_maybe_name(maybe_name: &Option<AccountName>, default_name: &str) -> AccountID {
    if let Some(name) = maybe_name {
        AccountID::new_seeded_ed25519(name.as_bytes())
    } else {
        AccountID::new_seeded_ed25519(default_name.as_bytes())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
