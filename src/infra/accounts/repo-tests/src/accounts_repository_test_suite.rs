// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use dill::Catalog;
use kamu_accounts::*;

use crate::make_test_account;

pub(crate) const GITHUB_ACCOUNT_ID_WASYA: &str = "8875907";
pub(crate) const GITHUB_ACCOUNT_ID_PETYA: &str = "8875908";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_missing_account_not_found(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    let maybe_account_id = account_repo
        .find_account_id_by_email("test@example.com")
        .await
        .unwrap();
    assert!(maybe_account_id.is_none());

    let account_name = odf::AccountName::new_unchecked("wasya");
    let maybe_account_id = account_repo
        .find_account_id_by_name(&account_name)
        .await
        .unwrap();
    assert!(maybe_account_id.is_none());

    let account_id = odf::AccountID::new_seeded_ed25519(b"wrong");
    let account_result = account_repo.get_account_by_id(&account_id).await;
    assert_matches!(account_result, Err(GetAccountByIdError::NotFound(_)));

    let account_result = account_repo.get_account_by_name(&account_name).await;
    assert_matches!(account_result, Err(GetAccountByNameError::NotFound(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_locate_password_account(catalog: &Catalog) {
    let account = Account {
        email: Some(String::from("test@example.com")),
        display_name: String::from("Wasya Pupkin"),
        ..make_test_account("wasya", PROVIDER_PASSWORD, "wasya")
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account).await.unwrap();

    let maybe_account_id = account_repo
        .find_account_id_by_email("test@example.com")
        .await
        .unwrap();
    assert_eq!(maybe_account_id.as_ref(), Some(&account.id));

    let account_name = odf::AccountName::new_unchecked("wasya");
    let maybe_account_id = account_repo
        .find_account_id_by_name(&account_name)
        .await
        .unwrap();
    assert_eq!(maybe_account_id.as_ref(), Some(&account.id));

    let maybe_account_id = account_repo
        .find_account_id_by_provider_identity_key("wasya")
        .await
        .unwrap();
    assert_eq!(maybe_account_id.as_ref(), Some(&account.id));

    let db_account = account_repo
        .get_account_by_id(&maybe_account_id.unwrap())
        .await
        .unwrap();
    assert_eq!(db_account, account);

    let db_account = account_repo
        .get_account_by_name(&account_name)
        .await
        .unwrap();
    assert_eq!(db_account, account);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_locate_github_account(catalog: &Catalog) {
    const GITHUB_ACCOUNT_ID: &str = "8875909";

    let account = Account {
        email: Some(String::from("test@example.com")),
        display_name: String::from("Wasya Pupkin"),
        ..make_test_account(
            "wasya",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            GITHUB_ACCOUNT_ID,
        )
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account).await.unwrap();

    let maybe_account_id = account_repo
        .find_account_id_by_email("test@example.com")
        .await
        .unwrap();
    assert_eq!(maybe_account_id.as_ref(), Some(&account.id));

    let account_name = odf::AccountName::new_unchecked("wasya");
    let maybe_account_id = account_repo
        .find_account_id_by_name(&account_name)
        .await
        .unwrap();
    assert_eq!(maybe_account_id.as_ref(), Some(&account.id));

    let maybe_account_id = account_repo
        .find_account_id_by_provider_identity_key(GITHUB_ACCOUNT_ID)
        .await
        .unwrap();
    assert_eq!(maybe_account_id.as_ref(), Some(&account.id));

    let db_account = account_repo
        .get_account_by_id(&maybe_account_id.unwrap())
        .await
        .unwrap();
    assert_eq!(db_account, account);

    let db_account = account_repo
        .get_account_by_name(&account_name)
        .await
        .unwrap();
    assert_eq!(db_account, account);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_locate_multiple_github_account(catalog: &Catalog) {
    let account_wasya = Account {
        email: Some(String::from("wasya_test@example.com")),
        display_name: String::from("Wasya Pupkin"),
        ..make_test_account(
            "wasya",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            GITHUB_ACCOUNT_ID_WASYA,
        )
    };
    let account_petya = Account {
        email: Some(String::from("test@example.com")),
        display_name: String::from("Petya Pupkin"),
        ..make_test_account(
            "petya",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            GITHUB_ACCOUNT_ID_PETYA,
        )
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account_wasya).await.unwrap();
    account_repo.create_account(&account_petya).await.unwrap();

    let account_id_wasya = account_repo
        .find_account_id_by_name(&odf::AccountName::new_unchecked("wasya"))
        .await
        .unwrap()
        .unwrap();
    let account_id_petya = account_repo
        .find_account_id_by_name(&odf::AccountName::new_unchecked("petya"))
        .await
        .unwrap()
        .unwrap();

    let mut db_accounts = account_repo
        .get_accounts_by_ids(vec![account_id_wasya, account_id_petya])
        .await
        .unwrap();

    // Different databases returning different order for where in clause
    // Such as for this specific query order is not important sort here to
    // keep tests consistent
    db_accounts.sort_by(|a, b| a.registered_at.cmp(&b.registered_at));
    assert_eq!(db_accounts, [account_wasya, account_petya]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_locate_account_without_email(catalog: &Catalog) {
    let account = Account {
        display_name: String::from("Wasya Pupkin"),
        ..make_test_account("wasya", PROVIDER_PASSWORD, "wasya")
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account).await.unwrap();

    let loaded_account = account_repo
        .get_account_by_name(&account.account_name)
        .await
        .unwrap();
    assert_eq!(account, loaded_account);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_duplicate_password_account_id(catalog: &Catalog) {
    let id = odf::AccountID::new_generated_ed25519().1;
    let account = Account {
        id: id.clone(),
        ..make_test_account("wasya", PROVIDER_PASSWORD, "wasya")
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    account_repo
        .create_account(&Account {
            id, // re-use same id
            ..make_test_account("petya", PROVIDER_PASSWORD, "petya")
        })
        .await
        .unwrap();

    assert_matches!(
        account_repo.create_account(&account).await,
        Err(CreateAccountError::Duplicate(CreateAccountErrorDuplicate{ account_field: field })) if field == CreateAccountDuplicateField::Id
    );

    // Id, name, and provider credentials key in Password case are all connected
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_duplicate_password_account_email(catalog: &Catalog) {
    let account = Account {
        email: Some(String::from("test@example.com")),
        ..make_test_account("wasya", PROVIDER_PASSWORD, "wasya")
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    account_repo.create_account(&account).await.unwrap();

    assert_matches!(
        account_repo.create_account(&Account {
            email: Some(String::from("test@example.com")),
            ..make_test_account("petya", PROVIDER_PASSWORD, "petya")
        }).await,
        Err(CreateAccountError::Duplicate(CreateAccountErrorDuplicate{ account_field: field })) if field == CreateAccountDuplicateField::Email
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_duplicate_github_account_id(catalog: &Catalog) {
    const GITHUB_ACCOUNT_ID: &str = "8875909";

    let id = odf::AccountID::new_generated_ed25519().1;
    let account = Account {
        id: id.clone(),
        ..make_test_account(
            "wasya",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            GITHUB_ACCOUNT_ID,
        )
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    account_repo
        .create_account(&Account {
            id, // reuse id
            ..make_test_account("petya", kamu_adapter_oauth::PROVIDER_GITHUB, "12345")
        })
        .await
        .unwrap();

    assert_matches!(
        account_repo.create_account(&account).await,
        Err(CreateAccountError::Duplicate(CreateAccountErrorDuplicate{ account_field: field })) if field == CreateAccountDuplicateField::Id
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_duplicate_github_account_name(catalog: &Catalog) {
    const GITHUB_ACCOUNT_ID: &str = "8875909";

    let account = Account {
        id: odf::AccountID::new_generated_ed25519().1,
        ..make_test_account(
            "wasya",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            GITHUB_ACCOUNT_ID,
        )
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    account_repo.create_account(&account).await.unwrap();

    assert_matches!(
        account_repo.create_account(&Account {
            id: odf::AccountID::new_generated_ed25519().1,
            ..make_test_account(
                "wasya",
                kamu_adapter_oauth::PROVIDER_GITHUB,
                "12345",
            )
        }).await,
        Err(CreateAccountError::Duplicate(CreateAccountErrorDuplicate{ account_field: field })) if field == CreateAccountDuplicateField::Name
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_duplicate_github_account_provider_identity(catalog: &Catalog) {
    const GITHUB_ACCOUNT_ID: &str = "8875909";

    let account = Account {
        id: odf::AccountID::new_generated_ed25519().1,
        ..make_test_account(
            "wasya",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            GITHUB_ACCOUNT_ID,
        )
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    account_repo.create_account(&account).await.unwrap();

    assert_matches!(
        account_repo.create_account(&Account {
            id: odf::AccountID::new_generated_ed25519().1,
            ..make_test_account(
                "petya",
                kamu_adapter_oauth::PROVIDER_GITHUB,
                GITHUB_ACCOUNT_ID,
            )
        }).await,
        Err(CreateAccountError::Duplicate(CreateAccountErrorDuplicate{ account_field: field })) if field == CreateAccountDuplicateField::ProviderIdentityKey
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_duplicate_github_account_email(catalog: &Catalog) {
    const GITHUB_ACCOUNT_ID: &str = "8875909";

    let account = Account {
        id: odf::AccountID::new_generated_ed25519().1,
        email: Some(String::from("test@example.com")),
        ..make_test_account(
            "wasya",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            GITHUB_ACCOUNT_ID,
        )
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    account_repo.create_account(&account).await.unwrap();

    assert_matches!(
        account_repo.create_account(&Account {
            id: odf::AccountID::new_generated_ed25519().1,
            email: Some(String::from("test@example.com")),
            ..make_test_account("petya", kamu_adapter_oauth::PROVIDER_GITHUB, "12345")
        }).await,
        Err(CreateAccountError::Duplicate(CreateAccountErrorDuplicate{ account_field: field })) if field == CreateAccountDuplicateField::Email
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
