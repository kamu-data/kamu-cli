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

use chrono::Utc;
use database_common::PaginationOpts;
use dill::Catalog;
use email_utils::Email;
use kamu_accounts::*;

use crate::make_test_account;

pub(crate) const GITHUB_ACCOUNT_ID_WASYA: &str = "8875907";
pub(crate) const GITHUB_ACCOUNT_ID_PETYA: &str = "8875908";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_missing_account_not_found(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    let maybe_account_id = account_repo
        .find_account_id_by_email(&Email::parse("test@example.com").unwrap())
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
        email: Email::parse("test@example.com").unwrap(),
        display_name: String::from("Wasya Pupkin"),
        ..make_test_account("wasya", "wasya@example.com", PROVIDER_PASSWORD, "wasya")
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account).await.unwrap();

    let maybe_account_id = account_repo
        .find_account_id_by_email(&Email::parse("test@example.com").unwrap())
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
        email: Email::parse("test@example.com").unwrap(),
        display_name: String::from("Wasya Pupkin"),
        ..make_test_account(
            "wasya",
            "wasya@example.com",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            GITHUB_ACCOUNT_ID,
        )
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account).await.unwrap();

    let maybe_account_id = account_repo
        .find_account_id_by_email(&Email::parse("test@example.com").unwrap())
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
        email: Email::parse("wasya.test@example.com").unwrap(),
        display_name: String::from("Wasya Pupkin"),
        ..make_test_account(
            "wasya",
            "wasya@example.com",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            GITHUB_ACCOUNT_ID_WASYA,
        )
    };
    let account_petya = Account {
        email: Email::parse("test@example.com").unwrap(),
        display_name: String::from("Petya Pupkin"),
        ..make_test_account(
            "petya",
            "petya@example.com",
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
        .get_accounts_by_ids(&[account_id_wasya, account_id_petya])
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
        ..make_test_account("wasya", "wasya@example.com", PROVIDER_PASSWORD, "wasya")
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
        ..make_test_account("wasya", "wasya@example.com", PROVIDER_PASSWORD, "wasya")
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    account_repo
        .create_account(&Account {
            id, // re-use same id
            ..make_test_account("petya", "petya@example.com", PROVIDER_PASSWORD, "petya")
        })
        .await
        .unwrap();

    assert_matches!(
        account_repo.create_account(&account).await,
        Err(CreateAccountError::Duplicate(AccountErrorDuplicate{ account_field: field })) if field == AccountDuplicateField::Id
    );

    // Id, name, and provider credentials key in Password case are all connected
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_duplicate_password_account_email(catalog: &Catalog) {
    let account = Account {
        email: Email::parse("test@example.com").unwrap(),
        ..make_test_account("wasya", "wasya@example.com", PROVIDER_PASSWORD, "wasya")
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    account_repo.create_account(&account).await.unwrap();

    assert_matches!(
        account_repo.create_account(&Account {
            email: Email::parse("test@example.com").unwrap(),
            ..make_test_account("petya", "petya@example.com", PROVIDER_PASSWORD, "petya")
        }).await,
        Err(CreateAccountError::Duplicate(AccountErrorDuplicate{ account_field: field })) if field == AccountDuplicateField::Email
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
            "wasya@example.com",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            GITHUB_ACCOUNT_ID,
        )
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    account_repo
        .create_account(&Account {
            id, // reuse id
            ..make_test_account(
                "petya",
                "petya@example.com",
                kamu_adapter_oauth::PROVIDER_GITHUB,
                "12345",
            )
        })
        .await
        .unwrap();

    assert_matches!(
        account_repo.create_account(&account).await,
        Err(CreateAccountError::Duplicate(AccountErrorDuplicate{ account_field: field })) if field == AccountDuplicateField::Id
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_duplicate_github_account_name(catalog: &Catalog) {
    const GITHUB_ACCOUNT_ID: &str = "8875909";

    let account = Account {
        id: odf::AccountID::new_generated_ed25519().1,
        ..make_test_account(
            "wasya",
            "wasya@example.com",
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
                "wasya2@example.com", // different email
                kamu_adapter_oauth::PROVIDER_GITHUB,
                "12345",
            )
        }).await,
        Err(CreateAccountError::Duplicate(AccountErrorDuplicate{ account_field: field })) if field == AccountDuplicateField::Name
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_duplicate_github_account_provider_identity(catalog: &Catalog) {
    const GITHUB_ACCOUNT_ID: &str = "8875909";

    let account = Account {
        id: odf::AccountID::new_generated_ed25519().1,
        ..make_test_account(
            "wasya",
            "wasya@example.com",
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
                "petya@example.com",
                kamu_adapter_oauth::PROVIDER_GITHUB,
                GITHUB_ACCOUNT_ID,
            )
        }).await,
        Err(CreateAccountError::Duplicate(AccountErrorDuplicate{ account_field: field })) if field == AccountDuplicateField::ProviderIdentityKey
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_duplicate_github_account_email(catalog: &Catalog) {
    const GITHUB_ACCOUNT_ID: &str = "8875909";

    let account = Account {
        id: odf::AccountID::new_generated_ed25519().1,
        ..make_test_account(
            "wasya",
            "wasya@example.com",
            kamu_adapter_oauth::PROVIDER_GITHUB,
            GITHUB_ACCOUNT_ID,
        )
    };

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    account_repo.create_account(&account).await.unwrap();

    assert_matches!(
        account_repo.create_account(&Account {
            id: odf::AccountID::new_generated_ed25519().1,
            ..make_test_account("petya", "wasya@example.com",kamu_adapter_oauth::PROVIDER_GITHUB, "12345")
        }).await,
        Err(CreateAccountError::Duplicate(AccountErrorDuplicate{ account_field: field })) if field == AccountDuplicateField::Email
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_accounts_by_name_pattern(catalog: &Catalog) {
    fn account(account_name: &str, display_name: &str, email: &str) -> Account {
        Account {
            id: account_id(&account_name),
            account_name: odf::AccountName::new_unchecked(account_name),
            email: Email::parse(email).unwrap(),
            display_name: display_name.to_string(),
            account_type: AccountType::User,
            avatar_url: None,
            registered_at: Utc::now(),
            is_admin: false,
            provider: PROVIDER_PASSWORD.to_string(),
            provider_identity_key: account_name.to_string(),
        }
    }

    async fn search(
        repo: &Arc<dyn AccountRepository>,
        query: &str,
        filters: SearchAccountsByNamePatternFilters,
    ) -> Vec<odf::AccountName> {
        use futures::TryStreamExt;

        let accounts = repo
            .search_accounts_by_name_pattern(
                query,
                filters,
                PaginationOpts {
                    limit: 20,
                    offset: 0,
                },
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        accounts.into_iter().map(|a| a.account_name).collect()
    }

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    use odf::metadata::testing::{account_id, account_name as name};
    use SearchAccountsByNamePatternFilters as Filters;

    let accounts = [
        account("user1", "alice (deactivated)", "alice@example.com"),
        account("user2", "alice", "alice-new@example.com"),
        account("user3", "bob", "bob@example.com"),
        account("admin1", "admin", "admin@example.com"),
    ];

    for account in accounts {
        account_repo.create_account(&account).await.unwrap();
    }

    let empty_accounts: [odf::AccountName; 0] = [];

    // All
    pretty_assertions::assert_eq!(
        [
            name(&"admin1"),
            name(&"user1"),
            name(&"user2"),
            name(&"user3"),
        ],
        *search(&account_repo, "", Filters::default()).await
    );

    // Search by account name
    pretty_assertions::assert_eq!(
        [name(&"user1"), name(&"user2"), name(&"user3")],
        *search(&account_repo, "us", Filters::default()).await
    );
    pretty_assertions::assert_eq!(
        [name(&"user1"), name(&"user2"), name(&"user3")],
        *search(&account_repo, "se", Filters::default()).await
    );
    pretty_assertions::assert_eq!(
        [name(&"user1")],
        *search(&account_repo, "r1", Filters::default()).await
    );
    pretty_assertions::assert_eq!(
        [name(&"user2")],
        *search(
            &account_repo,
            "user",
            Filters {
                exclude_accounts_by_ids: Some(vec![account_id(&"user1"), account_id(&"user3")])
            }
        )
        .await
    );

    // Search by display name
    pretty_assertions::assert_eq!(
        [name(&"user1"), name(&"user2")],
        *search(&account_repo, "ali", Filters::default()).await
    );
    pretty_assertions::assert_eq!(
        [name(&"user3")],
        *search(&account_repo, "ob", Filters::default()).await
    );
    pretty_assertions::assert_eq!(
        empty_accounts,
        *search(
            &account_repo,
            "ob",
            Filters {
                exclude_accounts_by_ids: Some(vec![account_id(&"user3")])
            }
        )
        .await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_email_success(catalog: &Catalog) {
    let account = make_test_account("wasya", "wasya@example.com", PROVIDER_PASSWORD, "wasya");

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account).await.unwrap();

    let updated_email: Email = Email::parse("wasenka@example.com").unwrap();

    account_repo
        .update_account_email(&account.id, updated_email.clone())
        .await
        .unwrap();

    let updated_account = account_repo.get_account_by_id(&account.id).await.unwrap();
    assert_eq!(updated_account.email, updated_email);

    assert_matches!(
        account_repo
            .get_account_by_name(&account.account_name)
            .await,
        Ok(res_account) if updated_account == res_account
    );

    assert_matches!(
        account_repo
            .find_account_id_by_email(&updated_email)
            .await,
        Ok(Some(account_id)) if updated_account.id == account_id
    );

    assert_matches!(
        account_repo.find_account_id_by_email(&account.email).await,
        Ok(None)
    );

    assert_matches!(
        account_repo
            .find_account_id_by_provider_identity_key(&updated_account.provider_identity_key)
            .await,
        Ok(Some(account_id)) if updated_account.id == account_id
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_email_errors(catalog: &Catalog) {
    let account_1 = make_test_account("wasya", "wasya@example.com", PROVIDER_PASSWORD, "wasya");
    let account_2 = make_test_account("petya", "petya@example.com", PROVIDER_PASSWORD, "petya");

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account_1).await.unwrap();
    account_repo.create_account(&account_2).await.unwrap();

    let updated_email: Email = Email::parse("petya@example.com").unwrap();

    assert_matches!(
        account_repo
            .update_account_email(
                &odf::AccountID::new_seeded_ed25519(b"wrong"),
                updated_email.clone(),
            )
            .await,
        Err(UpdateAccountError::NotFound(_))
    );

    assert_matches!(
        account_repo
            .update_account_email(&account_1.id, updated_email,)
            .await,
        Err(UpdateAccountError::Duplicate(AccountErrorDuplicate {
            account_field: AccountDuplicateField::Email
        }))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_account_success(catalog: &Catalog) {
    let account = make_test_account("wasya", "wasya@example.com", PROVIDER_PASSWORD, "wasya");

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account).await.unwrap();

    let updated_name = odf::AccountName::new_unchecked("wasya_superhero");
    let updated_email = Email::parse("wasya.super@example.com").unwrap();

    let updated_account = Account {
        account_name: updated_name.clone(),
        email: updated_email.clone(),
        display_name: "Wasilius".to_string(),
        avatar_url: Some("wasilius.png".to_string()),
        ..account.clone()
    };

    account_repo
        .update_account(updated_account.clone())
        .await
        .unwrap();

    let result_account = account_repo.get_account_by_id(&account.id).await.unwrap();
    assert_eq!(result_account, updated_account);

    assert_matches!(
        account_repo
            .get_account_by_name(&updated_name)
            .await,
        Ok(res_account) if updated_account == res_account
    );

    assert_matches!(
        account_repo
            .get_account_by_name(&account.account_name)
            .await,
        Err(GetAccountByNameError::NotFound(_))
    );

    assert_matches!(
        account_repo
            .find_account_id_by_email(&updated_email)
            .await,
        Ok(Some(account_id)) if updated_account.id == account_id
    );

    assert_matches!(
        account_repo.find_account_id_by_email(&account.email).await,
        Ok(None)
    );

    assert_matches!(
        account_repo
            .find_account_id_by_provider_identity_key(&updated_account.provider_identity_key)
            .await,
        Ok(Some(account_id)) if updated_account.id == account_id
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_account_not_found(catalog: &Catalog) {
    let account = make_test_account("wasya", "wasya@example.com", PROVIDER_PASSWORD, "wasya");

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account).await.unwrap();

    assert_matches!(
        account_repo
            .update_account(Account {
                id: odf::AccountID::new_seeded_ed25519(b"wrong"),
                ..account.clone()
            })
            .await,
        Err(UpdateAccountError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_account_duplicate_email(catalog: &Catalog) {
    let account_1 = make_test_account("wasya", "wasya@example.com", PROVIDER_PASSWORD, "wasya");
    let account_2 = make_test_account("petya", "petya@example.com", PROVIDER_PASSWORD, "petya");

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account_1).await.unwrap();
    account_repo.create_account(&account_2).await.unwrap();

    assert_matches!(
        account_repo
            .update_account(Account {
                email: account_2.email.clone(),
                ..account_1.clone()
            })
            .await,
        Err(UpdateAccountError::Duplicate(AccountErrorDuplicate {
            account_field: AccountDuplicateField::Email
        }))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_account_duplicate_name(catalog: &Catalog) {
    let account_1 = make_test_account("wasya", "wasya@example.com", PROVIDER_PASSWORD, "wasya");
    let account_2 = make_test_account("petya", "petya@example.com", PROVIDER_PASSWORD, "petya");

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account_1).await.unwrap();
    account_repo.create_account(&account_2).await.unwrap();

    assert_matches!(
        account_repo
            .update_account(Account {
                account_name: account_2.account_name.clone(),
                ..account_1.clone()
            })
            .await,
        Err(UpdateAccountError::Duplicate(AccountErrorDuplicate {
            account_field: AccountDuplicateField::Name
        }))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_account_duplicate_provider_identity(catalog: &Catalog) {
    let account_1 = make_test_account("wasya", "wasya@example.com", PROVIDER_PASSWORD, "wasya");
    let account_2 = make_test_account("petya", "petya@example.com", PROVIDER_PASSWORD, "petya");

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    account_repo.create_account(&account_1).await.unwrap();
    account_repo.create_account(&account_2).await.unwrap();

    assert_matches!(
        account_repo
            .update_account(Account {
                provider_identity_key: account_2.provider_identity_key.clone(),
                ..account_1.clone()
            })
            .await,
        Err(UpdateAccountError::Duplicate(AccountErrorDuplicate {
            account_field: AccountDuplicateField::ProviderIdentityKey
        }))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
