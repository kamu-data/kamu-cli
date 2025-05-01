// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::{SubsecRound, Utc};
use database_common::PaginationOpts;
use dill::Catalog;
use kamu_accounts::*;
use uuid::Uuid;

use crate::{make_test_access_token, make_test_account, GITHUB_ACCOUNT_ID_WASYA};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_missing_access_token_not_found(catalog: &Catalog) {
    let access_token_repo = catalog.get_one::<dyn AccessTokenRepository>().unwrap();
    let access_token_result = access_token_repo.get_token_by_id(&Uuid::new_v4()).await;
    assert_matches!(access_token_result, Err(GetAccessTokenError::NotFound(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_locate_access_token(catalog: &Catalog) {
    let access_token = make_test_access_token("foo", None, "wasya");
    let account = make_test_account(
        "wasya",
        "wasya@example.com",
        kamu_adapter_oauth::PROVIDER_GITHUB,
        GITHUB_ACCOUNT_ID_WASYA,
        None,
    );

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let access_token_repo = catalog.get_one::<dyn AccessTokenRepository>().unwrap();

    account_repo.save_account(&account).await.unwrap();
    access_token_repo
        .save_access_token(&access_token)
        .await
        .unwrap();

    let db_access_token = access_token_repo
        .get_token_by_id(&access_token.id)
        .await
        .unwrap();

    assert_eq!(db_access_token, access_token);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_locate_multiple_access_tokens(catalog: &Catalog) {
    let foo_access_token = make_test_access_token("foo", None, "wasya");
    let bar_access_token = make_test_access_token("bar", None, "wasya");
    let account = make_test_account(
        "wasya",
        "wasya@example.com",
        kamu_adapter_oauth::PROVIDER_GITHUB,
        GITHUB_ACCOUNT_ID_WASYA,
        None,
    );

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let access_token_repo = catalog.get_one::<dyn AccessTokenRepository>().unwrap();

    account_repo.save_account(&account).await.unwrap();
    access_token_repo
        .save_access_token(&foo_access_token)
        .await
        .unwrap();
    access_token_repo
        .save_access_token(&bar_access_token)
        .await
        .unwrap();

    let db_foo_access_token = access_token_repo
        .get_token_by_id(&foo_access_token.id)
        .await
        .unwrap();

    assert_eq!(db_foo_access_token, foo_access_token);

    let db_bar_access_token = access_token_repo
        .get_token_by_id(&bar_access_token.id)
        .await
        .unwrap();

    assert_eq!(db_bar_access_token, bar_access_token);

    let mut db_access_tokens = access_token_repo
        .get_access_tokens_by_account_id(
            &account.id,
            &PaginationOpts {
                limit: 10,
                offset: 0,
            },
        )
        .await
        .unwrap();

    db_access_tokens.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    assert_eq!(db_access_tokens, vec![foo_access_token, bar_access_token]);

    let db_access_tokens_count = access_token_repo
        .get_access_tokens_count_by_account_id(&account.id)
        .await
        .unwrap();

    assert_eq!(db_access_tokens_count, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_mark_existing_access_token_revoked(catalog: &Catalog) {
    let access_token = make_test_access_token("foo", None, "wasya");
    let account = make_test_account(
        "wasya",
        "wasya@example.com",
        kamu_adapter_oauth::PROVIDER_GITHUB,
        GITHUB_ACCOUNT_ID_WASYA,
        None,
    );

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let access_token_repo = catalog.get_one::<dyn AccessTokenRepository>().unwrap();

    account_repo.save_account(&account).await.unwrap();
    access_token_repo
        .save_access_token(&access_token)
        .await
        .unwrap();

    let db_access_token = access_token_repo
        .get_token_by_id(&access_token.id)
        .await
        .unwrap();

    assert_eq!(db_access_token.revoked_at, None);

    let revoke_time = Utc::now().round_subsecs(6);
    let revoke_result = access_token_repo
        .mark_revoked(&access_token.id, revoke_time)
        .await;

    assert!(revoke_result.is_ok());

    let db_access_token = access_token_repo
        .get_token_by_id(&access_token.id)
        .await
        .unwrap();
    assert_eq!(db_access_token.revoked_at, Some(revoke_time));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_duplicate_active_access_token(catalog: &Catalog) {
    let access_token = make_test_access_token("foo", None, "wasya");
    let access_token_duplicate = make_test_access_token("foo", None, "wasya");

    let account = make_test_account(
        "wasya",
        "wasya@example.com",
        kamu_adapter_oauth::PROVIDER_GITHUB,
        GITHUB_ACCOUNT_ID_WASYA,
        None,
    );

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let access_token_repo = catalog.get_one::<dyn AccessTokenRepository>().unwrap();

    account_repo.save_account(&account).await.unwrap();
    access_token_repo
        .save_access_token(&access_token)
        .await
        .unwrap();

    let db_access_token = access_token_repo
        .get_token_by_id(&access_token.id)
        .await
        .unwrap();

    assert_eq!(db_access_token.revoked_at, None);

    let revoke_time = Utc::now().round_subsecs(6);
    let revoke_result = access_token_repo
        .mark_revoked(&access_token.id, revoke_time)
        .await;

    assert!(revoke_result.is_ok());

    let create_result = access_token_repo
        .save_access_token(&access_token_duplicate)
        .await;
    assert!(create_result.is_ok());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_duplicate_access_token_error(catalog: &Catalog) {
    let access_token = make_test_access_token("foo", None, "wasya");
    let access_token_duplicate = make_test_access_token("foo", None, "wasya");

    let account = make_test_account(
        "wasya",
        "wasya@example.com",
        kamu_adapter_oauth::PROVIDER_GITHUB,
        GITHUB_ACCOUNT_ID_WASYA,
        None,
    );

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let access_token_repo = catalog.get_one::<dyn AccessTokenRepository>().unwrap();

    account_repo.save_account(&account).await.unwrap();
    access_token_repo
        .save_access_token(&access_token)
        .await
        .unwrap();

    let db_access_token = access_token_repo
        .get_token_by_id(&access_token.id)
        .await
        .unwrap();

    assert_eq!(db_access_token.revoked_at, None);
    assert_matches!(
        access_token_repo
            .save_access_token(&access_token_duplicate)
            .await,
        Err(CreateAccessTokenError::Duplicate(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_mark_non_existing_access_token_revoked(catalog: &Catalog) {
    let access_token = make_test_access_token("foo", None, "wasya");

    let access_token_repo = catalog.get_one::<dyn AccessTokenRepository>().unwrap();

    let revoke_time = Utc::now();
    let revoke_result = access_token_repo
        .mark_revoked(&access_token.id, revoke_time)
        .await;
    assert_matches!(revoke_result, Err(RevokeTokenError::NotFound(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_account_by_active_token_id(catalog: &Catalog) {
    let access_token = make_test_access_token("foo", None, "wasya");
    let fake_access_token = make_test_access_token("bar", None, "wasya");
    let account = make_test_account(
        "wasya",
        "wasya@example.com",
        kamu_adapter_oauth::PROVIDER_GITHUB,
        GITHUB_ACCOUNT_ID_WASYA,
        None,
    );

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let access_token_repo = catalog.get_one::<dyn AccessTokenRepository>().unwrap();

    account_repo.save_account(&account).await.unwrap();
    access_token_repo
        .save_access_token(&access_token)
        .await
        .unwrap();

    let db_account = access_token_repo
        .find_account_by_active_token_id(&access_token.id, access_token.token_hash)
        .await
        .unwrap();
    assert_eq!(db_account, account);

    let db_account_res = access_token_repo
        .find_account_by_active_token_id(&access_token.id, fake_access_token.token_hash)
        .await;
    assert_matches!(
        db_account_res,
        Err(FindAccountByTokenError::InvalidTokenHash)
    );

    let revoke_time = Utc::now().round_subsecs(6);
    let revoke_result = access_token_repo
        .mark_revoked(&access_token.id, revoke_time)
        .await;
    assert!(revoke_result.is_ok());

    let db_account_res = access_token_repo
        .find_account_by_active_token_id(&access_token.id, access_token.token_hash)
        .await;
    assert_matches!(db_account_res, Err(FindAccountByTokenError::NotFound(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
