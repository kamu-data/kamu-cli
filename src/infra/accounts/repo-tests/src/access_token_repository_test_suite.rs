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
use uuid::Uuid;

use crate::{make_test_access_token, make_test_account, GITHUB_ACCOUNT_ID_WASYA};

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_missing_access_token_not_found(catalog: &Catalog) {
    let access_token_repo = catalog.get_one::<dyn AccessTokenRepository>().unwrap();
    let access_token_result = access_token_repo.get_token_by_id(&Uuid::new_v4()).await;
    assert_matches!(access_token_result, Err(GetAccessTokenError::NotFound(_)));
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_locate_access_token(catalog: &Catalog) {
    let access_token = make_test_access_token("foo", None, "wasya");
    let account = make_test_account(
        "wasya",
        kamu_adapter_oauth::PROVIDER_GITHUB,
        GITHUB_ACCOUNT_ID_WASYA,
    );

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let access_token_repo = catalog.get_one::<dyn AccessTokenRepository>().unwrap();

    account_repo.create_account(&account).await.unwrap();
    access_token_repo
        .create_access_token(&access_token)
        .await
        .unwrap();

    let db_access_token = access_token_repo
        .get_token_by_id(&access_token.id)
        .await
        .unwrap();

    assert_eq!(db_access_token, access_token);
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_and_locate_multiple_access_tokens(catalog: &Catalog) {
    let foo_access_token = make_test_access_token("foo", None, "wasya");
    let bar_access_token = make_test_access_token("bar", None, "wasya");
    let account = make_test_account(
        "wasya",
        kamu_adapter_oauth::PROVIDER_GITHUB,
        GITHUB_ACCOUNT_ID_WASYA,
    );

    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let access_token_repo = catalog.get_one::<dyn AccessTokenRepository>().unwrap();

    account_repo.create_account(&account).await.unwrap();
    access_token_repo
        .create_access_token(&foo_access_token)
        .await
        .unwrap();
    access_token_repo
        .create_access_token(&bar_access_token)
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

    let mut db_access_tokens = access_token_repo.get_access_tokens().await.unwrap();

    db_access_tokens.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    assert_eq!(db_access_tokens, vec![foo_access_token, bar_access_token]);
}
