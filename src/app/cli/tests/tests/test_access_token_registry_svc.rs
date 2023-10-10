// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

use std::assert_matches::assert_matches;
use std::sync::Arc;

use kamu::domain::auth::OdfServerAccessTokenResolver;
use kamu::domain::CurrentAccountSubject;
use kamu_cli::odf_server::*;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

const TEST_FRONTEND_URL: &str = "http://platform.example.com";
const TEST_BACKEND_URL: &str = "http://api.example.com";
const TEST_DATASET_URL: &str = "http://api.example.com/foo";

const TEST_FRONTEND_URL_2: &str = "http://platform2.example.com";
const TEST_BACKEND_URL_2: &str = "http://api2.example.com";
const TEST_DATASET_URL_2: &str = "http://api2.example.com/foo";

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_no_token_by_default() {
    let svc = AccessTokenRegistryService::new(
        Arc::new(DummyAccessTokenStore::new()),
        Arc::new(CurrentAccountSubject::new_test()),
    );

    let dataset_url = Url::parse(TEST_DATASET_URL).unwrap();
    let maybe_token = svc.resolve_odf_dataset_access_token(&dataset_url);

    assert_matches!(maybe_token, None);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_valid_logins() {
    let svc = AccessTokenRegistryService::new(
        Arc::new(DummyAccessTokenStore::new()),
        Arc::new(CurrentAccountSubject::new_test()),
    );

    svc.save_access_token(
        AccessTokenStoreScope::User,
        &Url::parse(TEST_FRONTEND_URL).unwrap(),
        &Url::parse(TEST_BACKEND_URL).unwrap(),
        "random-token-user".to_string(),
    )
    .unwrap();

    svc.save_access_token(
        AccessTokenStoreScope::Workspace,
        &Url::parse(TEST_FRONTEND_URL_2).unwrap(),
        &Url::parse(TEST_BACKEND_URL_2).unwrap(),
        "random-token-workspace".to_string(),
    )
    .unwrap();

    let dataset_url = Url::parse(TEST_DATASET_URL).unwrap();
    let maybe_token = svc.resolve_odf_dataset_access_token(&dataset_url);

    assert_matches!(maybe_token, Some(token) if token.as_str() == "random-token-user");

    let dataset_url = Url::parse(TEST_DATASET_URL_2).unwrap();
    let maybe_token = svc.resolve_odf_dataset_access_token(&dataset_url);

    assert_matches!(maybe_token, Some(token) if token.as_str() == "random-token-workspace");
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_workspace_token_dominates() {
    let svc = AccessTokenRegistryService::new(
        Arc::new(DummyAccessTokenStore::new()),
        Arc::new(CurrentAccountSubject::new_test()),
    );

    svc.save_access_token(
        AccessTokenStoreScope::Workspace,
        &Url::parse(TEST_FRONTEND_URL).unwrap(),
        &Url::parse(TEST_BACKEND_URL).unwrap(),
        "random-token-workspace".to_string(),
    )
    .unwrap();

    svc.save_access_token(
        AccessTokenStoreScope::User,
        &Url::parse(TEST_FRONTEND_URL).unwrap(),
        &Url::parse(TEST_BACKEND_URL).unwrap(),
        "random-token-user".to_string(),
    )
    .unwrap();

    let dataset_url = Url::parse(TEST_DATASET_URL).unwrap();
    let maybe_token = svc.resolve_odf_dataset_access_token(&dataset_url);

    assert_matches!(maybe_token, Some(token) if token.as_str() == "random-token-workspace");
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_no_token_after_logout() {
    let svc = AccessTokenRegistryService::new(
        Arc::new(DummyAccessTokenStore::new()),
        Arc::new(CurrentAccountSubject::new_test()),
    );

    svc.save_access_token(
        AccessTokenStoreScope::Workspace,
        &Url::parse(TEST_FRONTEND_URL).unwrap(),
        &Url::parse(TEST_BACKEND_URL).unwrap(),
        "random-token-workspace".to_string(),
    )
    .unwrap();

    svc.drop_access_token(
        AccessTokenStoreScope::Workspace,
        &Url::parse(TEST_FRONTEND_URL).unwrap(),
    )
    .unwrap();

    let dataset_url = Url::parse(TEST_DATASET_URL).unwrap();
    let maybe_token = svc.resolve_odf_dataset_access_token(&dataset_url);

    assert_matches!(maybe_token, None);
}

/////////////////////////////////////////////////////////////////////////////////////////

struct DummyAccessTokenStore {}

impl DummyAccessTokenStore {
    pub fn new() -> Self {
        Self {}
    }
}

impl AccessTokenStore for DummyAccessTokenStore {
    fn read_access_tokens_registry(
        &self,
        _scope: AccessTokenStoreScope,
    ) -> Result<OdfServerAccessTokenRegistry, internal_error::InternalError> {
        Ok(vec![])
    }

    fn write_access_tokens_registry(
        &self,
        _scope: AccessTokenStoreScope,
        _registry: &OdfServerAccessTokenRegistry,
    ) -> Result<(), internal_error::InternalError> {
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
