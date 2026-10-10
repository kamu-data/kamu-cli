// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::TenancyConfig;
use kamu_accounts::DEFAULT_ACCOUNT_NAME_STR;
use kamu_cli::services::accounts::AccountService;
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_single_tenant_current_account_is_default_account() {
    let indication =
        AccountService::current_account_indication(None, TenancyConfig::SingleTenant).unwrap();

    assert_eq!(indication.account_name.as_str(), DEFAULT_ACCOUNT_NAME_STR);
    assert_eq!(indication.user_name, DEFAULT_ACCOUNT_NAME_STR);
    assert!(!indication.is_explicit());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_multi_tenant_current_account_is_os_user() {
    let indication =
        AccountService::current_account_indication(None, TenancyConfig::MultiTenant).unwrap();

    assert_eq!(
        indication.account_name.as_str(),
        AccountService::default_account_name(TenancyConfig::MultiTenant)
    );
    assert_eq!(
        indication.user_name,
        AccountService::default_user_name(TenancyConfig::MultiTenant)
    );
    assert!(!indication.is_explicit());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_multi_tenant_current_account_follows_account_arg() {
    let indication = AccountService::current_account_indication(
        Some(String::from("alice")),
        TenancyConfig::MultiTenant,
    )
    .unwrap();

    assert_eq!(indication.account_name.as_str(), "alice");
    assert_eq!(indication.user_name, "alice");
    assert!(indication.is_explicit());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
