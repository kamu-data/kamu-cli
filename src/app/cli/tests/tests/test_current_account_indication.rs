// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu::domain::TenancyConfig;
use kamu_accounts::DEFAULT_ACCOUNT_NAME_STR;
use kamu_cli::services::accounts::{
    AccountService,
    CurrentAccountIndicationError,
    OsUserAccountNameError,
};
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
    let result = AccountService::current_account_indication(None, TenancyConfig::MultiTenant);

    // Fails exactly when this machine's OS user name has no usable account name
    match AccountService::account_name_from_os_user(whoami::username().ok().as_deref()) {
        Ok(os_account_name) => {
            let indication = result.unwrap();
            assert_eq!(indication.account_name, os_account_name);
            assert_eq!(
                indication.user_name,
                AccountService::default_user_name(TenancyConfig::MultiTenant)
            );
            assert!(!indication.is_explicit());
        }
        Err(_) => assert_matches!(
            result,
            Err(CurrentAccountIndicationError::OsUserAccountName(_))
        ),
    }
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

#[test]
fn test_explicit_account_valid_name_is_taken_verbatim() {
    let indication = AccountService::current_account_indication(
        Some(String::from("John-Doe")),
        TenancyConfig::MultiTenant,
    )
    .unwrap();

    assert_eq!(indication.account_name.as_str(), "John-Doe");
    assert!(indication.is_explicit());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_explicit_account_with_invalid_name_is_rejected() {
    for account in ["john_doe", "_www", "John Doe", "user$", "Jürgen", ""] {
        let result = AccountService::current_account_indication(
            Some(String::from(account)),
            TenancyConfig::MultiTenant,
        );

        assert_matches!(
            result,
            Err(CurrentAccountIndicationError::InvalidAccountName(_)),
            "{account:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_os_user_linux_names() {
    for (os_user, expected) in [
        ("john_doe", "john-doe"),
        ("svc__build", "svc-build"),
        ("user$", "user"),
        ("john@corp.example", "john-corp.example"),
        ("CORP\\john", "CORP-john"),
        ("john.doe", "john.doe"),
    ] {
        assert_eq!(
            AccountService::account_name_from_os_user(Some(os_user))
                .unwrap()
                .as_str(),
            expected,
            "OS user {os_user:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_os_user_macos_names() {
    for (os_user, expected) in [
        ("_www", "www"),
        ("_service", "service"),
        ("_spotlight_daemon", "spotlight-daemon"),
    ] {
        assert_eq!(
            AccountService::account_name_from_os_user(Some(os_user))
                .unwrap()
                .as_str(),
            expected,
            "OS user {os_user:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_os_user_windows_names() {
    for (os_user, expected) in [
        ("John Doe", "John-Doe"),
        ("DESKTOP-ABC$", "DESKTOP-ABC"),
        ("Jürgen", "Jrgen"),
        ("j.doe.", "j.doe"),
        ("a._b", "a-b"),
        (" lead", "lead"),
        ("o'brien", "o-brien"),
    ] {
        assert_eq!(
            AccountService::account_name_from_os_user(Some(os_user))
                .unwrap()
                .as_str(),
            expected,
            "OS user {os_user:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_os_user_unavailable_is_an_error() {
    assert_matches!(
        AccountService::account_name_from_os_user(None),
        Err(OsUserAccountNameError::Unavailable)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_os_user_without_valid_characters_is_an_error() {
    // Names in non-Latin scripts exist on every OS
    for os_user in ["", "__", "张伟", "Ольга", "$"] {
        assert_matches!(
            AccountService::account_name_from_os_user(Some(os_user)),
            Err(OsUserAccountNameError::NoValidCharacters { os_user_name })
                if os_user_name == os_user
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_os_user_valid_names_are_unchanged() {
    for os_user in ["alice", "JohnDoe", "a-b.c-d", "123", "kamu", "x"] {
        assert_eq!(
            AccountService::account_name_from_os_user(Some(os_user))
                .unwrap()
                .as_str(),
            os_user
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_os_user_name_always_parses() {
    // Every string of up to three characters, so each pair of character
    // classes appears in each position. The normalizer panics, naming the
    // input, if its result does not parse; it may fail only for names without
    // a single ASCII letter or digit
    let alphabet = ['a', 'Z', '0', '_', '-', '.', '$', '@', '\\', ' ', 'ü'];

    let mut os_users = vec![String::new()];
    for _ in 0..3 {
        os_users = os_users
            .iter()
            .flat_map(|prefix| alphabet.iter().map(move |c| format!("{prefix}{c}")))
            .collect();

        for os_user in &os_users {
            let result = AccountService::account_name_from_os_user(Some(os_user));

            assert_eq!(
                result.is_ok(),
                os_user.chars().any(|c| c.is_ascii_alphanumeric()),
                "OS user {os_user:?} gave {result:?}"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
