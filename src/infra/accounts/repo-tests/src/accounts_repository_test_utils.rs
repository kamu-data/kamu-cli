// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use argon2::Argon2;
use chrono::{SubsecRound, Utc};
use email_utils::Email;
use kamu_accounts::{Account, AccountType};
use password_hash::rand_core::OsRng;
use password_hash::{PasswordHash, PasswordHasher, SaltString};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_test_account(
    name: &str,
    email: &str,
    provider: &str,
    provider_identity_key: &str,
) -> Account {
    Account {
        id: odf::AccountID::new_seeded_ed25519(name.as_bytes()),
        account_name: odf::AccountName::new_unchecked(name),
        email: Email::parse(email).unwrap(),
        display_name: String::from(name),
        account_type: AccountType::User,
        avatar_url: None,
        registered_at: Utc::now().round_subsecs(6),
        is_admin: false,
        provider: String::from(provider),
        provider_identity_key: String::from(provider_identity_key),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn generate_salt() -> SaltString {
    SaltString::generate(&mut OsRng)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_password_hash<'a>(raw_password: &str, salt: &'a SaltString) -> PasswordHash<'a> {
    let argon2 = Argon2::default();
    argon2.hash_password(raw_password.as_bytes(), salt).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
