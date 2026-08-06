// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::AccountIdentityGenerator;
use kamu_accounts_services::{AccountIdentityGeneratorDefault, AccountIdentityGeneratorSeeded};
use pretty_assertions::{assert_eq, assert_ne};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountIdentityGeneratorSeeded
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_seeded_determinism() {
    let name = odf::AccountName::new_unchecked("alice");

    let (key1, id1) = AccountIdentityGeneratorSeeded.generate_ed25519(&name);
    let (key2, id2) = AccountIdentityGeneratorSeeded.generate_ed25519(&name);

    assert_eq!(key1, key2);
    assert_eq!(id1, id2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_seeded_different_names_different_ids() {
    let (key_alice, id_alice) =
        AccountIdentityGeneratorSeeded.generate_ed25519(&odf::AccountName::new_unchecked("alice"));
    let (key_bob, id_bob) =
        AccountIdentityGeneratorSeeded.generate_ed25519(&odf::AccountName::new_unchecked("bob"));

    assert_ne!(key_alice, key_bob);
    assert_ne!(id_alice, id_bob);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_seeded_key_matches_id() {
    let (key, id) =
        AccountIdentityGeneratorSeeded.generate_ed25519(&odf::AccountName::new_unchecked("alice"));

    assert_key_matches_id(&key, &id);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_seeded_aligned_with_testing_shortcut() {
    let (_, id) =
        AccountIdentityGeneratorSeeded.generate_ed25519(&odf::AccountName::new_unchecked("alice"));

    assert_eq!(id, odf::metadata::testing::account_id(&"alice"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_seeded_padding_short_name() {
    let name = odf::AccountName::new_unchecked("a");

    let (key1, id1) = AccountIdentityGeneratorSeeded.generate_ed25519(&name);
    let (key2, id2) = AccountIdentityGeneratorSeeded.generate_ed25519(&name);

    assert_eq!(key1, key2);
    assert_eq!(id1, id2);
    assert_key_matches_id(&key1, &id1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_seeded_long_name() {
    let name_1 = odf::AccountName::new_unchecked("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000");
    let name_2 = odf::AccountName::new_unchecked("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1111");
    //                                            |                              |
    //                                            0                             31

    let (key1, id1) = AccountIdentityGeneratorSeeded.generate_ed25519(&name_1);
    let (key2, id2) = AccountIdentityGeneratorSeeded.generate_ed25519(&name_2);

    assert_eq!(key1, key2);
    assert_eq!(id1, id2);
    assert_key_matches_id(&key1, &id1);
    assert_key_matches_id(&key2, &id2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountIdentityGeneratorDefault
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_default_non_deterministic() {
    let name = odf::AccountName::new_unchecked("alice");

    let (key1, id1) = AccountIdentityGeneratorDefault.generate_ed25519(&name);
    let (key2, id2) = AccountIdentityGeneratorDefault.generate_ed25519(&name);

    assert_ne!(key1, key2);
    assert_ne!(id1, id2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_key_matches_id() {
    let (key, id) =
        AccountIdentityGeneratorDefault.generate_ed25519(&odf::AccountName::new_unchecked("alice"));

    assert_key_matches_id(&key, &id);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Common
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_key_matches_id(key: &odf::metadata::SigningKey, id: &odf::AccountID) {
    let private_key = odf::metadata::PrivateKey::from(key.clone());

    assert_eq!(*id, odf::AccountID::from_signing_key(&private_key));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
