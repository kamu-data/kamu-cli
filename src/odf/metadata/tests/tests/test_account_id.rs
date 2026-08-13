// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric_metadata::auth::AccountID;
use opendatafabric_metadata::formats::*;
use pretty_assertions::{assert_eq, assert_ne};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_debug() {
    assert_eq!(
        r#"AccountID<Ed25519Pub>(fed017465732eb3393a007465732eb3393a004057364707000000e082e0b8bdb9d037)"#,
        format!(
            "{:?}",
            AccountID::from_did_str(
                "did:odf:fed017465732eb3393a007465732eb3393a004057364707000000e082e0b8bdb9d037"
            )
            .unwrap()
        ),
    );
    assert_eq!(
        r#"AccountID(DidPkh("eip155", "1", "0xb9c5714089478a327f09197987f16f9e5d936e8a"))"#,
        format!(
            "{:?}",
            AccountID::from_did_str("did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a")
                .unwrap()
        ),
    );
    assert_eq!(
        r#"AccountID(DidPkh("solana", "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZ", "CKg5d12Jhpej1JqtmxLJgaFqqeYjxgPqToJ4LBdvG9Ev"))"#,
        format!(
            "{:?}",
            AccountID::from_did_str(
                 r#"did:pkh:solana:4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZ:CKg5d12Jhpej1JqtmxLJgaFqqeYjxgPqToJ4LBdvG9Ev"#
            )
            .unwrap()
        ),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_display() {
    assert_eq!(
        r#"did:odf:fed017465732eb3393a007465732eb3393a004057364707000000e082e0b8bdb9d037"#,
        format!(
            "{}",
            AccountID::from_did_str(
                "did:odf:fed017465732eb3393a007465732eb3393a004057364707000000e082e0b8bdb9d037"
            )
            .unwrap()
        ),
    );
    assert_eq!(
        r#"did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a"#,
        format!(
            "{}",
            AccountID::from_did_str("did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a")
                .unwrap()
        ),
    );
    assert_eq!(
        r#"did:pkh:solana:4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZ:CKg5d12Jhpej1JqtmxLJgaFqqeYjxgPqToJ4LBdvG9Ev"#,
        format!(
            "{}",
            AccountID::from_did_str(
                 r#"did:pkh:solana:4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZ:CKg5d12Jhpej1JqtmxLJgaFqqeYjxgPqToJ4LBdvG9Ev"#
            )
            .unwrap()
        ),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_did_string() {
    assert_eq!(
        "did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a",
        AccountID::from_did_str("did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a")
            .unwrap()
            .as_did_pkh()
            .unwrap()
            .as_did_str()
            .to_string(),
    );
    assert_eq!(
        r#"did:pkh:solana:4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZ:CKg5d12Jhpej1JqtmxLJgaFqqeYjxgPqToJ4LBdvG9Ev"#,
        AccountID::from_did_str(r#"did:pkh:solana:4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZ:CKg5d12Jhpej1JqtmxLJgaFqqeYjxgPqToJ4LBdvG9Ev"#)
            .unwrap()
            .as_did_pkh()
            .unwrap()
            .as_did_str()
            .to_string(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde() {
    // NOTE: serde_yaml is used since it's already in dependencies

    #[derive(::serde::Serialize, ::serde::Deserialize, Debug, PartialEq)]
    struct TestStruct {
        account_id: AccountID,
    }

    {
        let value = TestStruct {
            account_id: AccountID::from_did_str(
                "did:odf:fed017465732eb3393a007465732eb3393a004057364707000000e082e0b8bdb9d037",
            )
            .unwrap(),
        };
        let serialized = indoc::indoc!(
            r#"
            account_id: did:odf:fed017465732eb3393a007465732eb3393a004057364707000000e082e0b8bdb9d037
            "#
        );

        assert_eq!(serialized, serde_yaml::to_string(&value).unwrap());
        assert_eq!(value, serde_yaml::from_str(serialized).unwrap());
    }
    {
        let value = TestStruct {
            account_id: AccountID::from_did_str(
                "did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a",
            )
            .unwrap(),
        };
        let serialized = indoc::indoc!(
            r#"
            account_id: did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a
            "#
        );

        assert_eq!(serialized, serde_yaml::to_string(&value).unwrap());
        assert_eq!(value, serde_yaml::from_str(serialized).unwrap());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_stack_string() {
    {
        let account_did_odf = AccountID::from_did_str(
            "did:odf:fed017465732eb3393a007465732eb3393a004057364707000000e082e0b8bdb9d037",
        )
        .unwrap();
        let stack_account_did_odf = account_did_odf.as_stack_string();

        assert_eq!(
            "did:odf:fed017465732eb3393a007465732eb3393a004057364707000000e082e0b8bdb9d037",
            stack_account_did_odf.as_str(),
        );
        assert_eq!(77, stack_account_did_odf.len());
    }

    {
        let account_did_pkh =
            AccountID::from_did_str("did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a")
                .unwrap();
        let stack_account_did_pkh = account_did_pkh.as_stack_string();

        assert_eq!(
            "did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a",
            stack_account_did_pkh.as_str(),
        );
        assert_eq!(59, stack_account_did_pkh.len());
    }

    {
        let account_did_pkh =
            AccountID::from_did_str(
                r#"did:pkh:chainstd:8c3444cf8970a9e41a706fab93e7a6c4:6d9b0b4b9994e8a6afbd3dc3ed983cd51c755afb27cd1dc7825ef59c134a39f7"#
            ).unwrap();
        let stack_account_did_pkh = account_did_pkh.as_stack_string();

        assert_eq!(
            r#"did:pkh:chainstd:8c3444cf8970a9e41a706fab93e7a6c4:6d9b0b4b9994e8a6afbd3dc3ed983cd51c755afb27cd1dc7825ef59c134a39f7"#,
            stack_account_did_pkh.as_str(),
        );
        assert_eq!(MAX_DID_PKH_STRING_REPR_LEN, stack_account_did_pkh.len());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_non_deterministic() {
    let (key1, id1) = AccountID::new_generated_ed25519();
    let (key2, id2) = AccountID::new_generated_ed25519();

    assert_ne!(key1, key2);
    assert_ne!(id1, id2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_key_matches_id() {
    let (key, id) = AccountID::new_generated_ed25519();

    let private_key = PrivateKey::from(key);

    assert_eq!(id, AccountID::from_signing_key(&private_key));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
