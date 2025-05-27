// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric_metadata::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_debug() {
    pretty_assertions::assert_eq!(
        r#"AccountID<Ed25519Pub>(fed017465732eb3393a007465732eb3393a004057364707000000e082e0b8bdb9d037)"#,
        format!(
            "{:?}",
            AccountID::from_did_str(
                "did:odf:fed017465732eb3393a007465732eb3393a004057364707000000e082e0b8bdb9d037"
            )
            .unwrap()
        ),
    );
    pretty_assertions::assert_eq!(
        r#"AccountID(DidPkh("eip155", "1", "0xb9c5714089478a327f09197987f16f9e5d936e8a"))"#,
        format!(
            "{:?}",
            AccountID::from_did_str("did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a")
                .unwrap()
        ),
    );
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
        r#"did:odf:fed017465732eb3393a007465732eb3393a004057364707000000e082e0b8bdb9d037"#,
        format!(
            "{}",
            AccountID::from_did_str(
                "did:odf:fed017465732eb3393a007465732eb3393a004057364707000000e082e0b8bdb9d037"
            )
            .unwrap()
        ),
    );
    pretty_assertions::assert_eq!(
        r#"did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a"#,
        format!(
            "{}",
            AccountID::from_did_str("did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a")
                .unwrap()
        ),
    );
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
        "did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a",
        AccountID::from_did_str("did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a")
            .unwrap()
            .as_did_pkh()
            .unwrap()
            .as_did_str()
            .to_string(),
    );
    pretty_assertions::assert_eq!(
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

    pretty_assertions::assert_eq!(serialized, serde_yaml::to_string(&value).unwrap());
    pretty_assertions::assert_eq!(value, serde_yaml::from_str(serialized).unwrap());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
