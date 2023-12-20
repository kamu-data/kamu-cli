// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use multiformats::*;

#[test]
fn test_debug() {
    assert_eq!(
        format!("{:?}", DidKey::new_seeded_ed25519(b"multihash")),
        "DidKey<Ed25519Pub>(z6MkggfEoTnRceFGdwjUtm1JaWeHQVKUpttKZUY6KAMaWvqq)"
    );

    assert_eq!(
        format!(
            "{:?}",
            DidKey::new_seeded_ed25519(b"multihash").as_did_str()
        ),
        "did:key:z6MkggfEoTnRceFGdwjUtm1JaWeHQVKUpttKZUY6KAMaWvqq"
    );

    assert_eq!(
        format!(
            "{:?}",
            DidKey::new_seeded_ed25519(b"multihash").as_multibase()
        ),
        "z6MkggfEoTnRceFGdwjUtm1JaWeHQVKUpttKZUY6KAMaWvqq"
    );
}

#[test]
fn test_did_string() {
    let value =
        DidKey::from_did_str("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap();

    assert_eq!(value.key_type(), Multicodec::Ed25519Pub);

    assert_eq!(
        value.as_did_str().to_stack_string(),
        "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
    );

    assert_eq!(
        value.as_did_str().encoding(Multibase::Base16).to_string(),
        "did:key:fed012e6fcce36701dc791488e0d0b1745cc1e33a4c1c9fcc41c63bd343dbbe0970e6",
    );

    assert_matches!(DidKey::from_did_str("did:key:xASDSAD"), Err(_));
}

#[test]
fn test_multibase() {
    assert_eq!(
        DidKey::new_seeded_ed25519(b"multihash")
            .as_multibase()
            .to_stack_string(),
        "z6MkggfEoTnRceFGdwjUtm1JaWeHQVKUpttKZUY6KAMaWvqq"
    );

    assert_eq!(
        DidKey::new_seeded_ed25519(b"multihash")
            .as_multibase()
            .encoding(Multibase::Base16)
            .to_stack_string(),
        "fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc"
    );
}

#[test]
fn test_eq() {
    assert_eq!(
        DidKey::from_did_str("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap(),
        DidKey::from_did_str("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    );
    assert_ne!(
        DidKey::from_did_str("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap(),
        DidKey::from_did_str("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doL").unwrap()
    );
}
