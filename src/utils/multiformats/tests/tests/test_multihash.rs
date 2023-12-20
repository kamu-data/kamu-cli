// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use multiformats::*;

#[test]
fn test_debug() {
    assert_eq!(
        format!("{:?}", Multihash::from_digest_sha3_256(b"multihash")),
        "Multihash<Sha3_256>(f162008c3792b2a4deed1bd7ea2328fb5de5531eccf0fbfa04a7d800cdc267137c635)"
    );
}

#[test]
fn test_as_multibase() {
    assert_eq!(
        Multihash::from_digest_sha3_256(b"multihash")
            .as_multibase()
            .to_stack_string(),
        "f162008c3792b2a4deed1bd7ea2328fb5de5531eccf0fbfa04a7d800cdc267137c635"
    );

    assert_eq!(
        Multihash::from_digest_sha3_256(b"multihash")
            .as_multibase()
            .encoding(Multibase::Base58Btc)
            .to_stack_string(),
        "zW1a3CNT52HXiJNniLkWMeev3CPRy9QiNRMWGyTrVNg4hY8"
    );

    assert_eq!(
        Multihash::from_digest_sha3_256(b"multihash")
            .as_multibase()
            .short()
            .to_stack_string(),
        "7137c635"
    );
}

#[test]
fn test_decode_encode() {
    let value = Multihash::from_multibase(
        "f12209cbc07c3f991725836a3aa2a581ca2029198aa420b9d99bc0e131d9f3e2cbe47",
    )
    .unwrap();

    assert_eq!(value.code(), Multicodec::Sha2_256);

    assert_eq!(
        value
            .as_multibase()
            .encoding(Multibase::Base58Btc)
            .to_stack_string(),
        "zQmYtUc4iTCbbfVSDNKvtQqrfyezPPnFvE33wFmutw9PBBk",
    );
}

#[test]
fn test_eq() {
    assert_eq!(
        Multihash::from_multibase(
            "f162008c3792b2a4deed1bd7ea2328fb5de5531eccf0fbfa04a7d800cdc267137c635"
        )
        .unwrap(),
        Multihash::from_multibase(
            "F162008C3792B2A4DEED1BD7EA2328FB5DE5531ECCF0FBFA04A7D800CDC267137C635"
        )
        .unwrap()
    );
    assert_ne!(
        Multihash::from_multibase(
            "f162008c3792b2a4deed1bd7ea2328fb5de5531eccf0fbfa04a7d800cdc267137c635"
        )
        .unwrap(),
        Multihash::from_multibase(
            "f12209cbc07c3f991725836a3aa2a581ca2029198aa420b9d99bc0e131d9f3e2cbe47"
        )
        .unwrap()
    );
}
