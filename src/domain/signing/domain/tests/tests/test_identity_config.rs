// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_does_not_leak_keys() {
    let cfg = kamu_signing::entities::IdentityConfig {
        ed25519_private_key: odf::metadata::PrivateKey::from_bytes(&[123; _]),
        secp256k1_private_key: crypto_eip712_utils::Secp256k1Signer::from_bytes([124; _].into())
            .unwrap(),
    };

    assert_eq!(
        indoc::indoc! {
            r#"
            IdentityConfig {
                ed25519_private_key: PrivateKey(***),
                secp256k1_private_key: Secp256k1Signer(***),
            }"#
        },
        format!("{cfg:#?}"),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
