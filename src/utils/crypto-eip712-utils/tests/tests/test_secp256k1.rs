// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use alloy::primitives::b256;
use crypto_eip712_utils::{Secp256k1Signature, Secp256k1Signer};
use pretty_assertions::assert_eq;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_signer_does_not_leak_keys() -> eyre::Result<()> {
    // Label: "kamu-attester"
    //        privateKey = uint256(keccak256(abi.encodePacked(label)))
    let private_key = b256!("0x42f3bebeb03afa3f14440c6837fa653a84e76bb74d62856227a97f3ee487b601");
    let signer = Secp256k1Signer::from_bytes(zeroize::Zeroizing::new(*private_key))?;

    assert_eq!("Secp256k1Signer(***)", format!("{:?}", signer),);

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "serde")]
#[test]
fn test_signer_serde() -> eyre::Result<()> {
    use std::str::FromStr;

    // Label: "kamu-attester"
    //        privateKey = uint256(keccak256(abi.encodePacked(label)))
    let private_key = "0x42f3bebeb03afa3f14440c6837fa653a84e76bb74d62856227a97f3ee487b601";
    let proof_hash = b256!("0x0e270ea3c4029b14e3ca5f8c38ada678d9076889d3f3f1b7a8a6a5596124175c");
    let proof_signature = "0x2c135e2f43fe14d8f5027bf91e4bb4207f1aa9edcfc74031f1e4d6945293ca9d55216a0fe19964d2b6420b6c2f4bfd759db55f9bb95850d17f9e0ab8569f5c3f1b";

    let expected_deserialized = Secp256k1Signer::from_str(private_key)?;
    let actual_deserialized: Secp256k1Signer = serde_json::from_value(json!(private_key))?;

    assert_eq!(expected_deserialized, actual_deserialized);

    let signer_1 = expected_deserialized;
    let signer_2 = actual_deserialized;

    let expected_signature = proof_signature;
    let actual_signature_1 = signer_1.sign_prehash(&proof_hash)?;
    let actual_signature_2 = signer_2.sign_prehash(&proof_hash)?;

    assert_eq!(expected_signature, actual_signature_1.to_string());
    assert_eq!(expected_signature, actual_signature_2.to_string());
    assert_eq!(actual_signature_1, actual_signature_2); //

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "serde")]
#[test]
fn test_signature_serde() -> eyre::Result<()> {
    let expected_signature = "0x2c135e2f43fe14d8f5027bf91e4bb4207f1aa9edcfc74031f1e4d6945293ca9d55216a0fe19964d2b6420b6c2f4bfd759db55f9bb95850d17f9e0ab8569f5c3f1b";
    let actual_signature: Secp256k1Signature = expected_signature.parse()?;

    assert_eq!(expected_signature, actual_signature.to_string());
    assert_eq!(
        expected_signature,
        actual_signature.as_encoded_stack_buf().as_str()
    );

    assert_eq!(
        indoc::formatdoc!("\"{expected_signature}\""),
        serde_json::to_string(&actual_signature)?
    );

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
