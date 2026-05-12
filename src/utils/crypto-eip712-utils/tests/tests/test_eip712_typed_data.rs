// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crypto_eip712_utils::{Eip712TypedData, b256, sign_prefixed};
use pretty_assertions::assert_eq;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_molecule_provided_test_data() -> eyre::Result<()> {
    // Adapted from:
    // I. EIP-712 LinkDidRequest — Handoff for Kamu
    // https://github.com/moleculeprotocol/onchainlabs/blob/main/docs/identity/kamu-eip712-linkdidrequest-handoff.md
    // II. BE DID-Linking — EIP-712 Fixture Vectors
    // https://github.com/moleculeprotocol/onchainlabs/blob/main/docs/identity/be-did-linking-eip712-fixtures.md

    // I. EIP-712 LinkDidRequest — Handoff for Kamu
    let typed_data = Eip712TypedData::from_json(json!({
        "domain": {
            "name": "MoleculeOclDidRegistry",
            "version": "1",
            "chainId": 8453,
            "verifyingContract": "0x00000000000000000000000000000000DeaDBeeF"
        },
        "types": {
            "LinkDidRequest": [
                {
                    "name": "oclId",
                    "type": "bytes32"
                },
                {
                    "name": "provider",
                    "type": "bytes32"
                },
                {
                    "name": "subject",
                    "type": "bytes32"
                },
                {
                    "name": "did",
                    "type": "string"
                },
                {
                    "name": "requestId",
                    "type": "bytes32"
                },
                {
                    "name": "deadline",
                    "type": "uint256"
                }
            ]
        },
        "primaryType": "LinkDidRequest",
        "message": {
            "oclId": "0x0101000000000000000000000000000000000000000000000000000000000042",
            "provider": "0xd025eab60676ea26fe9a9a945c991e6d0f3f06e3a940bb5123899345a9b2a413",
            "subject": "0xd844bb55167ab332117049e2ccd3d8863d241bcc80f46302310a6d942a90e851",
            "did": "did:odf:ed25519:z6MkhaXgBZDvotAccount",
            "requestId": "0xad68a4a8b76681274554d59f863c35d7abcef09a798c99e9717a2582037764a5",
            "deadline": "1893456000",
        },
    }))?;

    // 4.) Test fixture — reproduce these values
    // https://github.com/moleculeprotocol/onchainlabs/blob/main/docs/identity/kamu-eip712-linkdidrequest-handoff.md#5-test-private-key--expected-signature

    // 4.2) Struct hash
    {
        let expected_hash =
            b256!("0x1150be8b733f893e4d086fd0a993433dd43c8d1d056e5a1539221b359dbbf041");
        let actual_hash = typed_data.hash_struct()?;

        assert_eq!(expected_hash, actual_hash);
    }

    // 4.3) Domain separator
    {
        let expected_hash =
            b256!("0x9d056bc714b1d8ad38256fe8ba99f3e343766ceff53a0e9eb8c31b93295d1e7e");
        let actual_hash = typed_data.domain_separator();

        assert_eq!(expected_hash, actual_hash);
    }

    // 4.4) EIP-712 digest (w/ EIP-191 prefix)
    {
        let expected_signing_hash =
            b256!("0xfc67633d930b129099d4600295c3676b35c191d90dd4b59274fb2dfd70396c9c");
        let actual_signing_hash = typed_data.signing_hash_with_eip191_prefix()?;

        assert_eq!(expected_signing_hash, actual_signing_hash);
    }

    // 5) Test private key + expected signature (w/ EIP-191 prefix)
    // https://github.com/moleculeprotocol/onchainlabs/blob/main/docs/identity/kamu-eip712-linkdidrequest-handoff.md#5-test-private-key--expected-signature
    {
        use crypto_eip712_utils::SigningKey;

        // kamu-attester
        let private_key =
            b256!("0x42f3bebeb03afa3f14440c6837fa653a84e76bb74d62856227a97f3ee487b601");
        let signing_key = SigningKey::from_slice(private_key.as_slice())?;

        let signed_hash = typed_data.signing_hash_with_eip191_prefix()?;

        let expected_signature = "0xf3073c2f0a3512fc896cb98d9a93c014f25c1dd758dd2c8e27d31aa6d6d2bc4756b739978bf784b52718653a46b9283b10f47359fee686bde8b70882e305eadc1c";
        let actual_signature = sign_prefixed(&signing_key, signed_hash.as_slice())?;

        assert_eq!(expected_signature, actual_signature);
    }

    // II. BE DID-Linking — EIP-712 Fixture Vectors

    // 3.2) Expected 65-byte signatures
    // https://github.com/moleculeprotocol/onchainlabs/blob/main/docs/identity/be-did-linking-eip712-fixtures.md#32-expected-65-byte-signatures-rsv
    {
        use alloy::primitives::Signature;
        use crypto_eip712_utils::SigningKey;

        // kamu-attester
        let private_key =
            b256!("0x42f3bebeb03afa3f14440c6837fa653a84e76bb74d62856227a97f3ee487b601");
        let signing_key = SigningKey::from_slice(private_key.as_slice())?;
        let proof_hash =
            b256!("0x0e270ea3c4029b14e3ca5f8c38ada678d9076889d3f3f1b7a8a6a5596124175c");

        let expected_signature =
            "0x2c135e2f43fe14d8f5027bf91e4bb4207f1aa9edcfc74031f1e4d6945293ca9d55216a0fe19964d2b6420b6c2f4bfd759db55f9bb95850d17f9e0ab8569f5c3f1b";
        let actual_signature = sign_prefixed(&signing_key, proof_hash.as_slice())?;

        assert_eq!(expected_signature, actual_signature);

        // We additionally verify the address recovery
        let signature: Signature = actual_signature.parse()?;

        let expected_address = "0xcb687F3f6Ae1fF2E65CfA6423c533E0Fc82FB356";
        let actual_address = signature.recover_address_from_prehash(&proof_hash)?;

        assert_eq!(expected_address, actual_address.to_checksum(None));
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
