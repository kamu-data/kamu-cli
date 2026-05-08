// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use alloy_primitives::{b256, hex};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use crypto_eip712_utils::eip712_typed_data;
use pretty_assertions::assert_eq;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_molecule_provided_test_data() -> eyre::Result<()> {
    // Adapted from:
    // https://github.com/moleculeprotocol/onchainlabs/blob/main/docs/identity/kamu-eip712-linkdidrequest-handoff.md

    let typed_data = eip712_typed_data::from_json(json!({
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
    // a) alloy: via sign_dynamic_typed_data_sync()
    {
        let signer: PrivateKeySigner =
            "0x42f3bebeb03afa3f14440c6837fa653a84e76bb74d62856227a97f3ee487b601".parse()?;

        let actual_signature = signer.sign_dynamic_typed_data_sync(typed_data.as_ref())?;
        let expected_signature = "0xf3073c2f0a3512fc896cb98d9a93c014f25c1dd758dd2c8e27d31aa6d6d2bc4756b739978bf784b52718653a46b9283b10f47359fee686bde8b70882e305eadc1c";

        assert_eq!(expected_signature, actual_signature.to_string());
    }
    // b) alloy: via sign_hash_sync()
    {
        let signer: PrivateKeySigner =
            "0x42f3bebeb03afa3f14440c6837fa653a84e76bb74d62856227a97f3ee487b601".parse()?;
        let signed_hash = typed_data.signing_hash_with_eip191_prefix()?;

        let actual_signature = signer.sign_hash_sync(&signed_hash)?;
        let expected_signature = "0xf3073c2f0a3512fc896cb98d9a93c014f25c1dd758dd2c8e27d31aa6d6d2bc4756b739978bf784b52718653a46b9283b10f47359fee686bde8b70882e305eadc1c";

        assert_eq!(expected_signature, actual_signature.to_string());
    }
    // b) odf
    {
        use odf_metadata::ed25519::Signer;

        let pk = odf_metadata::PrivateKey::from_bytes(&hex!(
            "0x42f3bebeb03afa3f14440c6837fa653a84e76bb74d62856227a97f3ee487b601"
        ));
        let signing_hash = typed_data.signing_hash_with_eip191_prefix()?;

        // TODO: Molecule: Phase 3: verify that the signature is correct
        //                 Find an example in the Molecule documentation?
        let actual_signature = hex::encode_prefixed(pk.sign(signing_hash.as_ref()).to_bytes());
        let expected_signature = "0xb4351e080c62d210b90d2eac67f079efce28c6eb16278c73a51c65f431c2dac65dda66edb4aef6180d77dfee74d02991996fdfb6671272b391d9ba34ea188407";

        assert_eq!(actual_signature, expected_signature);
    }

    // ---

    // TODO: Molecule: Phase 3: EIP-712 digest (w/o EIP-191 prefix)

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
