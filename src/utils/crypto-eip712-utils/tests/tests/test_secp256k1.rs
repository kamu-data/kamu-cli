// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use alloy::primitives::b256;
use crypto_eip712_utils::Secp256k1Signer;
use pretty_assertions::assert_eq;

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
