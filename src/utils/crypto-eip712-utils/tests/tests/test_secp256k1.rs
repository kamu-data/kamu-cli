// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crypto_eip712_utils::{SigningKey, b256, verification_key_prefixed};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_verification_key_prefixed() -> eyre::Result<()> {
    // kamu-attester
    let private_key = b256!("0x42f3bebeb03afa3f14440c6837fa653a84e76bb74d62856227a97f3ee487b601");
    let signing_key = SigningKey::from_slice(private_key.as_slice())?;

    assert_eq!(
        "0x03993fbdd2f7a840b78202496af7e699dc9fcd1667f16dcce73887d563f448cc31",
        verification_key_prefixed(&signing_key)
    );

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
