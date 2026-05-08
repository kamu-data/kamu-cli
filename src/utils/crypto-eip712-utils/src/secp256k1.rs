// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use alloy_primitives::hex;
use internal_error::{InternalError, ResultIntoInternal};
use k256::ecdsa::SigningKey;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type EthereumSignatureBytes = [u8; 65];

pub fn sign(key: &SigningKey, data: &[u8]) -> Result<EthereumSignatureBytes, InternalError> {
    let (signature, recovery_id) = key.sign_prehash_recoverable(data).int_err()?;

    let mut buf = [0u8; 65];
    buf[..64].copy_from_slice(&signature.to_bytes());
    // Safety: recovery_id is [0, 3]; Ethereum adds 27, no overlap
    buf[64] = recovery_id.to_byte() + 27;

    Ok(buf)
}

pub fn sign_prefixed(key: &SigningKey, data: &[u8]) -> Result<String, InternalError> {
    let signature_bytes = sign(key, data)?;
    Ok(hex::encode_prefixed(signature_bytes))
}

pub fn verification_key_prefixed(key: &SigningKey) -> String {
    hex::encode_prefixed(key.verifying_key().to_sec1_bytes())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
