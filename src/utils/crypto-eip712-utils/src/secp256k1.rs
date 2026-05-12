// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use alloy::hex;
use alloy::primitives::{Address, B256, Signature, keccak256};
use alloy::signers::k256::ecdsa::{SigningKey, VerifyingKey};
use alloy::signers::local::LocalSigner;
use internal_error::{InternalError, ResultIntoInternal};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Secp256k1Signer(LocalSigner<SigningKey>);

impl Secp256k1Signer {
    pub fn from_signing_key(key: SigningKey) -> Self {
        Secp256k1Signer(LocalSigner::from_signing_key(key))
    }

    pub fn from_slice(data: &[u8]) -> Result<Self, InternalError> {
        SigningKey::from_slice(data)
            .map(Self::from_signing_key)
            .int_err()
    }

    pub fn from_bytes(bytes: &B256) -> Result<Self, InternalError> {
        Self::from_slice(bytes.as_slice())
    }

    pub fn sign_prehash(&self, hash: &B256) -> Result<Signature, InternalError> {
        use alloy::signers::SignerSync;

        self.0.sign_hash_sync(hash).int_err()
    }

    pub fn sign(&self, data: &[u8]) -> Result<Signature, InternalError> {
        let hash = keccak256(data);

        self.sign_prehash(&hash)
    }

    pub fn verification_key(&self) -> Secp256k1VerifyingKey<'_> {
        Secp256k1VerifyingKey(self.0.credential().verifying_key())
    }
}

impl Drop for Secp256k1Signer {
    fn drop(&mut self) {
        unsafe { zeroize::zeroize_flat_type(self as *mut Self) }
    }
}

impl zeroize::ZeroizeOnDrop for Secp256k1Signer {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Secp256k1VerifyingKey<'a>(&'a VerifyingKey);

impl Secp256k1VerifyingKey<'_> {
    pub fn as_address(&self) -> Address {
        Address::from_public_key(self.0)
    }

    pub fn verify_prehash(&self, hash: &B256, signature: &Signature) -> Result<(), InternalError> {
        use alloy::signers::k256::ecdsa::signature::hazmat::PrehashVerifier;

        let signature = signature.to_k256().int_err()?;

        self.0.verify_prehash(hash.as_slice(), &signature).int_err()
    }
}

impl std::fmt::Display for Secp256k1VerifyingKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.0.to_sec1_bytes();

        write!(f, "{}", hex::encode_prefixed(bytes))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
