// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use alloy::primitives::{Address, B256, Signature, keccak256};
use alloy::signers::k256::ecdsa::{SigningKey, VerifyingKey};
use alloy::signers::local::LocalSigner;
use internal_error::{InternalError, ResultIntoInternal};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Secp256k1Signer(LocalSigner<SigningKey>);

impl Secp256k1Signer {
    pub fn from_signing_key(key: SigningKey) -> Self {
        Secp256k1Signer(LocalSigner::from_signing_key(key))
    }

    pub fn from_bytes(data: zeroize::Zeroizing<[u8; 32]>) -> Result<Self, InternalError> {
        SigningKey::from_slice(data.as_slice())
            .map(Self::from_signing_key)
            .int_err()
    }

    pub fn sign_prehash(&self, hash: &B256) -> Result<Secp256k1Signature, InternalError> {
        use alloy::signers::SignerSync;

        let signature = self.0.sign_hash_sync(hash).int_err()?;

        Ok(Secp256k1Signature::new(signature))
    }

    pub fn sign(&self, data: &[u8]) -> Result<Secp256k1Signature, InternalError> {
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

impl std::fmt::Debug for Secp256k1Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // NOTE: Default implementation outputs address, but we go further
        f.write_str("Secp256k1Signer(***)")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype::nutype(derive(Debug, Deref, Copy, Clone, Hash, PartialEq, Eq))]
pub struct Secp256k1Signature(Signature);

impl Secp256k1Signature {
    pub fn as_encoded_stack_buf(&self) -> const_hex::Buffer<65, true> {
        let mut buf = const_hex::Buffer::new();
        buf.format(&self.as_bytes());
        buf
    }
}

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

        write!(f, "{}", const_hex::encode_prefixed(bytes))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
