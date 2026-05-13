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

// NOTE: SigningKey is ZeroizeOnDrop,
//       so we don't need to worry about zeroing it manually
#[derive(Clone, PartialEq)]
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

    pub fn verification_key(&self) -> Secp256k1VerifyingKey {
        let verifying_key_ref = self.0.credential().verifying_key();

        Secp256k1VerifyingKey::new(*verifying_key_ref)
    }
}

impl std::str::FromStr for Secp256k1Signer {
    type Err = InternalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let key: LocalSigner<SigningKey> = s.parse().int_err()?;

        Ok(Secp256k1Signer(key))
    }
}

impl zeroize::ZeroizeOnDrop for Secp256k1Signer {}

impl std::fmt::Debug for Secp256k1Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // NOTE: SigningKey implementation outputs address, but we go further
        f.write_str("Secp256k1Signer(***)")
    }
}

// NOTE: needed for setty::Config
#[cfg(feature = "serde")]
impl serde::Serialize for Secp256k1Signer {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use zeroize::Zeroize;

        let bytes = zeroize::Zeroizing::new(self.0.to_bytes().0);
        let mut buf = const_hex::Buffer::<_, true>::new();

        let res = serializer.collect_str(buf.format(&bytes));

        // NOTE: encoded buf cleanup
        buf.as_mut_str().zeroize();

        res
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Secp256k1Signer {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = Secp256k1Signer;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a secp256k1 private key")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                v.parse().map_err(serde::de::Error::custom)
            }
        }

        deserializer.deserialize_string(Visitor)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Secp256k1Signer {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Secp256k1Signer".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Secp256k1Signer").into()
    }

    fn json_schema(g: &mut schemars::SchemaGenerator) -> schemars::Schema {
        str::json_schema(g)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype::nutype(derive(Debug, Deref, Copy, Clone, Hash, PartialEq, Eq, FromStr))]
pub struct Secp256k1Signature(Signature);

impl Secp256k1Signature {
    pub fn as_encoded_stack_buf(&self) -> const_hex::Buffer<65, true> {
        let mut stack_buf = const_hex::Buffer::new();
        stack_buf.format(&self.as_bytes());
        stack_buf
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Secp256k1Signature {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self.as_encoded_stack_buf().as_str())
    }
}

#[cfg(feature = "utoipa")]
impl utoipa::ToSchema for Secp256k1Signature {}

#[cfg(feature = "utoipa")]
impl utoipa::PartialSchema for Secp256k1Signature {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::*;

        let signature: Secp256k1Signature = "0x48b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353efffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c8041b".parse().unwrap();

        Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::String))
                .examples([serde_json::json!(signature)])
                .build(),
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype::nutype(derive(Debug, Deref, Copy, Clone, PartialEq, Eq))]
pub struct Secp256k1VerifyingKey(VerifyingKey);

impl Secp256k1VerifyingKey {
    const COMPRESSED_ENCODED_POINT_LENGTH: usize = 33;

    pub fn from_sec1_bytes(data: &[u8]) -> Result<Self, InternalError> {
        let verifying_key = VerifyingKey::from_sec1_bytes(data).int_err()?;

        Ok(Secp256k1VerifyingKey::new(verifying_key))
    }

    pub fn from_sec1_bytes_str(s: &str) -> Result<Self, InternalError> {
        let data = const_hex::decode(s).int_err()?;

        Self::from_sec1_bytes(&data)
    }

    pub fn as_address(&self) -> Address {
        Address::from_public_key(&*self)
    }

    pub fn verify_prehash(
        &self,
        hash: &B256,
        signature: &Secp256k1Signature,
    ) -> Result<(), InternalError> {
        use std::ops::Deref;

        use alloy::signers::k256::ecdsa::signature::hazmat::PrehashVerifier;

        let signature = signature.to_k256().int_err()?;

        self.deref()
            .verify_prehash(hash.as_slice(), &signature)
            .int_err()
    }

    pub fn as_encoded_stack_buf(
        &self,
    ) -> const_hex::Buffer<{ Self::COMPRESSED_ENCODED_POINT_LENGTH }, true> {
        let compressed_encoded_point = self.to_encoded_point(true);

        let Ok(bytes) = compressed_encoded_point.as_bytes().try_into() else {
            unreachable!()
        };

        let mut stack_buf = const_hex::Buffer::new();
        stack_buf.format(bytes);
        stack_buf
    }
}

impl std::fmt::Display for Secp256k1VerifyingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_encoded_stack_buf().as_str())
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Secp256k1VerifyingKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self.as_encoded_stack_buf().as_str())
    }
}

#[cfg(feature = "utoipa")]
impl utoipa::ToSchema for Secp256k1VerifyingKey {}

#[cfg(feature = "utoipa")]
impl utoipa::PartialSchema for Secp256k1VerifyingKey {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::*;

        let verifying_key = Secp256k1VerifyingKey::from_sec1_bytes_str(
            "0x03993fbdd2f7a840b78202496af7e699dc9fcd1667f16dcce73887d563f448cc31",
        )
        .unwrap();

        Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::String))
                .examples([serde_json::json!(verifying_key)])
                .build(),
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
