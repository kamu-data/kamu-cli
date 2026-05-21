// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Private keys are used to sign API responses.
/// Supported algorithms: `ed25519`, `secp256k1`.
#[derive(Debug, Clone)]
pub struct IdentityConfig {
    /// Root private key that corresponds to the `authority` and is used to sign
    /// responses.
    ///
    /// To generate, use:
    /// ```sh
    /// od -vN 32 -An -tx1 /dev/urandom | tr -d ' \n' && echo
    /// ```
    /// or
    /// ```sh
    /// openssl rand -hex 32
    /// ```
    pub ed25519_private_key: odf::metadata::PrivateKey,

    /// Secp256k1 private key used to sign EIP-712 typed data.
    ///
    /// To generate, use:
    /// ```sh
    /// od -vN 32 -An -tx1 /dev/urandom | tr -d ' \n' && echo
    /// ```
    /// or
    /// ```sh
    /// openssl rand -hex 32
    /// ```
    /// or
    /// ```sh
    /// cast wallet new
    /// ```
    pub secp256k1_private_key: crypto_eip712_utils::Secp256k1Signer,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
