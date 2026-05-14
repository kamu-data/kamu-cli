// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct IdentityConfig {
    /// Root private key that corresponds to the `authority` and is used to sign
    /// responses
    ///
    /// To generate, use:
    ///
    /// ```sh
    /// ssh-keygen -t ed25519 -C "coo@abc.com"
    /// ```
    pub ed25519_private_key: odf::metadata::PrivateKey,

    /// Secp256k1 private key used to sign EIP-712 typed data.
    ///
    /// ```sh
    /// cast wallet new
    /// ```
    pub secp256k1_private_key: crypto_eip712_utils::Secp256k1Signer,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
