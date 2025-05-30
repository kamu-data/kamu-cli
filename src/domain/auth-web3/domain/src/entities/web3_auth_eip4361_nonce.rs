// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use regex::Regex;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static EIP_4361_NONCE_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("^[A-Za-z0-9]{8,}$").unwrap());

#[nutype::nutype(
    sanitize(trim),
    validate(regex = EIP_4361_NONCE_REGEX),
    derive(AsRef, Clone, Debug, Display, Eq, PartialEq, TryFrom)
)]
pub struct Web3AuthenticationEip4361Nonce(String);

impl Web3AuthenticationEip4361Nonce {
    pub fn new() -> Self {
        Self::try_new(siwe::generate_nonce()).expect("Invalid nonce generated")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
