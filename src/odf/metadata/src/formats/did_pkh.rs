// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ssi_caips::caip10;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DID_PKH_PREFIX: &str = "did:pkh:";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents DID in `did:pkh` method.
///
/// Format: `did:pkh:<account_id(CAIP-10)>`
///
/// Example: `did:pkh:eip155:1:0xab16a96D359eC26a11e2C2b3d8f8B8942d5Bfcdb`
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DidPkh {
    caip10_account_id: caip10::BlockchainAccountId,
}

impl DidPkh {
    /// Parses DID from a canonical `did:pkh:<chain-code>:<address>` string
    pub fn from_did_str(s: &str) -> Result<Self, DidPkhParseError> {
        if let Some(stripped) = s.strip_prefix(DID_PKH_PREFIX) {
            DidPkh::parse_caip10_account_id(stripped)
        } else {
            Err(DidPkhParseError::InvalidValueFormat {
                value: s.to_string(),
            })
        }
    }

    /// Parses DID from a CAIP-10 account ID string
    pub fn parse_caip10_account_id(s: &str) -> Result<Self, DidPkhParseError> {
        Ok(Self {
            caip10_account_id: s.parse()?,
        })
    }

    /// Formats DID as a canonical `did:pkh:<account_id(CAIP-10)>` string
    pub fn as_did_str(&self) -> DidPkhFmt {
        DidPkhFmt::new(self)
    }

    pub fn wallet_address(&self) -> &str {
        &self.caip10_account_id.account_address
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum DidPkhParseError {
    #[error(transparent)]
    ParseError(#[from] caip10::BlockchainAccountIdParseError),

    #[error("Invalid value format: {value}")]
    InvalidValueFormat { value: String },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Ord for DidPkh {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match self
            .caip10_account_id
            .chain_id
            .namespace
            .cmp(&other.caip10_account_id.chain_id.namespace)
        {
            Ordering::Equal => {}
            res => return res,
        }

        match self
            .caip10_account_id
            .chain_id
            .reference
            .cmp(&other.caip10_account_id.chain_id.reference)
        {
            Ordering::Equal => {}
            res => return res,
        }

        self.caip10_account_id
            .account_address
            .cmp(&other.caip10_account_id.account_address)
    }
}

impl PartialOrd for DidPkh {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Debug for DidPkh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DidPkh")
            .field(&self.caip10_account_id.chain_id.namespace)
            .field(&self.caip10_account_id.chain_id.reference)
            .field(&self.caip10_account_id.account_address)
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl multiformats::Multiformat for DidPkh {
    fn format_name() -> &'static str {
        "did:pkh"
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Formats [`DidPkh`] as a canonical `did:pkh:<chain-code>:<address>` string
pub struct DidPkhFmt<'a> {
    value: &'a DidPkh,
}

// TODO: Wallet-based auth: implement to_stack_string()
impl<'a> DidPkhFmt<'a> {
    pub fn new(value: &'a DidPkh) -> Self {
        Self { value }
    }
}

impl std::fmt::Debug for DidPkhFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::Display for DidPkhFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{DID_PKH_PREFIX}{}", self.value.caip10_account_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
