// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type EvmWalletAddress = alloy_primitives::Address;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EvmWalletAddressConvertor {}

impl EvmWalletAddressConvertor {
    const NO_CHAIN_ID: Option<u64> = None;

    pub fn parse_checksummed<S: AsRef<str>>(
        value: S,
    ) -> Result<EvmWalletAddress, alloy_primitives::AddressError> {
        EvmWalletAddress::parse_checksummed(value, Self::NO_CHAIN_ID)
    }

    pub fn checksummed_string(wallet: &EvmWalletAddress) -> String {
        wallet.to_checksum(Self::NO_CHAIN_ID)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
