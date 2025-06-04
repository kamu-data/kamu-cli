// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_auth_web3::{EvmWalletAddress, EvmWalletAddressConvertor};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_evm_wallet_address_convertor() {
    let addresses = [
        (
            EvmWalletAddress::ZERO,
            "0x0000000000000000000000000000000000000000",
        ),
        (
            EvmWalletAddress::new([200; 20]),
            "0xc8c8c8c8c8C8c8C8C8C8c8c8C8c8c8C8c8c8C8C8",
        ),
    ];

    for (as_struct, as_string) in addresses {
        pretty_assertions::assert_eq!(
            as_string,
            EvmWalletAddressConvertor::checksummed_string(&as_struct),
            "Tag: {as_string}",
        );

        pretty_assertions::assert_eq!(
            as_struct,
            EvmWalletAddressConvertor::parse_checksummed(as_string).expect(as_string),
            "Tag: {as_string}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
