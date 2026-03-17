// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use email_utils::Email;

use crate::AccountDisplayName;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DidPkhAccountIdentity {
    pub provider_identity_key: String,
    pub account_name: odf::AccountName,
    pub email: Email,
    pub display_name: AccountDisplayName,
}

impl DidPkhAccountIdentity {
    /// Derives account identity fields from a did:pkh:ABC.. .
    /// Guarantees uniqueness for the same wallet address across different
    /// networks by incorporating chain type and chain ID.
    pub fn from_did_pkh(did_pkh: &odf::metadata::DidPkh) -> Self {
        let wallet_address = did_pkh.wallet_address().to_string();

        let unique_wallet_address_based_ident = {
            let chain_type = &did_pkh.chain_id().namespace;
            let chain_id = &did_pkh.chain_id().reference;
            // Example (eth mainnet): did.pkh.eip155.1.0x...
            format!("did.pkh.{chain_type}.{chain_id}.{wallet_address}")
        };

        let account_name = odf::AccountName::new_unchecked(&unique_wallet_address_based_ident);
        let email =
            Email::parse(&format!("{unique_wallet_address_based_ident}@example.com")).unwrap();

        Self {
            provider_identity_key: unique_wallet_address_based_ident,
            account_name,
            email,
            display_name: wallet_address,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
