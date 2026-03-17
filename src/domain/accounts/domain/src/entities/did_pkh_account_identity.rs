// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use email_utils::Email;
use internal_error::{InternalError, ResultIntoInternal};

use crate::AccountDisplayName;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
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
    pub fn from_did_pkh(did_pkh: &odf::metadata::DidPkh) -> Result<Self, InternalError> {
        let wallet_address = did_pkh.wallet_address().to_string();

        let unique_wallet_address_based_ident = {
            let chain_type = &did_pkh.chain_id().namespace;
            let chain_id = &did_pkh.chain_id().reference;
            // Example (eth mainnet): did.pkh.eip155.1.0x...
            format!("did.pkh.{chain_type}.{chain_id}.{wallet_address}")
        };

        let account_name =
            odf::AccountName::from_str(&unique_wallet_address_based_ident).int_err()?;
        let email =
            Email::parse(&format!("{unique_wallet_address_based_ident}@example.com")).int_err()?;

        Ok(Self {
            provider_identity_key: unique_wallet_address_based_ident,
            account_name,
            email,
            display_name: wallet_address,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use email_utils::Email;

    use crate::DidPkhAccountIdentity;

    #[test]
    fn test_from_did_pkh() {
        let did_pkh = odf::metadata::DidPkh::from_did_str(
            "did:pkh:eip155:1:0xab16a96D359eC26a11e2C2b3d8f8B8942d5Bfcdb",
        )
        .unwrap();

        let actual = DidPkhAccountIdentity::from_did_pkh(&did_pkh).unwrap();
        let expected = DidPkhAccountIdentity {
            provider_identity_key: "did.pkh.eip155.1.0xab16a96D359eC26a11e2C2b3d8f8B8942d5Bfcdb"
                .to_string(),
            account_name: odf::AccountName::from_str(
                "did.pkh.eip155.1.0xab16a96D359eC26a11e2C2b3d8f8B8942d5Bfcdb",
            )
            .unwrap(),
            email: Email::parse(
                "did.pkh.eip155.1.0xab16a96D359eC26a11e2C2b3d8f8B8942d5Bfcdb@example.com",
            )
            .unwrap(),
            display_name: "0xab16a96D359eC26a11e2C2b3d8f8B8942d5Bfcdb".to_string(),
        };

        assert_eq!(expected, actual);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
