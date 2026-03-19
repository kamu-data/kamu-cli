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
        // NOTE: we use uuid because the username is limited to 64 characters per RFC,
        //       which doesn't always suit us. Besides, this email is a placeholder.
        let email = {
            use uuid::Uuid;

            let user = Uuid::new_v5(
                &Uuid::NAMESPACE_URL,
                unique_wallet_address_based_ident.as_bytes(),
            );
            Email::parse(&format!("{user}@example.com")).int_err()?
        };

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

    use constcat::concat as const_concat;
    use email_utils::Email;
    use pretty_assertions::assert_eq;

    use crate::DidPkhAccountIdentity;

    struct FromDidPkhTestCase {
        input_did_str: &'static str,
        expected_identity_key_and_account_name: &'static str,
        expected_email: &'static str,
        expected_display_name: &'static str,
    }

    const WALLET_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

    #[rstest::rstest]
    // EVM:
    #[case(FromDidPkhTestCase {
        input_did_str: const_concat!("did:pkh:eip155:1:", WALLET_ADDRESS),
        expected_identity_key_and_account_name: const_concat!("did.pkh.eip155.1.", WALLET_ADDRESS),
        expected_email: "8b64fc46-d223-5eee-94ab-ef34d398c23c@example.com",
        expected_display_name: WALLET_ADDRESS,
    })]
    #[case(FromDidPkhTestCase {
        input_did_str: const_concat!("did:pkh:eip155:11155111:", WALLET_ADDRESS),
        expected_identity_key_and_account_name: const_concat!("did.pkh.eip155.11155111.", WALLET_ADDRESS),
        expected_email: "39a3abbd-0c97-53c9-9128-1de56f21bbdb@example.com",
        expected_display_name: WALLET_ADDRESS,
    })]
    fn test_from_did_pkh(
        #[case] FromDidPkhTestCase {
            input_did_str,
            expected_identity_key_and_account_name,
            expected_email,
            expected_display_name,
        }: FromDidPkhTestCase,
    ) {
        let did_pkh = odf::metadata::DidPkh::from_did_str(input_did_str).unwrap();

        let actual = DidPkhAccountIdentity::from_did_pkh(&did_pkh).unwrap();
        let expected = DidPkhAccountIdentity {
            provider_identity_key: expected_identity_key_and_account_name.to_string(),
            account_name: odf::AccountName::from_str(expected_identity_key_and_account_name)
                .unwrap(),
            email: Email::parse(expected_email).unwrap(),
            display_name: expected_display_name.to_string(),
        };

        assert_eq!(expected, actual);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
