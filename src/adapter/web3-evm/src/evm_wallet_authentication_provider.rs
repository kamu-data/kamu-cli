// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{AuthenticationProvider, ProviderLoginError, ProviderLoginResponse};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const PROVIDER_EVM_WALLET: &str = "evm-wallet";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn AuthenticationProvider)]
pub struct EvmWalletAuthenticationProvider {}

impl EvmWalletAuthenticationProvider {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AuthenticationProvider for EvmWalletAuthenticationProvider {
    fn provider_name(&self) -> &'static str {
        PROVIDER_EVM_WALLET
    }

    // TODO: Wallet-based auth: absorb into login() method
    // TODO: Wallet-based auth: return enum:
    //       - odf::AccountID; and
    //       - the brand new did:ethr:0x1337....FFFF
    fn generate_id(&self, _: &odf::AccountName) -> odf::AccountID {
        todo!("TODO: Wallet-based auth: implement")
    }

    async fn login(
        &self,
        _login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError> {
        todo!("TODO: Wallet-based auth: implement")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
