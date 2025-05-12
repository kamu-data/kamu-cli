// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct AuthWeb3Mut;

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl AuthWeb3Mut {
    #[tracing::instrument(level = "info", name = AuthWeb3Mut_nonce, skip_all, fields(%account))]
    async fn nonce(
        &self,
        ctx: &Context<'_>,
        account: EvmWalletAddress<'_>,
    ) -> Result<NonceResponse> {
        let authentication_service = from_catalog_n!(ctx, dyn kamu_auth_web3::Web3NonceService);

        let nonce_entity = authentication_service
            .create_nonce(account.into())
            .await
            .int_err()?;

        Ok(NonceResponse {
            value: nonce_entity.nonce.into_inner(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub(crate) struct NonceResponse {
    value: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
