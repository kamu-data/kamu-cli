// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Duration;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_auth_web3::*;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const WEB3_AUTH_EIP_4361_EXPIRES_IN_15_MINUTES: Duration = Duration::minutes(15);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Web3AuthEip4361NonceService)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: JOB_KAMU_WEB_3_NONCE_SERVICE,
    depends_on: &[],
    requires_transaction: true,
})]
pub struct Web3AuthEip4361NonceServiceImpl {
    nonce_repo: Arc<dyn Web3AuthEip4361NonceRepository>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Web3AuthEip4361NonceService for Web3AuthEip4361NonceServiceImpl {
    async fn create_nonce(
        &self,
        wallet_address: EvmWalletAddress,
    ) -> Result<Web3AuthEip4361NonceEntity, CreateNonceError> {
        let now = self.time_source.now();

        let entity = Web3AuthEip4361NonceEntity {
            wallet_address,
            nonce: Web3AuthenticationEip4361Nonce::new(),
            expires_at: now + WEB3_AUTH_EIP_4361_EXPIRES_IN_15_MINUTES,
        };

        self.nonce_repo.set_nonce(&entity).await.int_err()?;

        Ok(entity)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl InitOnStartup for Web3AuthEip4361NonceServiceImpl {
    #[tracing::instrument(level = "debug", skip_all, name = Web3NonceServiceImpl_run_initialization)]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        let now = self.time_source.now();

        self.nonce_repo.cleanup_expired_nonces(now).await.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
