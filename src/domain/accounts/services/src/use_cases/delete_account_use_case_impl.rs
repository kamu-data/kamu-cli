// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn DeleteAccountUseCase)]
pub struct DeleteAccountUseCaseImpl {
    account_service: Arc<dyn AccountService>,
}

#[async_trait::async_trait]
impl DeleteAccountUseCase for DeleteAccountUseCaseImpl {
    async fn execute(&self, account_name: &odf::AccountName) -> Result<(), DeleteAccountError> {
        self.account_service
            .delete_account_by_name(account_name)
            .await?;

        // TODO: notify other domains

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
