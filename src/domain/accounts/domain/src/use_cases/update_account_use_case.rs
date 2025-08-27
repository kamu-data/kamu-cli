// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{Account, UpdateAccountError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UpdateAccountUseCase: Send + Sync {
    async fn execute(&self, account: &Account) -> Result<(), UpdateAccountError>;

    async fn execute_internal(
        &self,
        account: &Account,
        original_account: &Account,
    ) -> Result<(), UpdateAccountError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
