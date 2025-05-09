// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use email_utils::Email;

use crate::{Account, CreateAccountError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CreateAccountUseCase: Send + Sync {
    async fn execute(
        &self,
        creator_account: &Account,
        account_name: &odf::AccountName,
        email_maybe: Option<Email>,
    ) -> Result<Account, CreateAccountError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
