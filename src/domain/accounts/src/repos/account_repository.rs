// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::AccountModel;

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AccountRepository: Send + Sync {
    async fn create_account(
        &self,
        account_model: &AccountModel,
    ) -> Result<(), AccountRepositoryError>;

    async fn find_account_by_email(
        &self,
        email: &str,
    ) -> Result<Option<AccountModel>, AccountRepositoryError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum AccountRepositoryError {
    #[error(transparent)]
    Internal(InternalError),
}

///////////////////////////////////////////////////////////////////////////////
