// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{EntityListing, PaginationOpts};
use internal_error::InternalError;
use thiserror::Error;

use crate::{Account, AccountStream};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AccountService: Sync + Send {
    // TODO: Private Datasets: extract to AccountRegistry?
    fn all_accounts(&self) -> AccountStream;

    async fn list_all_accounts(
        &self,
        pagination: PaginationOpts,
    ) -> Result<EntityListing<Account>, ListAccountError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Error
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ListAccountError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
