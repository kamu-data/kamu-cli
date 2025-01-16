// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use internal_error::InternalError;
use opendatafabric as odf;
use thiserror::Error;

use crate::{Account, AccountPageStream};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Private Datasets: merge with AuthenticationService?
// TODO: Private Datasets: tests
#[async_trait::async_trait]
pub trait AccountService: Sync + Send {
    // TODO: Private Datasets: extract to AccountRegistry?
    fn all_accounts(&self) -> AccountPageStream;

    async fn get_account_map(
        &self,
        account_ids: Vec<odf::AccountID>,
    ) -> Result<HashMap<odf::AccountID, Account>, GetAccountMapError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Error
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetAccountMapError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
