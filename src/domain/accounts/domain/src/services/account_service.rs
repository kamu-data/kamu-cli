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
use thiserror::Error;

use crate::Account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Private Datasets: tests
#[async_trait::async_trait]
pub trait AccountService: Sync + Send {
    async fn account_by_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Option<Account>, InternalError>;

    async fn accounts_by_ids(
        &self,
        account_ids: Vec<odf::AccountID>,
    ) -> Result<Vec<Account>, InternalError>;

    async fn get_account_map(
        &self,
        account_ids: Vec<odf::AccountID>,
    ) -> Result<HashMap<odf::AccountID, Account>, GetAccountMapError>;

    async fn account_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Option<Account>, InternalError>;

    async fn find_account_id_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Option<odf::AccountID>, InternalError>;

    async fn find_account_name_by_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Option<odf::AccountName>, InternalError>;
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
