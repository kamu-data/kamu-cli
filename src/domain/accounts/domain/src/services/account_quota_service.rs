// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{AccountQuota, GetAccountQuotaError, SaveAccountQuotaError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AccountQuotaService: Sync + Send {
    async fn set_account_quota(
        &self,
        account_id: &odf::AccountID,
        limit_bytes: u64,
        quota_type: crate::QuotaType,
    ) -> Result<(), SetAccountQuotaError>;

    async fn get_account_quota(
        &self,
        account_id: &odf::AccountID,
        quota_type: crate::QuotaType,
    ) -> Result<AccountQuota, GetAccountQuotaError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Quota Checking
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum QuotaExceededError {
    #[error(transparent)]
    Limit(#[from] LimitError),

    #[error("Quota not configured")]
    NotConfigured,

    #[error(transparent)]
    Internal(#[from] internal_error::InternalError),
}

#[derive(Debug, thiserror::Error)]
#[error("Quota exceeded: used={used}, incoming={incoming}, limit={limit}")]
pub struct LimitError {
    pub used: u64,
    pub incoming: u64,
    pub limit: u64,
}

#[async_trait::async_trait]
pub trait AccountQuotaStorageChecker: Sync + Send {
    async fn ensure_within_quota(
        &self,
        account_id: &odf::AccountID,
        incoming_bytes: u64,
    ) -> Result<(), QuotaExceededError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum SetAccountQuotaError {
    #[error(transparent)]
    Save(#[from] SaveAccountQuotaError),

    #[error(transparent)]
    Internal(#[from] internal_error::InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
