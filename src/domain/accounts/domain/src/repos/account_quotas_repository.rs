// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{
    Aggregate,
    ConcurrentModificationError,
    EventID,
    EventStore,
    SaveEventsError,
};
use internal_error::{ErrorIntoInternal, InternalError};
use thiserror::Error;

use crate::{AccountQuota, AccountQuotaEvent, AccountQuotaQuery, AccountQuotaState, QuotaType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AccountQuotaEventStore: EventStore<AccountQuotaState> {
    async fn get_quota(
        &self,
        query: &AccountQuotaQuery,
    ) -> Result<AccountQuota, GetAccountQuotaError> {
        let aggregate = Aggregate::<AccountQuotaState, Self>::load(query, self).await;

        match aggregate {
            Ok(agg) => {
                let quota = agg.into_state().quota;
                if quota.active {
                    Ok(quota)
                } else {
                    Err(GetAccountQuotaError::NotFound(AccountQuotaNotFoundError {
                        account_id: query.account_id.clone(),
                        quota_type: query.quota_type.clone(),
                    }))
                }
            }
            Err(event_sourcing::LoadError::NotFound(_)) => {
                Err(GetAccountQuotaError::NotFound(AccountQuotaNotFoundError {
                    account_id: query.account_id.clone(),
                    quota_type: query.quota_type.clone(),
                }))
            }
            Err(event_sourcing::LoadError::ProjectionError(err)) => {
                Err(GetAccountQuotaError::Internal(err.int_err()))
            }
            Err(event_sourcing::LoadError::Internal(err)) => {
                Err(GetAccountQuotaError::Internal(err))
            }
        }
    }

    async fn get_quota_by_account_id(
        &self,
        account_id: &odf::AccountID,
        quota_type: QuotaType,
    ) -> Result<AccountQuota, GetAccountQuotaError> {
        self.get_quota(&AccountQuotaQuery {
            account_id: account_id.clone(),
            quota_type,
        })
        .await
    }

    async fn save_quota_events(
        &self,
        query: &AccountQuotaQuery,
        maybe_prev_event_id: Option<EventID>,
        events: Vec<AccountQuotaEvent>,
    ) -> Result<EventID, SaveAccountQuotaError>;

    async fn last_event_id(
        &self,
        query: &AccountQuotaQuery,
    ) -> Result<Option<EventID>, GetAccountQuotaError> {
        match Aggregate::<AccountQuotaState, Self>::try_load(query, self).await {
            Ok(Some(agg)) => Ok(agg.last_stored_event_id()),
            Ok(None) => Ok(None),
            Err(event_sourcing::TryLoadError::ProjectionError(err)) => {
                Err(GetAccountQuotaError::Internal(err.int_err()))
            }
            Err(event_sourcing::TryLoadError::Internal(err)) => {
                Err(GetAccountQuotaError::Internal(err))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetAccountQuotaError {
    #[error(transparent)]
    NotFound(AccountQuotaNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Account quota not found: account_id={account_id}, quota_type='{quota_type:?}'")]
pub struct AccountQuotaNotFoundError {
    pub account_id: odf::AccountID,
    pub quota_type: QuotaType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SaveAccountQuotaError {
    #[error(transparent)]
    Concurrent(ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<SaveEventsError> for SaveAccountQuotaError {
    fn from(value: SaveEventsError) -> Self {
        match value {
            SaveEventsError::ConcurrentModification(err) => SaveAccountQuotaError::Concurrent(err),
            SaveEventsError::NothingToSave => {
                SaveAccountQuotaError::Internal("Nothing to save".int_err())
            }
            SaveEventsError::Internal(err) => SaveAccountQuotaError::Internal(err),
        }
    }
}
