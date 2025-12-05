// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{
    ConcurrentModificationError,
    EventID,
    EventStore,
    GetEventsError,
    GetEventsOpts,
    Projection,
    SaveEventsError,
};
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError};
use thiserror::Error;

use crate::{
    AccountQuota,
    AccountQuotaEvent,
    AccountQuotaState,
    AccountQuotaQuery,
    QuotaType,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AccountQuotaEventStore: EventStore<AccountQuotaState> {
    async fn get_quota(
        &self,
        query: &AccountQuotaQuery,
    ) -> Result<AccountQuota, GetAccountQuotaError> {
        let mut stream = self.get_events(query, GetEventsOpts::default());
        let mut state: Option<AccountQuotaState> = None;

        while let Some(next_event) = stream
            .try_next()
            .await
            .map_err(map_get_events_error)? 
        {
            let (_, event) = next_event;
            state = Some(
                AccountQuotaState::apply(state, event).map_err(InternalError::new)?,
            );
        }

        if let Some(projection) = state
            && let Some(quota) = projection.quota
        {
            return Ok(quota);
        }

        Err(GetAccountQuotaError::NotFound(AccountQuotaNotFoundError {
            account_id: query.account_id.clone(),
            quota_type: query.quota_type,
        }))
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
        let mut stream = self.get_events(query, GetEventsOpts::default());
        let mut last_event_id: Option<EventID> = None;

        while let Some(next_event) = stream
            .try_next()
            .await
            .map_err(map_get_events_error)?
        {
            let (event_id, _) = next_event;
            last_event_id = Some(event_id);
        }

        Ok(last_event_id)
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
            SaveEventsError::ConcurrentModification(err) => {
                SaveAccountQuotaError::Concurrent(err)
            }
            SaveEventsError::NothingToSave => {
                SaveAccountQuotaError::Internal("Nothing to save".int_err())
            }
            SaveEventsError::Internal(err) => SaveAccountQuotaError::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_get_events_error(e: GetEventsError) -> GetAccountQuotaError {
    match e {
        GetEventsError::Internal(err) => GetAccountQuotaError::Internal(err),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
