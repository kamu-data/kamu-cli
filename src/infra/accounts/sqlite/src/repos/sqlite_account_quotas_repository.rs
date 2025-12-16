// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::TransactionRefT;
use dill::{component, interface};
use event_sourcing::{EventID, EventStore, GetEventsOpts, SaveEventsError};
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn AccountQuotaEventStore)]
pub struct SqliteAccountQuotaEventStore {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SqliteAccountQuotaEventStore {
    fn quota_type_to_db(quota_type: &QuotaType) -> String {
        quota_type.to_string()
    }

    fn parse_event(
        row: &AccountQuotaEventRow,
    ) -> Result<(EventID, AccountQuotaEvent), InternalError> {
        let event: AccountQuotaEvent = serde_json::from_str(&row.event_payload).int_err()?;

        Ok((EventID::new(row.id), event))
    }

    fn map_save_error(
        expected: Option<EventID>,
        actual: Option<i64>,
    ) -> Result<(), SaveEventsError> {
        match (expected, actual) {
            (None, None) => Ok(()),
            (Some(_), Some(last)) => {
                if expected.unwrap().into_inner() == last {
                    Ok(())
                } else {
                    Err(SaveEventsError::concurrent_modification())
                }
            }
            (Some(_), None) | (None, Some(_)) => Err(SaveEventsError::concurrent_modification()),
        }
    }

    async fn last_event_id(
        connection: &mut sqlx::SqliteConnection,
        query: &AccountQuotaQuery,
    ) -> Result<Option<i64>, InternalError> {
        use odf::metadata::AsStackString;

        let account_id = query.account_id.as_stack_string().to_string();
        let quota_type = Self::quota_type_to_db(&query.quota_type);
        let account_id_str = account_id.as_str();
        let quota_type_str = quota_type.as_str();

        let res = sqlx::query_scalar!(
            r#"
            SELECT id
            FROM account_quota_events
            WHERE account_id = ?1 AND quota_type = ?2
            ORDER BY id DESC
            LIMIT 1
            "#,
            account_id_str,
            quota_type_str,
        )
        .fetch_optional(connection)
        .await
        .int_err()?;

        Ok(res.flatten())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<AccountQuotaState> for SqliteAccountQuotaEventStore {
    fn get_all_events(
        &self,
        opts: GetEventsOpts,
    ) -> event_sourcing::EventStream<'_, AccountQuotaEvent> {
        Box::pin(async_stream::try_stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let from = opts.from.map(EventID::into_inner).unwrap_or(i64::MIN);
            let to = opts.to.map(EventID::into_inner).unwrap_or(i64::MAX);

            let mut rows = sqlx::query_as::<_, AccountQuotaEventRow>(
                r#"
                SELECT
                    id,
                    account_id,
                    quota_type,
                    event_type,
                    event_payload,
                    event_time
                FROM account_quota_events
                WHERE id > ?1
                  AND id <= ?2
                ORDER BY id
                "#,
            )
            .bind(from)
            .bind(to)
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = rows.try_next().await? {
                yield Self::parse_event(&row)?;
            }
        })
    }

    fn get_events(
        &self,
        query: &AccountQuotaQuery,
        opts: GetEventsOpts,
    ) -> event_sourcing::EventStream<'_, AccountQuotaEvent> {
        let query = query.clone();

        Box::pin(async_stream::try_stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            use odf::metadata::AsStackString;

            let account_id = query.account_id.as_stack_string().to_string();
            let quota_type = Self::quota_type_to_db(&query.quota_type);
            let from = opts.from.map(EventID::into_inner).unwrap_or(i64::MIN);
            let to = opts.to.map(EventID::into_inner).unwrap_or(i64::MAX);
            let account_id_str = account_id.as_str();
            let quota_type_str = quota_type.as_str();

            let mut rows = sqlx::query_as::<_, AccountQuotaEventRow>(
                r#"
                SELECT
                    id,
                    account_id,
                    quota_type,
                    event_type,
                    event_payload,
                    event_time
                FROM account_quota_events
                WHERE account_id = ?1
                  AND quota_type = ?2
                  AND id > ?3
                  AND id <= ?4
                ORDER BY id
                "#,
            )
            .bind(account_id_str)
            .bind(quota_type_str)
            .bind(from)
            .bind(to)
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = rows.try_next().await? {
                yield Self::parse_event(&row)?;
            }
        })
    }

    async fn save_events(
        &self,
        query: &AccountQuotaQuery,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<AccountQuotaEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(ErrorIntoInternal::int_err)?;

        let last_id = Self::last_event_id(connection_mut, query)
            .await
            .map_err(ErrorIntoInternal::int_err)?;

        Self::map_save_error(maybe_prev_stored_event_id, last_id)?;

        use odf::metadata::AsStackString;

        let account_id = query.account_id.as_stack_string().to_string();
        let quota_type = Self::quota_type_to_db(&query.quota_type);
        let account_id_str = account_id.as_str();
        let quota_type_str = quota_type.as_str();
        let mut last_event_id = None;

        for event in events {
            let payload = serde_json::to_string(&event)
                .int_err()
                .map_err(SaveEventsError::Internal)?;
            let event_type = event.typename().to_string();
            let event_time = event.event_time();

            let inserted: i64 = sqlx::query_scalar!(
                r#"
                INSERT INTO account_quota_events (
                    account_id,
                    quota_type,
                    event_type,
                    event_payload,
                    event_time
                )
                VALUES (?1, ?2, ?3, ?4, ?5)
                RETURNING id
                "#,
                account_id_str,
                quota_type_str,
                event_type,
                payload,
                event_time,
            )
            .fetch_one(&mut *connection_mut)
            .await
            .int_err()
            .map_err(SaveEventsError::Internal)?;

            last_event_id = Some(EventID::new(inserted));
        }

        Ok(last_event_id.expect("events is not empty"))
    }

    async fn len(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let total_events: i64 = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) FROM account_quota_events
            "#
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(usize::try_from(total_events).int_err()?)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccountQuotaEventStore for SqliteAccountQuotaEventStore {
    async fn save_quota_events(
        &self,
        query: &AccountQuotaQuery,
        maybe_prev_event_id: Option<EventID>,
        events: Vec<AccountQuotaEvent>,
    ) -> Result<EventID, SaveAccountQuotaError> {
        self.save_events(query, maybe_prev_event_id, events)
            .await
            .map_err(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(sqlx::FromRow)]
struct AccountQuotaEventRow {
    #[allow(dead_code)]
    pub id: i64,
    #[allow(dead_code)]
    pub account_id: String,
    #[allow(dead_code)]
    pub quota_type: String,
    #[allow(dead_code)]
    pub event_type: String,
    pub event_payload: String,
    #[allow(dead_code)]
    pub event_time: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
