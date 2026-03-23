// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{PaginationOpts, TransactionRefT};
use dill::{component, interface};
use event_sourcing::{EventID, GetEventsOpts, SaveEventsError};
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_resources::{
    NewStoredResourceEvent,
    ResourceEventStore,
    ResourceID,
    ResourceIDStream,
    ResourceName,
    ResourceRepository,
    ResourceRow,
    ResourceStreamKey,
    StoredResourceEvent,
    StoredResourceEventStream,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn ResourceEventStore)]
#[interface(dyn ResourceRepository)]
pub struct PostgresResourceEventStore {
    transaction: TransactionRefT<sqlx::Postgres>,
}

impl PostgresResourceEventStore {
    async fn save_event_rows(
        connection_mut: &mut sqlx::PgConnection,
        key: &ResourceStreamKey,
        account_id: &odf::AccountID,
        events: &[NewStoredResourceEvent],
    ) -> Result<EventID, SaveEventsError> {
        use odf::metadata::AsStackString;

        let account_id = account_id.as_stack_string();
        let mut last_event_id = None;

        for event in events {
            let inserted = sqlx::query_scalar!(
                r#"
                INSERT INTO resource_events (
                    resource_id,
                    account_id,
                    resource_kind,
                    event_time,
                    event_type,
                    event_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING event_id
                "#,
                key.id,
                account_id.as_str(),
                key.kind,
                event.event_time,
                event.event_type,
                event.payload,
            )
            .fetch_one(&mut *connection_mut)
            .await
            .int_err()
            .map_err(SaveEventsError::Internal)?;

            last_event_id = Some(EventID::new(inserted));
        }

        Ok(last_event_id.expect("events is not empty"))
    }

    async fn get_last_event_id(
        connection_mut: &mut sqlx::PgConnection,
        key: &ResourceStreamKey,
    ) -> Result<Option<EventID>, InternalError> {
        let last_event_id = sqlx::query_scalar!(
            r#"
            SELECT event_id
            FROM resource_events
            WHERE resource_id = $1
              AND resource_kind = $2
            ORDER BY event_id DESC
            LIMIT 1
            "#,
            key.id,
            key.kind,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map(|maybe_event_id| maybe_event_id.map(EventID::new))?;

        Ok(last_event_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceRepository for PostgresResourceEventStore {
    async fn new_resource_id(&self) -> Result<ResourceID, InternalError> {
        Ok(ResourceID::new_v4())
    }

    async fn get_resource_id_by_name(
        &self,
        _account_id: odf::AccountID,
        _kind: &str,
        _name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError> {
        Err(InternalError::new(
            "Resource lookup by name is not implemented for the event-only Postgres store",
        ))
    }

    async fn get_resource_row(
        &self,
        _key: &ResourceStreamKey,
    ) -> Result<Option<ResourceRow>, InternalError> {
        Err(InternalError::new(
            "Resource snapshot rows are not implemented for the event-only Postgres store",
        ))
    }

    fn list_resource_ids(
        &self,
        _account_id: odf::AccountID,
        _kind: &str,
        _pagination: PaginationOpts,
    ) -> ResourceIDStream<'_> {
        Box::pin(async_stream::stream! {
            yield Err(InternalError::new(
                "Resource listing is not implemented for the event-only Postgres store",
            ));
        })
    }

    async fn get_count_resources(
        &self,
        _account_id: odf::AccountID,
        _kind: &str,
    ) -> Result<usize, InternalError> {
        Err(InternalError::new(
            "Resource counting is not implemented for the event-only Postgres store",
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceEventStore for PostgresResourceEventStore {
    async fn total_events_stored(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let events_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM resource_events
            "#
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(usize::try_from(events_count).unwrap())
    }

    fn get_all_events(&self, opts: GetEventsOpts) -> StoredResourceEventStream<'_> {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut rows = sqlx::query!(
                r#"
                SELECT
                    event_id,
                    resource_id,
                    resource_kind as "kind!",
                    event_time,
                    event_type,
                    event_payload as "payload!"
                FROM resource_events
                WHERE ($1::bigint IS NULL OR event_id > $1)
                  AND ($2::bigint IS NULL OR event_id <= $2)
                ORDER BY event_id
                "#,
                opts.from.map(EventID::into_inner),
                opts.to.map(EventID::into_inner),
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = rows.try_next().await? {
                yield Ok(StoredResourceEvent {
                    event_id: EventID::new(row.event_id),
                    key: ResourceStreamKey {
                        kind: row.kind,
                        id: row.resource_id,
                    },
                    event_time: row.event_time,
                    event_type: row.event_type,
                    payload: row.payload,
                });
            }
        })
    }

    fn get_events(
        &self,
        key: &ResourceStreamKey,
        opts: GetEventsOpts,
    ) -> StoredResourceEventStream<'_> {
        let key = key.clone();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut rows = sqlx::query!(
                r#"
                SELECT
                    event_id,
                    resource_id,
                    resource_kind as "kind!",
                    event_time,
                    event_type,
                    event_payload as "payload!"
                FROM resource_events
                WHERE resource_id = $1
                  AND resource_kind = $2
                  AND ($3::bigint IS NULL OR event_id > $3)
                  AND ($4::bigint IS NULL OR event_id <= $4)
                ORDER BY event_id
                "#,
                key.id,
                key.kind,
                opts.from.map(EventID::into_inner),
                opts.to.map(EventID::into_inner),
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = rows.try_next().await? {
                yield Ok(StoredResourceEvent {
                    event_id: EventID::new(row.event_id),
                    key: ResourceStreamKey {
                        kind: row.kind,
                        id: row.resource_id,
                    },
                    event_time: row.event_time,
                    event_type: row.event_type,
                    payload: row.payload,
                });
            }
        })
    }

    async fn save_events(
        &self,
        account_id: odf::AccountID,
        key: &ResourceStreamKey,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<NewStoredResourceEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(ErrorIntoInternal::int_err)?;

        let actual_last_event_id = Self::get_last_event_id(connection_mut, key)
            .await
            .map_err(SaveEventsError::Internal)?;

        if actual_last_event_id != maybe_prev_stored_event_id {
            return Err(SaveEventsError::concurrent_modification());
        }

        Self::save_event_rows(connection_mut, key, &account_id, &events).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
