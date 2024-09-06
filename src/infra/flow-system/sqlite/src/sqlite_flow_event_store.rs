// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use database_common::{PaginationOpts, TransactionRef, TransactionRefT};
use dill::*;
use futures::TryStreamExt;
use kamu_flow_system::*;
use opendatafabric::{AccountID, DatasetID};
use sqlx::sqlite::SqliteRow;
use sqlx::{FromRow, QueryBuilder, Row, Sqlite};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SYSTEM_INITIATOR: &str = "<system>";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteFlowEventStore {
    transaction: TransactionRefT<Sqlite>,
}

#[component(pub)]
#[interface(dyn FlowEventStore)]
impl SqliteFlowEventStore {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }

    fn prepare_initiator_filter(by_initiator: &InitiatorFilter) -> Vec<String> {
        match by_initiator {
            InitiatorFilter::System => vec![SYSTEM_INITIATOR.to_string()],
            InitiatorFilter::Account(a) => a.iter().map(ToString::to_string).collect(),
        }
    }

    fn generate_placeholders_list<T>(args: &[T], index_offset: usize) -> String {
        args.iter()
            .enumerate()
            .map(|(i, _)| format!("${}", i + index_offset))
            .collect::<Vec<_>>()
            .join(", ")
    }

    async fn save_flow_updates_from_events(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Sqlite>,
        events: &[FlowEvent],
    ) -> Result<(), SaveEventsError> {
        for event in events {
            let event_flow_id: i64 = (event.flow_id()).try_into().unwrap();

            if let FlowEvent::Initiated(e) = &event {
                let connection_mut = tr.connection_mut().await?;
                let initiator = e
                    .trigger
                    .initiator_account_id()
                    .map_or_else(|| SYSTEM_INITIATOR.to_string(), ToString::to_string);

                match &e.flow_key {
                    FlowKey::Dataset(fk_dataset) => {
                        let dataset_id = fk_dataset.dataset_id.to_string();
                        let dataset_flow_type = fk_dataset.flow_type;

                        sqlx::query!(
                            r#"
                            INSERT INTO flows (flow_id, dataset_id, dataset_flow_type, initiator, flow_status)
                                VALUES ($1, $2, $3, $4, $5)
                            "#,
                            event_flow_id,
                            dataset_id,
                            dataset_flow_type,
                            initiator,
                            FlowStatus::Waiting,
                        )
                        .execute(connection_mut)
                        .await
                        .map_err(|e| SaveEventsError::Internal(e.int_err()))?;
                    }
                    FlowKey::System(fk_system) => {
                        let system_flow_type = fk_system.flow_type;
                        sqlx::query!(
                            r#"
                            INSERT INTO flows (flow_id, system_flow_type, initiator, flow_status)
                                VALUES ($1, $2, $3, $4)
                            "#,
                            event_flow_id,
                            system_flow_type,
                            initiator,
                            FlowStatus::Waiting as FlowStatus,
                        )
                        .execute(connection_mut)
                        .await
                        .map_err(|e| SaveEventsError::Internal(e.int_err()))?;
                    }
                }
            }
            /* Existing flow must have been indexed, update status */
            else if let Some(new_status) = event.new_status() {
                let connection_mut = tr.connection_mut().await?;
                sqlx::query!(
                    r#"
                    UPDATE flows
                        SET flow_status = $2
                        WHERE flow_id = $1
                    "#,
                    event_flow_id,
                    new_status,
                )
                .execute(connection_mut)
                .await
                .map_err(|e| SaveEventsError::Internal(e.int_err()))?;
            }
        }

        Ok(())
    }

    async fn save_events_impl(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Sqlite>,
        events: &[FlowEvent],
    ) -> Result<EventID, SaveEventsError> {
        let connection_mut = tr.connection_mut().await?;

        #[derive(FromRow)]
        struct ResultRow {
            event_id: i64,
        }

        let mut query_builder = QueryBuilder::<sqlx::Sqlite>::new(
            r#"
            INSERT INTO flow_events (flow_id, event_time, event_type, event_payload)
            "#,
        );

        query_builder.push_values(events, |mut b, event| {
            let event_flow_id: i64 = (event.flow_id()).try_into().unwrap();
            b.push_bind(event_flow_id);
            b.push_bind(event.event_time());
            b.push_bind(event.typename());
            b.push_bind(serde_json::to_value(event).unwrap());
        });

        query_builder.push("RETURNING event_id");

        let rows = query_builder
            .build_query_as::<ResultRow>()
            .fetch_all(connection_mut)
            .await
            .int_err()?;
        let last_event_id = rows.last().unwrap().event_id;
        Ok(EventID::new(last_event_id))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowState> for SqliteFlowEventStore {
    fn get_events(&self, flow_id: &FlowID, opts: GetEventsOpts) -> EventStream<FlowEvent> {
        let flow_id: i64 = (*flow_id).try_into().unwrap();
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
            #[allow(dead_code)]
            pub struct EventModel {
                pub event_id: i64,
                pub event_payload: sqlx::types::JsonValue
            }

            let mut query_stream = sqlx::query_as!(
                EventModel,
                r#"
                SELECT event_id, event_payload as "event_payload: _"
                FROM flow_events
                WHERE flow_id = $1
                    AND (cast($2 as INT8) IS NULL OR event_id > $2)
                    AND (cast($3 as INT8) IS NULL OR event_id <= $3)
                "#,
                flow_id,
                maybe_from_id,
                maybe_to_id,
            ).try_map(|event_row| {
                let event = serde_json::from_value::<FlowEvent>(event_row.event_payload)
                    .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

                Ok((EventID::new(event_row.event_id), event))
            })
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some((event_id, event)) = query_stream.try_next().await? {
                yield Ok((event_id, event));
            }
        })
    }

    async fn save_events(
        &self,
        _flow_id: &FlowID,
        events: Vec<FlowEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut tr = self.transaction.lock().await;

        self.save_flow_updates_from_events(&mut tr, &events).await?;
        let last_event_id = self.save_events_impl(&mut tr, &events).await?;

        Ok(last_event_id)
    }

    async fn len(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let result = sqlx::query!(
            r#"
            SELECT COUNT(event_id) AS events_count
                FROM flow_events
            "#,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let count = usize::try_from(result.events_count).int_err()?;
        Ok(count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
#[allow(dead_code)]
pub struct NewFlow {
    pub flow_id: i64,
}

#[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
#[allow(dead_code)]
pub struct RunStatsRow {
    pub last_event_time: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowEventStore for SqliteFlowEventStore {
    async fn new_flow_id(&self) -> Result<FlowID, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let created_time = Utc::now();

        let result = sqlx::query_as!(
            NewFlow,
            r#"
            INSERT INTO flow_ids(created_time) VALUES($1) RETURNING flow_id as "flow_id: _"
            "#,
            created_time
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(FlowID::try_from(result.flow_id).unwrap())
    }

    async fn try_get_pending_flow(
        &self,
        flow_key: &FlowKey,
    ) -> Result<Option<FlowID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_flow_id = match flow_key {
            FlowKey::Dataset(flow_key_dataset) => {
                let dataset_id = flow_key_dataset.dataset_id.to_string();
                let flow_type = flow_key_dataset.flow_type;

                sqlx::query!(
                    r#"
                    SELECT flow_id FROM flows
                    WHERE dataset_id = $1 AND
                          dataset_flow_type = $2 AND
                          flow_status != 'finished'
                    ORDER BY flow_id DESC
                    LIMIT 1
                    "#,
                    dataset_id,
                    flow_type,
                )
                .fetch_optional(connection_mut)
                .await
                .int_err()?
                .map(|r| r.flow_id)
            }

            FlowKey::System(flow_key_system) => {
                let flow_type = flow_key_system.flow_type;

                sqlx::query!(
                    r#"
                    SELECT flow_id FROM flows
                        WHERE system_flow_type = $1 AND
                              flow_status != 'finished'
                        ORDER BY flow_id DESC
                        LIMIT 1
                    "#,
                    flow_type,
                )
                .fetch_optional(connection_mut)
                .await
                .int_err()?
                .map(|r| r.flow_id)
            }
        };

        Ok(maybe_flow_id.map(|id| FlowID::try_from(id).unwrap()))
    }

    async fn get_dataset_flow_run_stats(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<FlowRunStats, InternalError> {
        let mut tr = self.transaction.lock().await;

        let dataset_id = dataset_id.to_string();

        let connection_mut = tr.connection_mut().await?;
        let maybe_attempt_result = sqlx::query_as!(
            RunStatsRow,
            r#"
            SELECT attempt.last_event_time as "last_event_time: _"
            FROM (
                SELECT e.event_id as event_id, e.event_time AS last_event_time
                    FROM flow_events e
                    INNER JOIN flows f ON f.flow_id = e.flow_id
                    WHERE
                        e.event_type = 'FlowEventTaskFinished' AND
                        f.dataset_id = $1 AND
                        f.dataset_flow_type = $2
                    ORDER BY e.event_id DESC
                    LIMIT 1
            ) AS attempt
            "#,
            dataset_id,
            flow_type,
        )
        .map(|event_row| event_row.last_event_time)
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        let connection_mut = tr.connection_mut().await?;
        let maybe_success_result = sqlx::query_as!(
            RunStatsRow,
            r#"
            SELECT success.last_event_time as "last_event_time: _"
            FROM (
                SELECT e.event_id as event_id, e.event_time AS last_event_time
                    FROM flow_events e
                    INNER JOIN flows f ON f.flow_id = e.flow_id
                    WHERE
                        e.event_type = 'FlowEventTaskFinished' AND
                        e.event_payload ->> '$.TaskFinished.task_outcome.Success' IS NOT NULL AND
                        f.dataset_id = $1 AND
                        f.dataset_flow_type = $2
                    ORDER BY e.event_id DESC
                    LIMIT 1
            ) AS success
            "#,
            dataset_id,
            flow_type
        )
        .map(|event_row| event_row.last_event_time)
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(FlowRunStats {
            last_attempt_time: maybe_attempt_result,
            last_success_time: maybe_success_result,
        })
    }

    async fn get_system_flow_run_stats(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<FlowRunStats, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;
        let maybe_attempt_result = sqlx::query_as!(
            RunStatsRow,
            r#"
            SELECT attempt.last_event_time as "last_event_time: _"
            FROM (
                SELECT e.event_id as event_id, e.event_time AS last_event_time
                    FROM flow_events e
                    INNER JOIN flows f ON f.flow_id = e.flow_id
                    WHERE
                        e.event_type = 'FlowEventTaskFinished' AND
                        f.system_flow_type = $1
                    ORDER BY e.event_id DESC
                    LIMIT 1
            ) AS attempt
            "#,
            flow_type
        )
        .map(|event_row| event_row.last_event_time)
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        let connection_mut = tr.connection_mut().await?;
        let maybe_success_result = sqlx::query_as!(
            RunStatsRow,
            r#"
            SELECT success.last_event_time as "last_event_time: _"
            FROM (
                SELECT e.event_id as event_id, e.event_time AS last_event_time
                    FROM flow_events e
                    INNER JOIN flows f ON f.flow_id = e.flow_id
                    WHERE
                        e.event_type = 'FlowEventTaskFinished' AND
                        e.event_payload ->> '$.TaskFinished.task_outcome.Success' IS NOT NULL AND
                        f.system_flow_type = $1
                    ORDER BY e.event_id DESC
                    LIMIT 1
            ) AS success
            "#,
            flow_type
        )
        .map(|event_row| event_row.last_event_time)
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(FlowRunStats {
            last_attempt_time: maybe_attempt_result,
            last_success_time: maybe_success_result,
        })
    }

    fn get_all_flow_ids_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: &DatasetFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let dataset_id = dataset_id.to_string();

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_type = filters.by_flow_type;
        let maybe_by_flow_status = filters.by_flow_status;

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let query_str = format!(
                r#"
                SELECT flow_id FROM flows
                    WHERE dataset_id = $1
                    AND (cast($2 as dataset_flow_type) IS NULL OR dataset_flow_type = $2)
                    AND (cast($3 as flow_status_type) IS NULL OR flow_status = $3)
                    AND ($4 = 0 OR initiator IN ({}))
                ORDER BY flow_id DESC
                LIMIT $5 OFFSET $6
                "#,
                maybe_initiators
                    .as_ref()
                    .map(|initiators| Self::generate_placeholders_list(initiators, 7))
                    .unwrap_or_default(),
            );

            let mut query = sqlx::query(&query_str)
                .bind(dataset_id)
                .bind(maybe_by_flow_type)
                .bind(maybe_by_flow_status)
                .bind(i32::from(maybe_initiators.is_some()))
                .bind(i64::try_from(pagination.limit).unwrap())
                .bind(i64::try_from(pagination.offset).unwrap());

            if let Some(initiators) = maybe_initiators {
                for initiator in initiators {
                    query = query.bind(initiator);
                }
            }

            let mut query_stream = query.try_map(|event_row: SqliteRow| {
                Ok(FlowID::new(event_row.get(0)))
            })
            .fetch(connection_mut);

            while let Some(flow_id) = query_stream.try_next().await.int_err()? {
                yield Ok(flow_id);
            }
        })
    }

    async fn get_count_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: &DatasetFlowFilters,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let dataset_id = dataset_id.to_string();
        let maybe_filters_by_flow_type = filters.by_flow_type;
        let maybe_filters_by_flow_status = filters.by_flow_status;

        let query_str = format!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
            FROM flows
                WHERE dataset_id = $1
                AND (cast($2 as dataset_flow_type) IS NULL OR dataset_flow_type = $2)
                AND (cast($3 as flow_status_type) IS NULL OR flow_status = $3)
                AND ($4 = 0 OR initiator IN ({}))
            "#,
            maybe_initiators
                .as_ref()
                .map(|initiators| Self::generate_placeholders_list(initiators, 5))
                .unwrap_or_default()
        );

        let mut query = sqlx::query(&query_str)
            .bind(dataset_id)
            .bind(maybe_filters_by_flow_type)
            .bind(maybe_filters_by_flow_status)
            .bind(i32::from(maybe_initiators.is_some()));

        if let Some(initiators) = maybe_initiators {
            for initiator in initiators {
                query = query.bind(initiator);
            }
        }

        let query_result = query.fetch_one(connection_mut).await.int_err()?;
        let flows_count: i64 = query_result.get(0);

        Ok(usize::try_from(flows_count).unwrap())
    }

    fn get_all_flow_ids_by_datasets(
        &self,
        dataset_ids: HashSet<DatasetID>,
        filters: &DatasetFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let dataset_ids: Vec<_> = dataset_ids.iter().map(ToString::to_string).collect();

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_type = filters.by_flow_type;
        let maybe_by_flow_status = filters.by_flow_status;

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr
                .connection_mut()
                .await?;

            let query_str = format!(
                r#"
                SELECT flow_id FROM flows
                    WHERE dataset_id in ({})
                    AND (cast($1 as dataset_flow_type) IS NULL OR dataset_flow_type = $1)
                    AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                    AND ($3 = 0 OR initiator in ({}))
                ORDER BY flow_id DESC
                LIMIT $4 OFFSET $5
                "#,
                Self::generate_placeholders_list(&dataset_ids, 6),
                maybe_initiators
                    .as_ref()
                    .map(|initiators| Self::generate_placeholders_list(initiators, 6 + dataset_ids.len()))
                    .unwrap_or_default()
            );

            let mut query = sqlx::query(&query_str)
                .bind(maybe_by_flow_type)
                .bind(maybe_by_flow_status)
                .bind(i32::from(maybe_initiators.is_some()))
                .bind(i64::try_from(pagination.limit).unwrap())
                .bind(i64::try_from(pagination.offset).unwrap());

            for dataset_id in dataset_ids {
                query = query.bind(dataset_id);
            }

            if let Some(initiators) = maybe_initiators {
                for initiator in initiators {
                    query = query.bind(initiator);
                }
            }

            let mut query_stream = query.try_map(|event_row: SqliteRow| {
                Ok(FlowID::new(event_row.get(0)))
            })
            .fetch(connection_mut);

            while let Some(flow_id) = query_stream.try_next().await.int_err()? {
                yield Ok(flow_id);
            }
        })
    }

    fn get_unique_flow_initiator_ids_by_dataset(
        &self,
        dataset_id: &DatasetID,
    ) -> InitiatorIDStream {
        let dataset_id = dataset_id.to_string();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT DISTINCT(initiator) FROM flows
                    WHERE dataset_id = $1 AND initiator != $2
                "#,
                dataset_id,
                SYSTEM_INITIATOR,
            ).try_map(|event_row| {
                Ok(AccountID::from_did_str(&event_row.initiator).unwrap())
            })
            .fetch(connection_mut);

            while let Some(initiator) = query_stream.try_next().await.int_err()? {
                yield Ok(initiator);
            }
        })
    }

    fn get_all_system_flow_ids(
        &self,
        filters: &SystemFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_type = filters.by_flow_type;
        let maybe_by_flow_status = filters.by_flow_status;

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr
                .connection_mut()
                .await?;

            let query_str = format!(
                r#"
                SELECT flow_id FROM flows
                    WHERE system_flow_type IS NOT NULL
                    AND (cast($1 as system_flow_type) IS NULL OR system_flow_type = $1)
                    AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                    AND ($3 = 0 OR initiator IN ({}))
                ORDER BY flow_id DESC
                LIMIT $4 OFFSET $5
                "#,
                maybe_initiators
                    .as_ref()
                    .map(|initiators| Self::generate_placeholders_list(initiators, 6))
                    .unwrap_or_default()
            );

            let mut query = sqlx::query(&query_str)
                .bind(maybe_by_flow_type)
                .bind(maybe_by_flow_status)
                .bind(i32::from(maybe_initiators.is_some()))
                .bind(i64::try_from(pagination.limit).unwrap())
                .bind(i64::try_from(pagination.offset).unwrap());

            if let Some(initiators) = maybe_initiators {
                for initiator in initiators {
                    query = query.bind(initiator);
                }
            }

            let mut query_stream = query.try_map(|event_row: SqliteRow| {
                Ok(FlowID::new(event_row.get(0)))
            })
            .fetch(connection_mut);

            while let Some(flow_id) = query_stream.try_next().await.int_err()? {
                yield Ok(flow_id);
            }
        })
    }

    async fn get_count_system_flows(
        &self,
        filters: &SystemFlowFilters,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_by_flow_type = filters.by_flow_type;
        let maybe_by_flow_status = filters.by_flow_status;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let query_str = format!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
            FROM flows
                WHERE system_flow_type IS NOT NULL
                AND (cast($1 as system_flow_type) IS NULL OR system_flow_type = $1)
                AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                AND ($3 = 0 OR initiator IN ({}))
            "#,
            maybe_initiators
                .as_ref()
                .map(|initiators| Self::generate_placeholders_list(initiators, 4))
                .unwrap_or_default()
        );

        let mut query = sqlx::query(&query_str)
            .bind(maybe_by_flow_type)
            .bind(maybe_by_flow_status)
            .bind(i32::from(maybe_initiators.is_some()));

        if let Some(initiators) = maybe_initiators {
            for initiator in initiators {
                query = query.bind(initiator);
            }
        }

        let query_result = query.fetch_one(connection_mut).await.int_err()?;
        let flows_count: i64 = query_result.get(0);

        Ok(usize::try_from(flows_count).unwrap())
    }

    fn get_all_flow_ids(
        &self,
        filters: &AllFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream<'_> {
        let maybe_by_flow_status = filters.by_flow_status;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr
                .connection_mut()
                .await?;

            let query_str = format!(
                r#"
                SELECT flow_id FROM flows
                    WHERE
                        (cast($1 as flow_status_type) IS NULL OR flow_status = $1)
                         AND ($2 = 0 OR initiator IN ({}))
                ORDER BY flow_id DESC
                LIMIT $3 OFFSET $4
                "#,
                maybe_initiators
                    .as_ref()
                    .map(|initiators| Self::generate_placeholders_list(initiators, 5))
                    .unwrap_or_default()
            );

            let mut query = sqlx::query(&query_str)
                .bind(maybe_by_flow_status)
                .bind(i32::from(maybe_initiators.is_some()))
                .bind(i64::try_from(pagination.limit).unwrap())
                .bind(i64::try_from(pagination.offset).unwrap());

            if let Some(initiators) = maybe_initiators {
                for initiator in initiators {
                    query = query.bind(initiator);
                }
            }

            let mut query_stream = query.try_map(|event_row: SqliteRow| {
                Ok(FlowID::new(event_row.get(0)))
            })
            .fetch(connection_mut);

            while let Some(flow_id) = query_stream.try_next().await.int_err()? {
                yield Ok(flow_id);
            }
        })
    }

    async fn get_count_all_flows(&self, filters: &AllFlowFilters) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_by_flow_status = filters.by_flow_status;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let query_str = format!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
                FROM flows
                WHERE
                    (cast($1 as flow_status_type) IS NULL OR flow_status = $1)
                    AND ($2 = 0 OR initiator IN ({}))
            "#,
            maybe_initiators
                .as_ref()
                .map(|initiators| Self::generate_placeholders_list(initiators, 3))
                .unwrap_or_default()
        );

        let mut query = sqlx::query(&query_str)
            .bind(maybe_by_flow_status)
            .bind(i32::from(maybe_initiators.is_some()));

        if let Some(initiators) = maybe_initiators {
            for initiator in initiators {
                query = query.bind(initiator);
            }
        }

        let query_result = query.fetch_one(connection_mut).await.int_err()?;
        let flows_count: i64 = query_result.get(0);

        Ok(usize::try_from(flows_count).unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
