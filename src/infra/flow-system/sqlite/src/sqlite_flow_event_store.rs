// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use chrono::{DateTime, Utc};
use database_common::{
    PaginationOpts,
    TransactionRef,
    TransactionRefT,
    sqlite_generate_placeholders_list,
};
use dill::*;
use futures::TryStreamExt;
use kamu_flow_system::*;
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

    async fn register_flow(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Sqlite>,
        e: &FlowEventInitiated,
    ) -> Result<(), InternalError> {
        let connection_mut = tr.connection_mut().await?;

        let initiator = e
            .activation_cause
            .initiator_account_id()
            .map_or_else(|| SYSTEM_INITIATOR.to_string(), ToString::to_string);

        let flow_id: i64 = e.flow_id.try_into().unwrap();

        let scope_data_json = serde_json::to_value(&e.flow_binding.scope).int_err()?;

        sqlx::query!(
            r#"
            INSERT INTO flows (flow_id, flow_type, scope_data, initiator, flow_status, last_event_id)
                VALUES ($1, $2, $3, $4, 'waiting', NULL)
            "#,
            flow_id,
            e.flow_binding.flow_type,
            scope_data_json,
            initiator,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn update_flow_from_events(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Sqlite>,
        flow_id: FlowID,
        events: &[FlowEvent],
        maybe_prev_stored_event_id: Option<EventID>,
        last_event_id: EventID,
    ) -> Result<(), SaveEventsError> {
        let flow_id: i64 = flow_id.try_into().unwrap();
        let last_event_id: i64 = last_event_id.into();
        let maybe_prev_stored_event_id: Option<i64> = maybe_prev_stored_event_id.map(Into::into);

        // Determine if we have a status change between these events
        let mut maybe_latest_status = None;
        let mut maybe_scheduled_for_activation_at = None;
        let mut reset_scheduled_for_activation_at = false;

        for event in events {
            if let Some(new_status) = event.new_status() {
                maybe_latest_status = Some(new_status);
            }
            match event {
                FlowEvent::ScheduledForActivation(e) => {
                    maybe_scheduled_for_activation_at = Some(e.scheduled_for_activation_at);
                }
                FlowEvent::TaskFinished(e) => {
                    if let Some(next_activation_time) = e.next_attempt_at {
                        maybe_scheduled_for_activation_at = Some(next_activation_time);
                    }
                }
                FlowEvent::Aborted(_) | FlowEvent::TaskScheduled(_) => {
                    maybe_scheduled_for_activation_at = None;
                    reset_scheduled_for_activation_at = true;
                }
                _ => {
                    reset_scheduled_for_activation_at = false;
                }
            }

            if maybe_scheduled_for_activation_at.is_none() && !reset_scheduled_for_activation_at {
                #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
                #[allow(dead_code)]
                pub struct ActivationRow {
                    pub activation_time: Option<DateTime<Utc>>,
                }

                let connection_mut = tr.connection_mut().await?;
                maybe_scheduled_for_activation_at = sqlx::query_as!(
                    ActivationRow,
                    r#"
                    SELECT scheduled_for_activation_at as "activation_time: _"
                        FROM flows WHERE flow_id = $1
                    "#,
                    flow_id
                )
                .map(|result| result.activation_time)
                .fetch_one(connection_mut)
                .await
                .int_err()?;
            }
        }

        let connection_mut = tr.connection_mut().await?;
        let rows = sqlx::query!(
            r#"
            UPDATE flows
                SET flow_status = CASE WHEN $2 IS NOT NULL THEN $2 ELSE flow_status END,
                last_event_id = $3,
                scheduled_for_activation_at = $4
            WHERE flow_id = $1 AND (
                last_event_id IS NULL AND CAST($5 as INT8) IS NULL OR
                last_event_id IS NOT NULL AND CAST($5 as INT8) IS NOT NULL AND last_event_id = $5
            )
            RETURNING flow_id
            "#,
            flow_id,
            maybe_latest_status,
            last_event_id,
            maybe_scheduled_for_activation_at,
            maybe_prev_stored_event_id,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        // If a previously stored event id does not match the expected,
        // this means we've just detected a concurrent modification (version conflict)
        if rows.len() != 1 {
            return Err(SaveEventsError::concurrent_modification());
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
                ORDER BY event_id ASC
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

    fn get_events_multi(&self, queries: Vec<FlowID>) -> MultiEventStream<FlowID, FlowEvent> {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;


            #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
            struct EventRow {
                pub flow_id: FlowID,
                pub event_id: i64,
                pub event_payload: sqlx::types::JsonValue
            }

            let query_str = format!(
                r#"
                SELECT
                    flow_id as "flow_id: _",
                    event_id as "event_id: _",
                    event_payload as "event_payload: _"
                FROM flow_events
                    WHERE flow_id IN ({})
                    ORDER BY event_id ASC
                "#,
                sqlite_generate_placeholders_list(
                    queries.len(),
                    NonZeroUsize::new(1).unwrap()
                )
            );

            let mut query = sqlx::query(&query_str);
            for task_id in queries {
                let task_id: i64 = task_id.try_into().unwrap();
                query = query.bind(task_id);
            }

            use sqlx::Row;

            let mut query_stream = query
                .try_map(|row: sqlx::sqlite::SqliteRow| {
                    let flow_id: u64 = row.get(0);
                    let event_id: i64 = row.get(1);
                    let event_payload: sqlx::types::JsonValue = row.get(2);

                    let event = serde_json::from_value::<FlowEvent>(event_payload)
                        .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

                    Ok((FlowID::new(flow_id), EventID::new(event_id), event))
                })
                .fetch(connection_mut)
                .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some((flow_id, event_id, event)) = query_stream.try_next().await? {
                yield Ok((flow_id, event_id, event));
            }
        })
    }

    async fn save_events(
        &self,
        flow_id: &FlowID,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<FlowEvent>,
    ) -> Result<EventID, SaveEventsError> {
        // If there is nothing to save, exit quickly
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut tr = self.transaction.lock().await;

        // For the newly created flow, make sure it's registered before events
        let first_event = events.first().expect("Non empty event list expected");
        if let FlowEvent::Initiated(e) = first_event {
            assert_eq!(flow_id, &e.flow_id);

            // When creating a flow, there is no way something was already stored
            if maybe_prev_stored_event_id.is_some() {
                return Err(SaveEventsError::concurrent_modification());
            }

            // Make registration
            self.register_flow(&mut tr, e).await?;
        }

        // Save events one by one
        let last_event_id = self.save_events_impl(&mut tr, &events).await?;

        // Update denormalized flow record: latest status and stored event
        self.update_flow_from_events(
            &mut tr,
            *flow_id,
            &events,
            maybe_prev_stored_event_id,
            last_event_id,
        )
        .await?;

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
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let flow_type = flow_binding.flow_type.as_str();
        let scope_data_json = serde_json::to_value(&flow_binding.scope).int_err()?;

        let maybe_flow_id = sqlx::query!(
            r#"
            SELECT flow_id FROM flows
                WHERE
                    flow_type = $1 AND
                    scope_data = $2 AND
                    flow_status != 'finished'
                ORDER BY flow_id DESC
                LIMIT 1
            "#,
            flow_type,
            scope_data_json,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?
        .map(|r| r.flow_id);

        Ok(maybe_flow_id.map(|id| FlowID::try_from(id).unwrap()))
    }

    async fn try_get_all_dataset_pending_flows(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<FlowID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let flow_ids = sqlx::query!(
            r#"
            SELECT flow_id FROM flows
                WHERE
                    scope_data->>'dataset_id' = $1 AND
                    flow_status != 'finished'
                ORDER BY flow_id DESC
            "#,
            dataset_id_str,
        )
        .map(|row| FlowID::try_from(row.flow_id).unwrap())
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(flow_ids)
    }

    async fn try_get_all_webhook_pending_flows(
        &self,
        subscription_id: uuid::Uuid,
    ) -> Result<Vec<FlowID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let subscription_id = subscription_id.to_string();

        let flow_ids = sqlx::query!(
            r#"
            SELECT flow_id FROM flows
                WHERE
                    scope_data->>'subscription_id' = $1 AND
                    flow_status != 'finished'
                ORDER BY flow_id DESC
            "#,
            subscription_id,
        )
        .map(|row| FlowID::try_from(row.flow_id).unwrap())
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(flow_ids)
    }

    async fn get_flow_run_stats(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<FlowRunStats, InternalError> {
        let mut tr = self.transaction.lock().await;

        let flow_type = flow_binding.flow_type.as_str();
        let scope_data_json = serde_json::to_value(&flow_binding.scope).int_err()?;
        let scope_data_json_ref = &scope_data_json;

        let connection_mut = tr.connection_mut().await?;
        let maybe_attempt_result = sqlx::query_as!(
            RunStatsRow,
            r#"
            SELECT attempt.last_event_time AS "last_event_time: _"
            FROM (
                SELECT e.event_id AS event_id, e.event_time AS last_event_time
                FROM flow_events e
                INNER JOIN flows f ON f.flow_id = e.flow_id
                WHERE
                    e.event_type = 'FlowEventTaskFinished' AND
                    f.flow_type = $1 AND
                    f.scope_data = $2
                ORDER BY e.event_id DESC
                LIMIT 1
            ) AS attempt
            "#,
            flow_type,
            scope_data_json_ref,
        )
        .map(|event_row| event_row.last_event_time)
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        let connection_mut = tr.connection_mut().await?;
        let maybe_success_result = sqlx::query_as!(
            RunStatsRow,
            r#"
            SELECT success.last_event_time AS "last_event_time: _"
            FROM (
                SELECT e.event_id as event_id, e.event_time AS last_event_time
                FROM flow_events e
                INNER JOIN flows f ON f.flow_id = e.flow_id
                WHERE
                    e.event_type = 'FlowEventTaskFinished' AND
                    e.event_payload ->> '$.TaskFinished.task_outcome.Success' IS NOT NULL AND
                    f.flow_type = $1 AND
                    f.scope_data = $2
                ORDER BY e.event_id DESC
                LIMIT 1
            ) AS success
            "#,
            flow_type,
            scope_data_json_ref,
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

    /// Returns nearest time when one or more flows are scheduled for activation
    async fn nearest_flow_activation_moment(&self) -> Result<Option<DateTime<Utc>>, InternalError> {
        let mut tr = self.transaction.lock().await;

        #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
        #[allow(dead_code)]
        pub struct ActivationRow {
            pub activation_time: Option<DateTime<Utc>>,
        }

        let connection_mut = tr.connection_mut().await?;
        let maybe_activation_time = sqlx::query_as!(
            ActivationRow,
            r#"
            SELECT f.scheduled_for_activation_at as "activation_time: _"
                FROM flows f
                WHERE
                    f.scheduled_for_activation_at IS NOT NULL AND
                    (f.flow_status = 'waiting' OR f.flow_status = 'retrying')
                ORDER BY f.scheduled_for_activation_at ASC
                LIMIT 1
            "#,
        )
        .map(|result| {
            result
                .activation_time
                .expect("NULL values filtered by query")
        })
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_activation_time)
    }

    /// Returns flows scheduled for activation at the given time
    async fn get_flows_scheduled_for_activation_at(
        &self,
        scheduled_for_activation_at: DateTime<Utc>,
    ) -> Result<Vec<FlowID>, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;
        let flow_ids = sqlx::query!(
            r#"
            SELECT f.flow_id as flow_id
                FROM flows f
                WHERE
                    f.scheduled_for_activation_at = $1 AND
                    (f.flow_status = 'waiting' OR f.flow_status = 'retrying')
                ORDER BY f.flow_id ASC
            "#,
            scheduled_for_activation_at,
        )
        .map(|row| FlowID::try_from(row.flow_id).unwrap())
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(flow_ids)
    }

    fn get_all_flow_ids_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let dataset_id = dataset_id.as_did_str().to_stack_string();

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_type = filters.by_flow_type.clone();
        let maybe_by_flow_status = filters.by_flow_status;

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let query_str = format!(
                r#"
                SELECT flow_id FROM flows
                WHERE
                    scope_data->>'dataset_id' = $1
                    AND ($2::text IS NULL OR flow_type = $2)
                    AND (cast($3 as flow_status_type) IS NULL OR flow_status = $3)
                    AND ($4 = 0 OR initiator IN ({}))
                ORDER BY flow_status DESC, last_event_id DESC
                LIMIT $5 OFFSET $6
                "#,
                maybe_initiators
                    .as_ref()
                    .map(|initiators| sqlite_generate_placeholders_list(
                        initiators.len(),
                        NonZeroUsize::new(7).unwrap()
                    ))
                    .unwrap_or_default(),
            );

            let mut query = sqlx::query(&query_str)
                .bind(dataset_id.as_str())
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

            let mut query_stream = query
                .try_map(|event_row: SqliteRow| Ok(FlowID::new(event_row.get(0))))
                .fetch(connection_mut);

            while let Some(flow_id) = query_stream.try_next().await.int_err()? {
                yield Ok(flow_id);
            }
        })
    }

    async fn get_count_flows_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        filters: &FlowFilters,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let maybe_filters_by_flow_type = filters.by_flow_type.as_deref();
        let maybe_filters_by_flow_status = filters.by_flow_status;

        let query_str = format!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
            FROM flows
            WHERE
                scope_data->>'dataset_id' = $1
                AND ($2::text IS NULL OR flow_type = $2)
                AND (cast($3 as flow_status_type) IS NULL OR flow_status = $3)
                AND ($4 = 0 OR initiator IN ({}))
            "#,
            maybe_initiators
                .as_ref()
                .map(|initiators| {
                    sqlite_generate_placeholders_list(
                        initiators.len(),
                        NonZeroUsize::new(5).unwrap(),
                    )
                })
                .unwrap_or_default()
        );

        let mut query = sqlx::query(&query_str)
            .bind(dataset_id.as_str())
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
        dataset_ids: &[&odf::DatasetID],
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let dataset_ids: Vec<_> = dataset_ids.iter().map(ToString::to_string).collect();

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_type = filters.by_flow_type.clone();
        let maybe_by_flow_status = filters.by_flow_status;

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            let query_str = format!(
                r#"
                SELECT flow_id FROM flows
                WHERE
                    scope_data->>'dataset_id' in ({})
                    AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                    AND ($3 = 0 OR initiator in ({}))
                ORDER BY flow_status DESC, last_event_id DESC
                LIMIT $4 OFFSET $5
                "#,
                sqlite_generate_placeholders_list(dataset_ids.len(), NonZeroUsize::new(6).unwrap()),
                maybe_initiators
                    .as_ref()
                    .map(|initiators| sqlite_generate_placeholders_list(
                        initiators.len(),
                        NonZeroUsize::new(6 + dataset_ids.len()).unwrap()
                    ))
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

            let mut query_stream = query
                .try_map(|event_row: SqliteRow| Ok(FlowID::new(event_row.get(0))))
                .fetch(connection_mut);

            while let Some(flow_id) = query_stream.try_next().await.int_err()? {
                yield Ok(flow_id);
            }
        })
    }

    fn get_unique_flow_initiator_ids_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
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
                WHERE
                    scope_data->>'dataset_id' = $1 AND initiator != $2
                "#,
                dataset_id,
                SYSTEM_INITIATOR,
            ).try_map(|event_row| {
                Ok(odf::AccountID::from_did_str(&event_row.initiator).unwrap())
            })
            .fetch(connection_mut);

            while let Some(initiator) = query_stream.try_next().await.int_err()? {
                yield Ok(initiator);
            }
        })
    }

    fn get_all_system_flow_ids(
        &self,
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_type = filters.by_flow_type.clone();
        let maybe_by_flow_status = filters.by_flow_status;

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            let query_str = format!(
                r#"
                SELECT flow_id FROM flows
                WHERE
                    scope_data->>'type' = 'System'
                    AND ($1::text IS NULL OR flow_type = $1)
                    AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                    AND ($3 = 0 OR initiator IN ({}))
                ORDER BY flow_id DESC
                LIMIT $4 OFFSET $5
                "#,
                maybe_initiators
                    .as_ref()
                    .map(|initiators| {
                        sqlite_generate_placeholders_list(initiators.len(), NonZeroUsize::new(6).unwrap())
                    })
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

            let mut query_stream = query
                .try_map(|event_row: SqliteRow| Ok(FlowID::new(event_row.get(0))))
                .fetch(connection_mut);

            while let Some(flow_id) = query_stream.try_next().await.int_err()? {
                yield Ok(flow_id);
            }
        })
    }

    async fn get_count_system_flows(&self, filters: &FlowFilters) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_by_flow_type = filters.by_flow_type.as_deref();
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
                scope_data->>'type' = 'System'
                AND ($1::text IS NULL OR flow_type = $1)
                AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                AND ($3 = 0 OR initiator IN ({}))
            "#,
            maybe_initiators
                .as_ref()
                .map(|initiators| {
                    sqlite_generate_placeholders_list(
                        initiators.len(),
                        NonZeroUsize::new(4).unwrap(),
                    )
                })
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
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream<'_> {
        let maybe_by_flow_status = filters.by_flow_status;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_type = filters.by_flow_type.clone();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            let query_str = format!(
                r#"
                SELECT flow_id FROM flows
                WHERE
                    ($1::text IS NULL OR flow_type = $1)
                    AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                    AND ($3 = 0 OR initiator IN ({}))
                ORDER BY flow_id DESC
                LIMIT $4 OFFSET $5
                "#,
                maybe_initiators
                    .as_ref()
                    .map(|initiators| {
                        sqlite_generate_placeholders_list(initiators.len(), NonZeroUsize::new(5).unwrap())
                    })
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

            let mut query_stream = query
                .try_map(|event_row: SqliteRow| Ok(FlowID::new(event_row.get(0))))
                .fetch(connection_mut);

            while let Some(flow_id) = query_stream.try_next().await.int_err()? {
                yield Ok(flow_id);
            }
        })
    }

    async fn get_count_all_flows(&self, filters: &FlowFilters) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_by_flow_status = filters.by_flow_status;

        let maybe_by_flow_type = filters.by_flow_type.as_deref();

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let query_str = format!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
            FROM flows
            WHERE
                ($1::text IS NULL OR flow_type = $1)
                AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                AND ($3 = 0 OR initiator IN ({}))
            "#,
            maybe_initiators
                .as_ref()
                .map(|initiators| {
                    sqlite_generate_placeholders_list(
                        initiators.len(),
                        NonZeroUsize::new(3).unwrap(),
                    )
                })
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

    fn get_stream(&self, flow_ids: Vec<FlowID>) -> FlowStateStream {
        Box::pin(async_stream::try_stream! {
            const CHUNK_SIZE: usize = 256;
            for chunk in flow_ids.chunks(CHUNK_SIZE) {
                let flows = Flow::load_multi(
                    chunk.to_vec(),
                    self
                ).await.int_err()?;
                for flow in flows {
                    yield flow.int_err()?.into();
                }
            }
        })
    }

    async fn get_count_flows_by_multiple_datasets(
        &self,
        dataset_ids: &[&odf::DatasetID],
        filters: &FlowFilters,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let ids: Vec<String> = dataset_ids.iter().map(ToString::to_string).collect();

        let query_str = format!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
            FROM flows
            WHERE
                scope_data->>'dataset_id' IN ({})
                AND ($1::text IS NULL OR flow_type = $1)
                AND (cast($2 as flow_status_type) IS NULL or flow_status = $2)
                AND ($3 = 0 OR initiator IN ({}))
            "#,
            sqlite_generate_placeholders_list(ids.len(), NonZeroUsize::new(4).unwrap()),
            maybe_initiators
                .as_ref()
                .map(|initiators| sqlite_generate_placeholders_list(
                    initiators.len(),
                    NonZeroUsize::new(ids.len() + 4).unwrap()
                ))
                .unwrap_or_default()
        );

        let mut query = sqlx::query(&query_str)
            .bind(filters.by_flow_type.as_deref())
            .bind(filters.by_flow_status)
            .bind(i32::from(maybe_initiators.is_some()));

        for dataset_id in ids {
            query = query.bind(dataset_id);
        }

        if let Some(initiators) = maybe_initiators {
            for initiator in initiators {
                query = query.bind(initiator);
            }
        }

        let query_result = query.fetch_one(connection_mut).await.int_err()?;
        let flows_count: i64 = query_result.get(0);

        Ok(usize::try_from(flows_count).unwrap())
    }

    async fn filter_datasets_having_flows(
        &self,
        dataset_ids: &[&odf::DatasetID],
    ) -> Result<Vec<odf::DatasetID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let ids: Vec<String> = dataset_ids.iter().map(ToString::to_string).collect();

        let query_str = format!(
            r#"
            SELECT DISTINCT scope_data->>'dataset_id' AS dataset_id
            FROM flows
            WHERE
                scope_data->>'dataset_id' IN ({})
            "#,
            sqlite_generate_placeholders_list(ids.len(), NonZeroUsize::new(1).unwrap())
        );

        let mut query = sqlx::query(&query_str);

        for dataset_id in ids {
            query = query.bind(dataset_id);
        }

        let query_result = query.fetch_all(connection_mut).await.int_err()?;

        Ok(query_result
            .into_iter()
            .map(|row| odf::DatasetID::from_did_str(row.get(0)).unwrap())
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
