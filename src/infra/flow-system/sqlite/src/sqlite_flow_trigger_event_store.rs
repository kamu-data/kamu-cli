// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use database_common::{
    EventModel,
    ReturningEventModel,
    TransactionRefT,
    sqlite_generate_placeholders_list,
};
use dill::*;
use futures::TryStreamExt;
use kamu_flow_system::*;
use sqlx::{QueryBuilder, Sqlite};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn FlowTriggerEventStore)]
pub struct SqliteFlowTriggerEventStore {
    transaction: TransactionRefT<Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowTriggerState> for SqliteFlowTriggerEventStore {
    fn get_events(
        &self,
        flow_binding: &FlowBinding,
        opts: GetEventsOpts,
    ) -> EventStream<FlowTriggerEvent> {
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        let flow_type = flow_binding.flow_type.to_string();

        let scope_json = serde_json::to_value(&flow_binding.scope).unwrap();
        let scope_json_str = canonical_json::to_string(&scope_json).unwrap();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query_as!(
                EventModel,
                r#"
                SELECT event_id, event_payload as "event_payload: _"
                FROM flow_trigger_events
                WHERE flow_type = $1
                    AND scope_data = $2
                    AND (cast($3 as INT8) IS NULL or event_id > $3)
                    AND (cast($4 as INT8) IS NULL or event_id <= $4)
                ORDER BY event_id ASC
                "#,
                flow_type,
                scope_json_str,
                maybe_from_id,
                maybe_to_id,
            )
            .try_map(|event_row| {
                let event = serde_json::from_value::<FlowTriggerEvent>(event_row.event_payload)
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
        flow_binding: &FlowBinding,
        _prev_stored_event_id: Option<EventID>, // TODO: detecting concurrent modifications
        events: Vec<FlowTriggerEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let mut query_builder = QueryBuilder::<Sqlite>::new(
            r#"
            INSERT INTO flow_trigger_events (flow_type, scope_data, event_type, event_time, event_payload)
            "#,
        );

        let scope_json = serde_json::to_value(&flow_binding.scope).int_err()?;
        let scope_json_str = canonical_json::to_string(&scope_json).unwrap();

        query_builder.push_values(events, |mut b, event| {
            b.push_bind(flow_binding.flow_type.as_str());
            b.push_bind(&scope_json_str);
            b.push_bind(event.typename());
            b.push_bind(event.event_time());
            b.push_bind(serde_json::to_value(event).unwrap());
        });

        query_builder.push("RETURNING event_id");

        let rows = query_builder
            .build_query_as::<ReturningEventModel>()
            .fetch_all(connection_mut)
            .await
            .int_err()?;

        let last_event_id = rows.last().unwrap().event_id;

        Ok(EventID::new(last_event_id))
    }

    async fn len(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let result = sqlx::query!(
            r#"
            SELECT COUNT(event_id) AS events_count
                FROM flow_trigger_events
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

#[async_trait::async_trait]
impl FlowTriggerEventStore for SqliteFlowTriggerEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    fn stream_all_active_flow_bindings(&self) -> FlowBindingStream {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            let mut rows = sqlx::query(
                r#"
                WITH latest_events AS (
                    SELECT
                        flow_type,
                        scope_data,
                        event_type,
                        event_payload,
                        ROW_NUMBER() OVER (
                            PARTITION BY flow_type, scope_data
                            ORDER BY event_time DESC
                        ) AS row_num
                    FROM flow_trigger_events
                )
                SELECT flow_type, scope_data
                FROM latest_events
                WHERE row_num = 1
                AND event_type != 'FlowTriggerEventDatasetRemoved'
                AND (
                    (event_type = 'FlowTriggerEventCreated' AND json_extract(event_payload, '$.Created.paused') = false)
                    OR
                    (event_type = 'FlowTriggerEventModified' AND json_extract(event_payload, '$.Modified.paused') = false)
                )
                "#
            )
            .fetch(connection_mut);

            use futures::StreamExt;
            use sqlx::Row;
            while let Some(row_result) = rows.next().await {
                let row = row_result.int_err()?;
                let flow_type: String = row.try_get("flow_type").int_err()?;
                let scope_data_json: String = row.try_get("scope_data").unwrap();
                let scope = serde_json::from_str(&scope_data_json).int_err()?;
                let flow_binding = FlowBinding {
                    flow_type,
                    scope,
                };
                yield Ok(flow_binding);
            }
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_scope))]
    async fn all_trigger_bindings_for_scope(
        &self,
        flow_scope: &FlowScope,
    ) -> Result<Vec<FlowBinding>, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let scope_json = serde_json::to_value(flow_scope).int_err()?;
        let scope_json_str = canonical_json::to_string(&scope_json).unwrap();

        let flow_bindings = sqlx::query!(
            r#"
            SELECT DISTINCT flow_type, scope_data as "scope_data: String"
                FROM flow_trigger_events
                WHERE scope_data = $1
                    AND event_type = 'FlowTriggerEventCreated'
            "#,
            scope_json_str,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        flow_bindings
            .into_iter()
            .map(|row| {
                let scope = serde_json::from_str::<FlowScope>(&row.scope_data).unwrap();
                Ok(FlowBinding {
                    flow_type: row.flow_type,
                    scope,
                })
            })
            .collect()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn has_active_triggers_for_scopes(
        &self,
        scopes: &[FlowScope],
    ) -> Result<bool, InternalError> {
        if scopes.is_empty() {
            return Ok(false);
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let query_str = format!(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM (
                    SELECT
                        flow_type,
                        scope_data,
                        event_type,
                        event_payload,
                        ROW_NUMBER() OVER (
                            PARTITION BY flow_type, scope_data
                            ORDER BY event_time DESC
                        ) AS row_num
                    FROM flow_trigger_events
                ) AS latest_events
                WHERE row_num = 1
                AND scope_data IN ({})
                AND event_type != 'FlowTriggerEventDatasetRemoved'
                AND (
                    (event_type = 'FlowTriggerEventCreated' AND json_extract(event_payload, '$.Created.paused') = false)
                    OR
                    (event_type = 'FlowTriggerEventModified' AND json_extract(event_payload, '$.Modified.paused') = false)
                )
            )
            "#,
            sqlite_generate_placeholders_list(scopes.len(), NonZeroUsize::new(1).unwrap())
        );

        let mut query = sqlx::query_scalar(&query_str);
        for scope in scopes {
            let scope_json = serde_json::to_value(scope).int_err()?;
            let scope_json_str = canonical_json::to_string(&scope_json).int_err()?;
            query = query.bind(scope_json_str);
        }

        let has_active_triggers: Option<bool> = query.fetch_one(connection_mut).await.int_err()?;
        Ok(has_active_triggers.unwrap_or_default())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
