// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{EventModel, TransactionRefT};
use dill::*;
use futures::TryStreamExt;
use kamu_flow_system::*;
use sqlx::{QueryBuilder, Sqlite};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn FlowConfigurationEventStore)]
pub struct SqliteFlowConfigurationEventStore {
    transaction: TransactionRefT<Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowConfigurationState> for SqliteFlowConfigurationEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, FlowConfigurationEvent> {
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query_as!(
                EventModel,
                r#"
                SELECT event_id, event_payload as "event_payload: _"
                FROM flow_configuration_events
                WHERE
                    (cast($1 as INT8) IS NULL or event_id > $1) AND
                    (cast($2 as INT8) IS NULL or event_id <= $2)
                ORDER BY event_id ASC
                "#,
                maybe_from_id,
                maybe_to_id,
            )
            .try_map(|event_row| {
                let event = serde_json::from_value::<FlowConfigurationEvent>(event_row.event_payload)
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

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_binding))]
    fn get_events(
        &self,
        flow_binding: &FlowBinding,
        opts: GetEventsOpts,
    ) -> EventStream<'_, FlowConfigurationEvent> {
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        let flow_type = flow_binding.flow_type.clone();

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
                FROM flow_configuration_events
                WHERE flow_type = $1
                    AND scope_data = $2
                    AND (cast($3 as INT8) IS NULL or event_id > $3)
                    AND (cast($4 as INT8) IS NULL or event_id <= $4)
                ORDER BY event_id
                "#,
                flow_type,
                scope_json_str,
                maybe_from_id,
                maybe_to_id,
            )
            .try_map(|event_row| {
                let event = serde_json::from_value::<FlowConfigurationEvent>(event_row.event_payload)
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

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_binding))]
    async fn save_events(
        &self,
        flow_binding: &FlowBinding,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<FlowConfigurationEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let scope_json = serde_json::to_value(&flow_binding.scope).int_err()?;
        let scope_json_str = canonical_json::to_string(&scope_json).unwrap();
        let flow_type = flow_binding.flow_type.as_str();

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;
        // Rejects an expected event that is not the binding's last one. Two writers
        // based on the same event both pass it and collide on the unique index over
        // `prev_event_id` instead
        let last_stored_event_id = sqlx::query_scalar!(
            r#"
            SELECT MAX(event_id) AS "last_event_id?: i64"
                FROM flow_configuration_events
                WHERE flow_type = $1 AND scope_data = $2
            "#,
            flow_type,
            scope_json_str,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        if last_stored_event_id != maybe_prev_stored_event_id.map(EventID::into_inner) {
            return Err(SaveEventsError::concurrent_modification());
        }

        let mut query_builder = QueryBuilder::<Sqlite>::new(
            r#"
            INSERT INTO flow_configuration_events (flow_type, scope_data, event_type, event_time, event_payload, prev_event_id)
            "#,
        );

        let batch_prev_event_id = maybe_prev_stored_event_id.map_or(0, EventID::into_inner);

        query_builder.push_values(events.into_iter().enumerate(), |mut b, (i, event)| {
            b.push_bind(flow_type);
            b.push_bind(&scope_json_str);
            b.push_bind(event.typename());
            b.push_bind(event.event_time());
            b.push_bind(serde_json::to_value(event).unwrap());
            b.push_bind((i == 0).then_some(batch_prev_event_id));
        });

        let connection_mut = tr.connection_mut().await?;
        match query_builder.build().execute(connection_mut).await {
            Ok(_) => {}
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                return Err(SaveEventsError::concurrent_modification());
            }
            Err(e) => return Err(SaveEventsError::Internal(e.int_err())),
        }

        let connection_mut = tr.connection_mut().await?;
        let actual_last_event_id =
            sqlx::query_scalar!("SELECT val FROM flow_event_global_counter WHERE name = 'global'")
                .fetch_one(connection_mut)
                .await
                .int_err()?;

        Ok(EventID::new(actual_last_event_id))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn total_events_stored(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let result = sqlx::query!(
            r#"
            SELECT COUNT(event_id) AS events_count
                FROM flow_configuration_events
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
impl FlowConfigurationEventStore for SqliteFlowConfigurationEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    fn stream_all_existing_flow_bindings(&self) -> FlowBindingStream<'_> {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut query_stream = sqlx::query!(
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
                    FROM flow_configuration_events
                )
                SELECT flow_type, scope_data as "scope_data: String"
                FROM latest_events
                WHERE row_num = 1
                AND event_type != 'FlowConfigurationEventDatasetRemoved'
                "#,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = query_stream.try_next().await? {
                let flow_binding = FlowBinding {
                    flow_type: row.flow_type,
                    scope: serde_json::from_str(&row.scope_data).int_err()?,
                };
                yield Ok(flow_binding);
            }
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_scope))]
    async fn all_bindings_for_scope(
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
                FROM flow_configuration_events
                WHERE scope_data = $1
                    AND event_type = 'FlowConfigurationEventCreated'
            "#,
            scope_json_str,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(flow_bindings
            .into_iter()
            .map(|row| FlowBinding {
                flow_type: row.flow_type,
                scope: serde_json::from_str(&row.scope_data).unwrap(),
            })
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
