// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::TransactionRefT;
use dill::*;
use futures::TryStreamExt;
use kamu_flow_system::*;
use sqlx::{FromRow, Postgres, QueryBuilder};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn FlowConfigurationEventStore)]
pub struct PostgresFlowConfigurationEventStore {
    transaction: TransactionRefT<Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowConfigurationState> for PostgresFlowConfigurationEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, FlowConfigurationEvent> {
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT event_id, event_payload
                FROM flow_configuration_events
                WHERE
                    (cast($1 as INT8) IS NULL or event_id > $1)
                    AND (cast($2 as INT8) IS NULL or event_id <= $2)
                ORDER BY event_id ASC
                "#,
                maybe_from_id,
                maybe_to_id,
            ).try_map(|event_row| {
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

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT event_id, event_payload
                FROM flow_configuration_events
                WHERE flow_type = $1
                    AND scope_data = $2
                    AND (cast($3 as INT8) IS NULL or event_id > $3)
                    AND (cast($4 as INT8) IS NULL or event_id <= $4)
                ORDER BY event_id
                "#,
                flow_type,
                scope_json,
                maybe_from_id,
                maybe_to_id,
            ).try_map(|event_row| {
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
        _prev_stored_event_id: Option<EventID>, // TODO: detecting concurrent modifications
        events: Vec<FlowConfigurationEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let mut query_builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO flow_configuration_events (flow_type, scope_data, event_type, event_time, event_payload)
            "#,
        );

        let scope_data_json = serde_json::to_value(&flow_binding.scope).int_err()?;

        query_builder.push_values(events, |mut b, event| {
            b.push_bind(flow_binding.flow_type.as_str());
            b.push_bind(&scope_data_json);
            b.push_bind(event.typename());
            b.push_bind(event.event_time());
            b.push_bind(serde_json::to_value(event).unwrap());
        });

        query_builder.push("RETURNING event_id");

        #[derive(FromRow)]
        struct ResultRow {
            event_id: i64,
        }

        let rows = query_builder
            .build_query_as::<ResultRow>()
            .fetch_all(connection_mut)
            .await
            .int_err()?;
        let last_event_id = rows.last().unwrap().event_id;

        Ok(EventID::new(last_event_id))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn len(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        // Without type cast, PostgreSQL, for some unknown reason, returned type
        // `bignumeric`, which is not suitable for us
        let result = sqlx::query!(
            r#"
            SELECT COUNT(event_id) AS events_count
                FROM flow_configuration_events
            "#,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let count = usize::try_from(result.events_count.unwrap()).int_err()?;

        Ok(count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowConfigurationEventStore for PostgresFlowConfigurationEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    fn stream_all_existing_flow_bindings(&self) -> FlowBindingStream<'_> {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut query_stream = sqlx::query!(
                r#"
                WITH latest_events AS (
                    SELECT DISTINCT ON (flow_type, scope_data)
                        flow_type,
                        scope_data,
                        event_type,
                        event_payload
                    FROM flow_configuration_events
                    ORDER BY flow_type, scope_data, event_time DESC
                )
                SELECT flow_type, scope_data
                FROM latest_events
                WHERE event_type != 'FlowConfigurationEventDatasetRemoved'
                "#,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = query_stream.try_next().await? {
                let flow_binding = FlowBinding {
                    flow_type: row.flow_type,
                    scope: serde_json::from_value(row.scope_data).int_err()?,
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

        let flow_bindings = sqlx::query!(
            r#"
            SELECT DISTINCT flow_type, scope_data
                FROM flow_configuration_events
                WHERE scope_data = $1
                    AND event_type = 'FlowConfigurationEventCreated'
            "#,
            scope_json,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        flow_bindings
            .into_iter()
            .map(|row| {
                let scope: FlowScope = serde_json::from_value(row.scope_data).int_err()?;
                Ok(FlowBinding {
                    flow_type: row.flow_type,
                    scope,
                })
            })
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
