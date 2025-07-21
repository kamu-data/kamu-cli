// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{EventModel, ReturningEventModel, TransactionRef, TransactionRefT};
use dill::*;
use futures::TryStreamExt;
use kamu_flow_system::*;
use sqlx::types::uuid;
use sqlx::{QueryBuilder, Sqlite};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteFlowConfigurationEventStore {
    transaction: TransactionRefT<Sqlite>,
}

#[component(pub)]
#[interface(dyn FlowConfigurationEventStore)]
impl SqliteFlowConfigurationEventStore {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }

    fn get_system_events(
        &self,
        flow_type: String,
        maybe_from_id: Option<i64>,
        maybe_to_id: Option<i64>,
    ) -> EventStream<FlowConfigurationEvent> {
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
                    AND json_extract(scope_data, '$.type') = 'System'
                    AND (cast($2 as INT8) IS NULL or event_id > $2)
                    AND (cast($3 as INT8) IS NULL or event_id <= $3)
                ORDER BY event_id ASC
                "#,
                flow_type,
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

    fn get_dataset_events(
        &self,
        dataset_id: &odf::DatasetID,
        flow_type: String,
        maybe_from_id: Option<i64>,
        maybe_to_id: Option<i64>,
    ) -> EventStream<FlowConfigurationEvent> {
        let dataset_id = dataset_id.to_string();

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
                    AND json_extract(scope_data, '$.dataset_id') = $2
                    AND json_extract(scope_data, '$.type') = 'Dataset'
                    AND (cast($3 as INT8) IS NULL or event_id > $3)
                    AND (cast($4 as INT8) IS NULL or event_id <= $4)
                ORDER BY event_id ASC
                "#,
                flow_type,
                dataset_id,
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

    fn get_webhook_subscription_events(
        &self,
        webhook_subscription_id: uuid::Uuid,
        flow_type: String,
        maybe_from_id: Option<i64>,
        maybe_to_id: Option<i64>,
    ) -> EventStream<FlowConfigurationEvent> {
        let webhook_subscription_id = webhook_subscription_id.to_string();

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
                    AND json_extract(scope_data, '$.subscription_id') = $2
                    AND json_extract(scope_data, '$.type') = 'WebhookSubscription'
                    AND (cast($3 as INT8) IS NULL or event_id > $3)
                    AND (cast($4 as INT8) IS NULL or event_id <= $4)
                ORDER BY event_id ASC
                "#,
                flow_type,
                webhook_subscription_id,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowConfigurationState> for SqliteFlowConfigurationEventStore {
    fn get_events(
        &self,
        flow_binding: &FlowBinding,
        opts: GetEventsOpts,
    ) -> EventStream<FlowConfigurationEvent> {
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        match &flow_binding.scope {
            FlowScope::Dataset { dataset_id } => self.get_dataset_events(
                dataset_id,
                flow_binding.flow_type.clone(),
                maybe_from_id,
                maybe_to_id,
            ),
            FlowScope::WebhookSubscription {
                subscription_id,
                dataset_id: _,
            } => self.get_webhook_subscription_events(
                *subscription_id,
                flow_binding.flow_type.clone(),
                maybe_from_id,
                maybe_to_id,
            ),
            FlowScope::System => {
                self.get_system_events(flow_binding.flow_type.clone(), maybe_from_id, maybe_to_id)
            }
        }
    }

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

        let mut query_builder = QueryBuilder::<Sqlite>::new(
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
    fn stream_all_existing_flow_bindings(&self) -> FlowBindingStream {
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

        let flow_bindings = sqlx::query!(
            r#"
            SELECT DISTINCT flow_type, scope_data as "scope_data: String"
                FROM flow_configuration_events
                WHERE scope_data = $1
                    AND event_type = 'FlowConfigurationEventCreated'
            "#,
            scope_json,
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
