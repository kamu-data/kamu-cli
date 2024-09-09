// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use database_common::{TransactionRef, TransactionRefT};
use dill::*;
use futures::TryStreamExt;
use kamu_flow_system::*;
use opendatafabric::DatasetID;
use sqlx::{FromRow, QueryBuilder, Sqlite};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
struct EventModel {
    event_id: i64,
    event_payload: sqlx::types::JsonValue,
}

#[derive(Debug, FromRow)]
struct ReturningEventModel {
    event_id: i64,
}

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
        fk_system: FlowKeySystem,
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
                WHERE system_flow_type = $1
                    AND (cast($2 as INT8) IS NULL or event_id > $2)
                    AND (cast($3 as INT8) IS NULL or event_id <= $3)
                ORDER BY event_id ASC
                "#,
                fk_system.flow_type,
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
        fk_dataset: FlowKeyDataset,
        maybe_from_id: Option<i64>,
        maybe_to_id: Option<i64>,
    ) -> EventStream<FlowConfigurationEvent> {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let dataset_id = fk_dataset.dataset_id.to_string();

            let mut query_stream = sqlx::query_as!(
                EventModel,
                r#"
                SELECT event_id, event_payload as "event_payload: _"
                FROM flow_configuration_events
                WHERE dataset_id = $1
                    AND dataset_flow_type = $2
                    AND (cast($3 as INT8) IS NULL or event_id > $3)
                    AND (cast($4 as INT8) IS NULL or event_id <= $4)
                ORDER BY event_id ASC
                "#,
                dataset_id,
                fk_dataset.flow_type,
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
        flow_key: &FlowKey,
        opts: GetEventsOpts,
    ) -> EventStream<FlowConfigurationEvent> {
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        match flow_key.clone() {
            FlowKey::Dataset(fk_dataset) => {
                self.get_dataset_events(fk_dataset, maybe_from_id, maybe_to_id)
            }
            FlowKey::System(fk_system) => {
                self.get_system_events(fk_system, maybe_from_id, maybe_to_id)
            }
        }
    }

    async fn save_events(
        &self,
        flow_key: &FlowKey,
        events: Vec<FlowConfigurationEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let mut query_builder = match flow_key {
            FlowKey::Dataset(fk_dataset) => {
                let mut query_builder = QueryBuilder::<Sqlite>::new(
                    r#"
                    INSERT INTO flow_configuration_events (created_time, dataset_id, dataset_flow_type, event_type, event_time, event_payload)
                    "#,
                );

                query_builder.push_values(events.into_iter(), |mut b, event| {
                    b.push_bind(Utc::now());
                    b.push_bind(fk_dataset.dataset_id.to_string());
                    b.push_bind(fk_dataset.flow_type);
                    b.push_bind(event.typename());
                    b.push_bind(event.event_time());
                    b.push_bind(serde_json::to_value(event).unwrap());
                });

                query_builder
            }
            FlowKey::System(fk_system) => {
                let mut query_builder = QueryBuilder::<Sqlite>::new(
                    r#"
                    INSERT INTO flow_configuration_events (created_time, system_flow_type, event_type, event_time, event_payload)
                    "#,
                );

                query_builder.push_values(events.into_iter(), |mut b, event| {
                    b.push_bind(Utc::now());
                    b.push_bind(fk_system.flow_type);
                    b.push_bind(event.typename());
                    b.push_bind(event.event_time());
                    b.push_bind(serde_json::to_value(event).unwrap());
                });

                query_builder
            }
        };

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
    fn list_all_dataset_ids(&self) -> FailableDatasetIDStream<'_> {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT DISTINCT dataset_id
                    FROM flow_configuration_events
                    WHERE
                        dataset_id IS NOT NULL AND
                        event_type = 'FlowConfigurationEventCreated'
                "#,
            )
            .try_map(|event_row| {
                DatasetID::from_did_str(event_row.dataset_id.unwrap().as_str())
                    .map_err(|e| sqlx::Error::Decode(Box::new(e)))
            })
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(dataset_id) = query_stream.try_next().await? {
                yield Ok(dataset_id);
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
