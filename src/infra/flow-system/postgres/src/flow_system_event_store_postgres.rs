// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{TransactionRef, TransactionRefT};
use dill::*;
use futures::TryStreamExt;
use kamu_flow_system::*;
use opendatafabric::DatasetID;
use sqlx::{FromRow, Postgres, QueryBuilder};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowSystemEventStorePostgres {
    transaction: TransactionRefT<Postgres>,
}

#[component(pub)]
#[interface(dyn FlowConfigurationEventStore)]
impl FlowSystemEventStorePostgres {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }

    async fn get_system_events(
        &self,
        fk_system: FlowKeySystem,
        maybe_from_id: Option<i64>,
        maybe_to_id: Option<i64>,
    ) -> EventStream<FlowConfigurationEvent> {
        let mut tr = self.transaction.lock().await;

        Box::pin(async_stream::stream! {
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT event_id, event_payload
                FROM system_flow_configuration_events
                WHERE system_flow_type = ($1::text)::system_flow_type
                    AND (cast($2 as INT8) IS NULL or event_id > $2)
                    AND (cast($3 as INT8) IS NULL or event_id <= $3)
                "#,
                fk_system.flow_type as SystemFlowType,
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

    async fn get_dataset_events(
        &self,
        fk_dataset: FlowKeyDataset,
        maybe_from_id: Option<i64>,
        maybe_to_id: Option<i64>,
    ) -> EventStream<FlowConfigurationEvent> {
        let mut tr = self.transaction.lock().await;

        Box::pin(async_stream::stream! {
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT event_id, event_payload
                FROM dataset_flow_configuration_events
                WHERE dataset_id = $1
                    AND dataset_flow_type = ($2::text)::dataset_flow_type
                    AND (cast($3 as INT8) IS NULL or event_id > $3)
                    AND (cast($4 as INT8) IS NULL or event_id <= $4)
                "#,
                fk_dataset.dataset_id.to_string(),
                fk_dataset.flow_type as DatasetFlowType,
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
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowConfigurationState> for FlowSystemEventStorePostgres {
    async fn get_events(
        &self,
        flow_key: &FlowKey,
        opts: GetEventsOpts,
    ) -> EventStream<FlowConfigurationEvent> {
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        match flow_key.clone() {
            FlowKey::Dataset(fk_dataset) => {
                self.get_dataset_events(fk_dataset, maybe_from_id, maybe_to_id)
                    .await
            }
            FlowKey::System(fk_system) => {
                self.get_system_events(fk_system, maybe_from_id, maybe_to_id)
                    .await
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
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    r#"
                    INSERT INTO dataset_flow_configuration_events (dataset_id, dataset_flow_type, event_type, event_time, event_payload)
                    "#,
                );

                query_builder.push_values(events, |mut b, event| {
                    b.push_bind(fk_dataset.dataset_id.to_string());
                    b.push_bind(fk_dataset.flow_type);
                    b.push_bind(event.typename());
                    b.push_bind(event.event_time());
                    b.push_bind(serde_json::to_value(event).unwrap());
                });

                query_builder
            }
            FlowKey::System(fk_system) => {
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    r#"
                    INSERT INTO system_flow_configuration_events (system_flow_type, event_type, event_time, event_payload)
                    "#,
                );

                query_builder.push_values(events, |mut b, event| {
                    b.push_bind(fk_system.flow_type);
                    b.push_bind(event.typename());
                    b.push_bind(event.event_time());
                    b.push_bind(serde_json::to_value(event).unwrap());
                });

                query_builder
            }
        };

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

    async fn len(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        // Without type cast, PostgreSQL, for some unknown reason, returned type
        // `bignumeric`, which is not suitable for us
        let result = sqlx::query!(
            r#"
            SELECT SUM(event_count)::BIGINT as count
            FROM (SELECT COUNT(event_id) as event_count
                  FROM dataset_flow_configuration_events
                  UNION ALL
                  SELECT COUNT(event_id) as event_count
                  FROM system_flow_configuration_events) as counts;
            "#,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let count = usize::try_from(result.count.unwrap()).int_err()?;

        Ok(count)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowConfigurationEventStore for FlowSystemEventStorePostgres {
    async fn list_all_dataset_ids(&self) -> FailableDatasetIDStream<'_> {
        let mut tr = self.transaction.lock().await;

        Box::pin(async_stream::stream! {
            let connection_mut = tr.connection_mut().await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT DISTINCT dataset_id
                FROM dataset_flow_configuration_events
                WHERE event_type = 'FlowConfigurationEventCreated'
                "#,
            )
            .try_map(|event_row| {
                DatasetID::from_did_str(event_row.dataset_id.as_str())
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

/////////////////////////////////////////////////////////////////////////////////////////
