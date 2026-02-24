// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use database_common::TransactionRefT;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system::{
    EventID,
    FlowSystemEvent,
    FlowSystemEventBridge,
    FlowSystemEventSourceType,
};
use kamu_messaging_outbox_postgres::PostgresMessageStoreWakeupDetector;
use messaging_outbox::MessageStoreWakeupDetector;
use sqlx::Postgres;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const NOTIFY_CHANNEL_NAME: &str = "flow_system_events_ready";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresFlowSystemEventBridge {
    wakeup_detector: PostgresMessageStoreWakeupDetector,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::scope(dill::scopes::Agnostic)]
#[dill::interface(dyn FlowSystemEventBridge)]
impl PostgresFlowSystemEventBridge {
    pub fn new(pool: Arc<sqlx::PgPool>) -> Self {
        Self {
            wakeup_detector: PostgresMessageStoreWakeupDetector::new(pool, NOTIFY_CHANNEL_NAME),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventBridge for PostgresFlowSystemEventBridge {
    /// Provides event store wakeup detector instance
    fn wakeup_detector(&self) -> &dyn MessageStoreWakeupDetector {
        &self.wakeup_detector
    }

    /// Fetch next batch for the given projector; order by global id.
    #[tracing::instrument(level = "debug", skip_all, fields(projector_name, batch_size))]
    async fn fetch_next_batch(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        batch_size: usize,
    ) -> Result<Vec<FlowSystemEvent>, InternalError> {
        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let rows = sqlx::query!(
            r#"
            WITH projected_offsets AS (
                SELECT
                    COALESCE(
                        ( SELECT last_tx_id FROM flow_system_projected_offsets WHERE projector = $1),
                        '0'::xid8
                    ) AS last_tx_id,
                    COALESCE(
                        ( SELECT last_event_id FROM flow_system_projected_offsets WHERE projector = $1),
                        0::bigint
                    ) AS last_event_id
            )

            SELECT
                event_id            AS "event_id!",
                tx_id::text::bigint AS "tx_id!: i64",
                source_stream       AS "source_stream!: String",
                event_time          AS "occurred_at!: DateTime<Utc>",
                event_payload       AS "event_payload!"
            FROM flow_system_events e, projected_offsets
            WHERE
                -- Ignore rows from txns that might still be in flight ("Usain Bolt")
                e.tx_id < pg_snapshot_xmin(pg_current_snapshot()) AND (
                    (
                        -- Same transaction as last projected event, but higher event id
                        e.tx_id = projected_offsets.last_tx_id AND
                        e.event_id > projected_offsets.last_event_id
                    ) OR
                    (
                        -- Later transaction than last projected event
                        e.tx_id > projected_offsets.last_tx_id
                    )
                )
            ORDER BY e.tx_id ASC, e.event_id ASC
            LIMIT $2
            "#,
            projector_name,
            i64::try_from(batch_size).unwrap()
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let events: Vec<FlowSystemEvent> = rows
            .into_iter()
            .map(|r| FlowSystemEvent {
                event_id: EventID::new(r.event_id),
                tx_id: r.tx_id,
                source_type: match r.source_stream.as_str() {
                    "flows" => FlowSystemEventSourceType::Flow,
                    "triggers" => FlowSystemEventSourceType::FlowTrigger,
                    "configurations" => FlowSystemEventSourceType::FlowConfiguration,
                    _ => panic!("Unknown source_stream type"),
                },
                occurred_at: r.occurred_at,
                payload: r.event_payload,
            })
            .collect();

        Ok(events)
    }

    /// Mark these events as applied for this projector (idempotent).
    #[tracing::instrument(level = "debug", skip_all, fields(projector_name))]
    async fn mark_applied(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        event_ids_with_tx_ids: &[(EventID, i64)],
    ) -> Result<(), InternalError> {
        if event_ids_with_tx_ids.is_empty() {
            return Ok(());
        }

        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        // Order (tx_id, event_id) is critical here.
        // We must write the highest event_id for the highest tx_id to ensure
        // idempotency. This means there might be events with higher event_id
        // but lower tx_id in this batch.
        //
        // I.e.:
        //   tx-id: 226813, event-id: 7004-7006, 7009-7011, 7013-7018
        //   tx-id: 226814, event-id: 7003
        //   tx-id: 226815, event-id: 7007-7008
        // Event though the highest event-id is 7018, we must record (226815, 7008) as
        // the last projected offset. Recording (226813, 7018) would cause
        // re-processing of events from tx-id 226814 and 226815!!!

        let (last_tx_id, last_event_id) = event_ids_with_tx_ids
            .iter()
            .map(|(event_id, tx_id)| (*tx_id, event_id.into_inner()))
            .max()
            .unwrap();

        sqlx::query!(
            r#"
            INSERT INTO flow_system_projected_offsets (projector, last_tx_id, last_event_id, updated_at)
                VALUES ($1, ($2)::text::xid8, $3, now())
                ON CONFLICT (projector) DO UPDATE
                SET
                    last_tx_id    = EXCLUDED.last_tx_id,
                    last_event_id = EXCLUDED.last_event_id,
                    updated_at    = now()
                WHERE (EXCLUDED.last_tx_id, EXCLUDED.last_event_id)
                    > (flow_system_projected_offsets.last_tx_id, flow_system_projected_offsets.last_event_id);
            "#,
            projector_name,
            last_tx_id.to_string(),
            last_event_id,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
