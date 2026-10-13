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
    FLOW_SYSTEM_EVENT_AGENT_NAME,
    FlowSystemEvent,
    FlowSystemEventBridge,
    FlowSystemEventSourceType,
};
use kamu_wakeup_listener_postgres::PostgresNotificationHub;
use sqlx::Postgres;
use wakeup_listener::{HubWakeupListener, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const NOTIFY_CHANNEL_NAME: &str = "flow_system_events_ready";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresFlowSystemEventBridge {
    hub: Arc<PostgresNotificationHub>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::scope(dill::scopes::Agnostic)]
#[dill::interface(dyn FlowSystemEventBridge)]
impl PostgresFlowSystemEventBridge {
    pub fn new(hub: Arc<PostgresNotificationHub>) -> Self {
        Self { hub }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventBridge for PostgresFlowSystemEventBridge {
    fn new_wakeup_listener(&self) -> Box<dyn WakeupListener> {
        Box::new(HubWakeupListener::new(
            self.hub.clone(),
            NOTIFY_CHANNEL_NAME,
            FLOW_SYSTEM_EVENT_AGENT_NAME,
        ))
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

        // Deliver only below the oldest in-flight transaction: a transaction that
        // started writing earlier may still commit, and the cursor must not pass it

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

    #[tracing::instrument(level = "debug", skip_all)]
    async fn has_held_back_events(
        &self,
        transaction_catalog: &dill::Catalog,
    ) -> Result<bool, InternalError> {
        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        // Committed, yet at or above the oldest running transaction
        let has_held_back_events = sqlx::query_scalar!(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM flow_system_events
                WHERE
                    tx_id >= pg_snapshot_xmin(pg_current_snapshot())
                    AND pg_visible_in_snapshot(tx_id, pg_current_snapshot())
            ) AS "has_held_back_events!"
            "#,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(has_held_back_events)
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
