// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Duration;
use internal_error::InternalError;
use kamu_flow_system::FlowSystemEventStore;
use sqlx::postgres::PgListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const NOTIFY_CHANNEL_NAME: &str = "flow_system_events_ready";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresFlowSystemEventStore {
    pool: Arc<sqlx::PgPool>,
    listener: tokio::sync::Mutex<Option<PgListener>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl PostgresFlowSystemEventStore {
    async fn try_create_listener(&self) -> Option<PgListener> {
        match PgListener::connect_with(&self.pool).await {
            Ok(mut l) => match l.listen(NOTIFY_CHANNEL_NAME).await {
                Ok(_) => Some(l),
                Err(e) => {
                    tracing::error!(
                        error = ?e,
                        error_msg = %e,
                        "Failed to listen on channel '{NOTIFY_CHANNEL_NAME}'",
                    );
                    None
                }
            },
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    error_msg = %e,
                    "Failed to connect to PgListener"
                );
                None
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn FlowSystemEventStore)]
impl PostgresFlowSystemEventStore {
    pub fn new(pool: Arc<sqlx::PgPool>) -> Self {
        Self {
            pool,
            listener: tokio::sync::Mutex::new(None),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventStore for PostgresFlowSystemEventStore {
    async fn wait_wake(&self, timeout: Duration) -> Result<(), InternalError> {
        // Fast path: use the existing listener if present.
        if let Some(mut l) = { self.listener.lock().await.take() } {
            match tokio::time::timeout(timeout.to_std().unwrap(), l.recv()).await {
                // Got a NOTIFY — stash the listener back and return so runner can drain
                Ok(Ok(notification)) => {
                    // payload is a JSON string like: {"min":123,"max":130}
                    let payload =
                        serde_json::from_str::<serde_json::Value>(notification.payload()).ok();
                    println!("New flow system events detected, notification={payload:?}");
                    *self.listener.lock().await = Some(l);
                    return Ok(());
                }
                // Socket/conn error — drop it so we go to reconnect path below
                Ok(Err(conn_err)) => {
                    tracing::error!(
                        error = ?conn_err,
                        error_msg = %conn_err,
                        "PgListener connection error",
                    );
                    // fall through: no put-back; listener stays None
                }
                // Timed out waiting — stash it back and return (runner may sweep/back off)
                Err(_) => {
                    *self.listener.lock().await = Some(l);
                    return Ok(());
                }
            }
        }

        // No listener yet OR it broke above → try a single reconnect attempt
        if let Some(nl) = self.try_create_listener().await {
            *self.listener.lock().await = Some(nl);
            // Re-subscribed; return immediately (runner will try a drain or wait again)
            return Ok(());
        }

        // Reconnect failed — avoid tight spin
        tokio::time::sleep(timeout.to_std().unwrap()).await;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
