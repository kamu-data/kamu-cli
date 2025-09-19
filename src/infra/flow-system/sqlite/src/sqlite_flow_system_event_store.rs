// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use chrono::Duration;
use database_common::{DatabaseTransactionRunner, TransactionRefT};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system::FlowSystemEventStore;
use sqlx::Sqlite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteFlowSystemEventStore {
    catalog: dill::Catalog,
    last_seen_max_flow_system_event_id: Mutex<i64>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn FlowSystemEventStore)]
impl SqliteFlowSystemEventStore {
    pub fn new(catalog: dill::Catalog) -> Self {
        Self {
            catalog,
            last_seen_max_flow_system_event_id: Mutex::new(0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventStore for SqliteFlowSystemEventStore {
    async fn wait_wake(&self, timeout: Duration) -> Result<(), InternalError> {
        let max_id = DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional(|transaction_catalog| async move {
                let transaction: Arc<TransactionRefT<Sqlite>> =
                    transaction_catalog.get_one().unwrap();

                let mut tr = transaction.lock().await;
                let connection_mut = tr.connection_mut().await.int_err()?;

                let max_id: (Option<i64>,) =
                    sqlx::query_as("SELECT MAX(event_id) FROM flow_system_events")
                        .fetch_one(connection_mut)
                        .await
                        .unwrap_or((None,));
                let max_id = max_id.0.unwrap_or(0);
                Ok(max_id)
            })
            .await?;

        {
            let mut last = self.last_seen_max_flow_system_event_id.lock().unwrap();
            if max_id > *last {
                *last = max_id;
                println!("New flow system events detected, max_id={max_id}",);
                return Ok(()); // new work detected
            }
        }

        tokio::time::sleep(timeout.to_std().unwrap()).await;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
