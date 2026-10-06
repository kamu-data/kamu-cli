// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use chrono::{DateTime, Utc};
use database_common::{TransactionRefT, sqlite_generate_placeholders_list};
use dill::{component, interface};
use kamu_flow_system::*;
use sqlx::Sqlite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowActivationLinkRepository)]
pub struct SqliteFlowActivationLinkRepository {
    transaction: TransactionRefT<Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(sqlx::FromRow)]
struct FlowActivationLinkRow {
    upstream_flow_id: i64,
    downstream_flow_id: i64,
    activated_at: DateTime<Utc>,
}

impl From<FlowActivationLinkRow> for FlowActivationLink {
    fn from(row: FlowActivationLinkRow) -> Self {
        Self {
            // Flow IDs are always positive
            upstream_flow_id: FlowID::try_from(row.upstream_flow_id).unwrap(),
            downstream_flow_id: FlowID::try_from(row.downstream_flow_id).unwrap(),
            activated_at: row.activated_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowActivationLinkRepository for SqliteFlowActivationLinkRepository {
    async fn save_link(&self, link: &FlowActivationLink) -> Result<(), InternalError> {
        let upstream_flow_id: i64 = link.upstream_flow_id.try_into().unwrap();
        let downstream_flow_id: i64 = link.downstream_flow_id.try_into().unwrap();

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            INSERT INTO flow_activation_links (upstream_flow_id, downstream_flow_id, activated_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (upstream_flow_id, downstream_flow_id) DO NOTHING
            "#,
            upstream_flow_id,
            downstream_flow_id,
            link.activated_at,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn get_downstream_links(
        &self,
        upstream_flow_ids: &[FlowID],
    ) -> Result<Vec<FlowActivationLink>, InternalError> {
        if upstream_flow_ids.is_empty() {
            return Ok(vec![]);
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let query_str = format!(
            r#"
            SELECT upstream_flow_id, downstream_flow_id, activated_at
            FROM flow_activation_links
            WHERE upstream_flow_id IN ({})
            ORDER BY upstream_flow_id, activated_at, downstream_flow_id
            "#,
            sqlite_generate_placeholders_list(
                upstream_flow_ids.len(),
                NonZeroUsize::new(1).unwrap()
            )
        );

        let mut query = sqlx::query_as::<_, FlowActivationLinkRow>(&query_str);
        for upstream_flow_id in upstream_flow_ids {
            let upstream_flow_id: i64 = (*upstream_flow_id).try_into().unwrap();
            query = query.bind(upstream_flow_id);
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;

        Ok(rows.into_iter().map(Into::into).collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
