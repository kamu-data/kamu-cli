// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::TransactionRefT;
use dill::{component, interface};
use kamu_flow_system::*;
use sqlx::Postgres;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowActivationLinkRepository)]
pub struct PostgresFlowActivationLinkRepository {
    transaction: TransactionRefT<Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowActivationLinkRepository for PostgresFlowActivationLinkRepository {
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
        let upstream_flow_ids: Vec<i64> = upstream_flow_ids
            .iter()
            .map(|flow_id| (*flow_id).try_into().unwrap())
            .collect();

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let rows = sqlx::query!(
            r#"
            SELECT
                upstream_flow_id,
                downstream_flow_id,
                activated_at AS "activated_at: DateTime<Utc>"
            FROM flow_activation_links
            WHERE upstream_flow_id = ANY($1)
            ORDER BY upstream_flow_id, activated_at, downstream_flow_id
            "#,
            &upstream_flow_ids,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(|row| FlowActivationLink {
                // Flow IDs are always positive
                upstream_flow_id: FlowID::try_from(row.upstream_flow_id).unwrap(),
                downstream_flow_id: FlowID::try_from(row.downstream_flow_id).unwrap(),
                activated_at: row.activated_at,
            })
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
