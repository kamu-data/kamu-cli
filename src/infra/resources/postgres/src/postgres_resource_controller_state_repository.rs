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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{
    ActionableResourcesQuery,
    ResourceControllerName,
    ResourceControllerState,
    ResourceControllerStateRepository,
    ResourceID,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn ResourceControllerStateRepository)]
pub struct PostgresResourceControllerStateRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceControllerStateRepository for PostgresResourceControllerStateRepository {
    async fn get_state(
        &self,
        controller: &ResourceControllerName,
        resource_id: &ResourceID,
    ) -> Result<Option<ResourceControllerState>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_id_uuid: &uuid::Uuid = resource_id.as_ref();

        let maybe_row = sqlx::query!(
            r#"
            SELECT
                controller_revision,
                handled_generation,
                handled_status_generation,
                not_before,
                attempts,
                last_error
            FROM resource_controller_state
            WHERE controller = $1 AND resource_id = $2
            "#,
            controller.as_str(),
            resource_id_uuid,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_row.map(|row| ResourceControllerState {
            controller: controller.clone(),
            resource_id: *resource_id,
            controller_revision: u32::try_from(row.controller_revision).unwrap(),
            handled_generation: row
                .handled_generation
                .map(|generation| u64::try_from(generation).unwrap()),
            handled_status_generation: row
                .handled_status_generation
                .map(|generation| u64::try_from(generation).unwrap()),
            not_before: row.not_before,
            attempts: u32::try_from(row.attempts).unwrap(),
            last_error: row.last_error,
        }))
    }

    async fn upsert_state(&self, state: &ResourceControllerState) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_id: &uuid::Uuid = state.resource_id.as_ref();

        sqlx::query!(
            r#"
            INSERT INTO resource_controller_state (
                controller,
                resource_id,
                controller_revision,
                handled_generation,
                handled_status_generation,
                not_before,
                attempts,
                last_error
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (controller, resource_id) DO UPDATE SET
                controller_revision = EXCLUDED.controller_revision,
                handled_generation = EXCLUDED.handled_generation,
                handled_status_generation = EXCLUDED.handled_status_generation,
                not_before = EXCLUDED.not_before,
                attempts = EXCLUDED.attempts,
                last_error = EXCLUDED.last_error
            "#,
            state.controller.as_str(),
            resource_id,
            i32::try_from(state.controller_revision).unwrap(),
            state
                .handled_generation
                .map(|generation| i64::try_from(generation).unwrap()),
            state
                .handled_status_generation
                .map(|generation| i64::try_from(generation).unwrap()),
            state.not_before,
            i32::try_from(state.attempts).unwrap(),
            state.last_error,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn delete_states_for_resource(
        &self,
        resource_id: &ResourceID,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_id: &uuid::Uuid = resource_id.as_ref();

        sqlx::query!(
            "DELETE FROM resource_controller_state WHERE resource_id = $1",
            resource_id,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn find_actionable_resource_ids(
        &self,
        query: &ActionableResourcesQuery,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<ResourceID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        // One branch per predicate, each matching its partial index on
        // `resources`; the suppression condition is the same in all of them
        let resource_ids = sqlx::query_scalar!(
            r#"
            SELECT resource_id AS "resource_id!: uuid::Uuid"
            FROM (
                (
                    SELECT r.resource_id
                    FROM resources AS r
                    LEFT JOIN resource_controller_state AS s
                        ON s.controller = $1 AND s.resource_id = r.resource_id
                    WHERE r.resource_schema = $2
                      AND r.deletion_requested_at IS NULL
                      AND COALESCE((r.status ->> 'reconciledGeneration')::BIGINT, 0) < r.generation
                      AND NOT COALESCE(
                          CASE
                              WHEN s.not_before IS NOT NULL THEN s.not_before > $5
                              ELSE s.controller_revision = $3
                                   AND s.handled_generation = r.generation
                                   AND s.handled_status_generation = r.status_generation
                          END,
                          FALSE
                      )
                    ORDER BY r.resource_id
                    LIMIT $6
                )
                UNION
                (
                    SELECT r.resource_id
                    FROM resources AS r
                    LEFT JOIN resource_controller_state AS s
                        ON s.controller = $1 AND s.resource_id = r.resource_id
                    WHERE r.resource_schema = $2
                      AND r.deletion_requested_at IS NOT NULL
                      AND r.deleted_at IS NULL
                      AND NOT COALESCE(
                          CASE
                              WHEN s.not_before IS NOT NULL THEN s.not_before > $5
                              ELSE s.controller_revision = $3
                                   AND s.handled_generation = r.generation
                                   AND s.handled_status_generation = r.status_generation
                          END,
                          FALSE
                      )
                    ORDER BY r.resource_id
                    LIMIT $6
                )
                UNION
                (
                    SELECT r.resource_id
                    FROM resources AS r
                    LEFT JOIN resource_controller_state AS s
                        ON s.controller = $1 AND s.resource_id = r.resource_id
                    WHERE $4
                      AND r.resource_schema = $2
                      AND r.deleted_at IS NULL
                      AND s.handled_status_generation IS DISTINCT FROM r.status_generation
                      AND NOT COALESCE(
                          CASE
                              WHEN s.not_before IS NOT NULL THEN s.not_before > $5
                              ELSE s.controller_revision = $3
                                   AND s.handled_generation = r.generation
                                   AND s.handled_status_generation = r.status_generation
                          END,
                          FALSE
                      )
                    ORDER BY r.resource_id
                    LIMIT $6
                )
            ) AS actionable
            ORDER BY resource_id
            LIMIT $6
            "#,
            query.controller.as_str(),
            query.schema.as_str(),
            i32::try_from(query.controller_revision).unwrap(),
            query.reacts_to_status,
            now,
            i64::try_from(limit).unwrap(),
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(resource_ids.into_iter().map(ResourceID::new).collect())
    }

    async fn find_next_deadline(
        &self,
        query: &ActionableResourcesQuery,
    ) -> Result<Option<DateTime<Utc>>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let next_deadline = sqlx::query_scalar!(
            r#"
            SELECT MIN(s.not_before)
            FROM resource_controller_state AS s
            JOIN resources AS r ON r.resource_id = s.resource_id
            WHERE s.controller = $1
              AND s.not_before IS NOT NULL
              AND r.resource_schema = $2
              AND r.deleted_at IS NULL
              AND (
                  r.deletion_requested_at IS NULL
                      AND COALESCE((r.status ->> 'reconciledGeneration')::BIGINT, 0) < r.generation
                  OR r.deletion_requested_at IS NOT NULL
                  OR $3 AND s.handled_status_generation IS DISTINCT FROM r.status_generation
              )
            "#,
            query.controller.as_str(),
            query.schema.as_str(),
            query.reacts_to_status,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(next_deadline)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
