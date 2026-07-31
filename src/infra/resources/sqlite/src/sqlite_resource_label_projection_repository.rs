// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::TransactionRefT;
use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{ResourceID, ResourceLabelProjectionRepository};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn ResourceLabelProjectionRepository)]
pub struct SqliteResourceLabelProjectionRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceLabelProjectionRepository for SqliteResourceLabelProjectionRepository {
    async fn replace_entries(
        &self,
        resource_id: &ResourceID,
        entries: &[(String, String)],
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_id: &uuid::Uuid = resource_id.as_ref();

        sqlx::query!(
            "DELETE FROM resource_labels_projection WHERE resource_id = $1",
            resource_id,
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        if entries.is_empty() {
            return Ok(());
        }

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "INSERT INTO resource_labels_projection (resource_id, label_key, label_value) ",
        );
        query_builder.push_values(entries, |mut b, (key, value)| {
            b.push_bind(resource_id).push_bind(key).push_bind(value);
        });

        query_builder
            .build()
            .execute(connection_mut)
            .await
            .int_err()?;

        Ok(())
    }

    async fn find_entries(
        &self,
        resource_id: &ResourceID,
    ) -> Result<Vec<(String, String)>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_id: &uuid::Uuid = resource_id.as_ref();

        let rows = sqlx::query!(
            r#"
            SELECT label_key, label_value
            FROM resource_labels_projection
            WHERE resource_id = $1
            ORDER BY label_key
            "#,
            resource_id,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(|row| (row.label_key, row.label_value))
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
