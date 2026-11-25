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
use kamu_resources::{ResourceID, ResourceOwnerProjectionRepository};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn ResourceOwnerProjectionRepository)]
pub struct SqliteResourceOwnerProjectionRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceOwnerProjectionRepository for SqliteResourceOwnerProjectionRepository {
    async fn replace_owners(
        &self,
        resource_id: &ResourceID,
        owner_ids: &[ResourceID],
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_id: &uuid::Uuid = resource_id.as_ref();

        sqlx::query!(
            "DELETE FROM resource_owners_projection WHERE resource_id = $1",
            resource_id,
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        if owner_ids.is_empty() {
            return Ok(());
        }

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "INSERT INTO resource_owners_projection (resource_id, owner_id) ",
        );
        query_builder.push_values(owner_ids, |mut b, owner_id| {
            let owner_id: &uuid::Uuid = owner_id.as_ref();
            b.push_bind(resource_id).push_bind(owner_id);
        });

        query_builder
            .build()
            .execute(connection_mut)
            .await
            .int_err()?;

        Ok(())
    }

    async fn find_owners(
        &self,
        resource_id: &ResourceID,
    ) -> Result<Vec<ResourceID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_id: &uuid::Uuid = resource_id.as_ref();

        let rows = sqlx::query!(
            r#"
            SELECT owner_id as "owner_id: uuid::Uuid"
            FROM resource_owners_projection
            WHERE resource_id = $1
            ORDER BY owner_id
            "#,
            resource_id,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(|row| ResourceID::new(row.owner_id))
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
