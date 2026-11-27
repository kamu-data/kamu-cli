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
pub struct PostgresResourceOwnerProjectionRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceOwnerProjectionRepository for PostgresResourceOwnerProjectionRepository {
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

        let owner_ids = owner_ids.iter().map(|id| *id.as_ref()).collect::<Vec<_>>();

        sqlx::query!(
            r#"
            INSERT INTO resource_owners_projection (resource_id, owner_id)
            SELECT $1, owner_id
            FROM UNNEST($2::uuid[]) AS o(owner_id)
            "#,
            resource_id,
            &owner_ids,
        )
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
            SELECT owner_id
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
