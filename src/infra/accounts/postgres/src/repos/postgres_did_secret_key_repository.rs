// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{TransactionRef, TransactionRefT};
use internal_error::ResultIntoInternal;
use kamu_accounts::{
    DidEntity,
    DidEntityType,
    DidSecretKey,
    DidSecretKeyRepository,
    DidSecretKeyRowModel,
    GetDidSecretKeysByCreatorIdError,
    SaveDidSecretKeyError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresDidSecretKeyRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[dill::component(pub)]
#[dill::interface(dyn DidSecretKeyRepository)]
impl PostgresDidSecretKeyRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DidSecretKeyRepository for PostgresDidSecretKeyRepository {
    async fn save_did_secret_key(
        &self,
        entity: &DidEntity,
        creator_id: &odf::AccountID,
        did_secret_key: &DidSecretKey,
    ) -> Result<(), SaveDidSecretKeyError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
                INSERT INTO did_secret_keys (
                    entity_id, entity_type, secret_key, secret_nonce, creator_id
                )
                VALUES ($1, $2, $3, $4, $5)
            "#,
            &entity.entity_id,
            entity.entity_type as DidEntityType,
            did_secret_key.secret_key,
            did_secret_key.secret_nonce,
            creator_id.to_string(),
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn get_did_secret_keys_by_creator_id(
        &self,
        creator_id: &odf::AccountID,
        entity_type_maybe: Option<DidEntityType>,
    ) -> Result<Vec<DidSecretKey>, GetDidSecretKeysByCreatorIdError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let did_secret_keys = sqlx::query_as!(
            DidSecretKeyRowModel,
            r#"
                SELECT entity_id as "entity_id: _",
                       entity_type as "entity_type: _",
                       secret_key,
                       secret_nonce
                FROM did_secret_keys
                WHERE creator_id = $1
                    AND ($2::did_entity_type IS NULL OR entity_type = $2)
                "#,
            creator_id.to_string(),
            entity_type_maybe as Option<DidEntityType>,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(did_secret_keys.into_iter().map(Into::into).collect())
    }
}
