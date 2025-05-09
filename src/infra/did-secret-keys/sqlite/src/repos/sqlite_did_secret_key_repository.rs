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
use kamu_did_secret_keys::{
    DidEntity,
    DidEntityType,
    DidSecretKey,
    DidSecretKeyRepository,
    DidSecretKeyRowModel,
    GetDidSecretKeysByCreatorIdError,
    SaveDidSecretKeyError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteDidSecretKeyRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[dill::component(pub)]
#[dill::interface(dyn DidSecretKeyRepository)]
impl SqliteDidSecretKeyRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DidSecretKeyRepository for SqliteDidSecretKeyRepository {
    async fn save_did_secret_key(
        &self,
        entity: &DidEntity,
        creator_id: &odf::AccountID,
        did_secret_key: &DidSecretKey,
    ) -> Result<(), SaveDidSecretKeyError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let creator_id = creator_id.to_string();

        sqlx::query!(
            r#"
                INSERT INTO did_secret_keys (
                    entity_id, entity_type, secret_key, secret_nonce, creator_id
                )
                VALUES ($1, $2, $3, $4, $5)
            "#,
            entity.entity_id,
            entity.entity_type,
            did_secret_key.secret_key,
            did_secret_key.secret_nonce,
            creator_id,
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
        let creator_id = creator_id.to_string();

        let did_secret_keys = sqlx::query_as!(
            DidSecretKeyRowModel,
            r#"
                SELECT entity_id as "entity_id: _",
                       entity_type as "entity_type: _",
                       secret_key,
                       secret_nonce
                FROM did_secret_keys
                WHERE creator_id = $1
                    AND (cast($2 as entity_type) IS NULL OR entity_type = $2)
                "#,
            creator_id,
            entity_type_maybe,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(did_secret_keys.into_iter().map(Into::into).collect())
    }
}
