// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use database_common::TransactionRefT;
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_configuration::{
    DatasetConfigurationSetBinding,
    DatasetConfigurationSetBindingRowModel,
    DatasetResourceBindingDuplicateError,
    DatasetSecretSetBindingRepository,
    ReplaceDatasetBindingsError,
};
use sqlx::{QueryBuilder, Sqlite};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetSecretSetBindingRepository)]
pub struct SqliteDatasetSecretSetBindingRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetSecretSetBindingRepository for SqliteDatasetSecretSetBindingRepository {
    async fn list_bindings(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<DatasetConfigurationSetBinding>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let stack_dataset_id_as_str = stack_dataset_id.as_str();

        let rows = sqlx::query_as!(
            DatasetConfigurationSetBindingRowModel,
            r#"
            SELECT
                dataset_id as "dataset_id: odf::DatasetID",
                resource_uid as "resource_uid: Uuid",
                binding_order as "binding_order: i64"
            FROM config_dataset_secret_set_bindings
            WHERE dataset_id = $1
            ORDER BY binding_order
            "#,
            stack_dataset_id_as_str,
        )
        .fetch_all(&mut *connection_mut)
        .await
        .int_err()?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn replace_bindings(
        &self,
        dataset_id: &odf::DatasetID,
        resource_uids: &[kamu_resources::ResourceUID],
    ) -> Result<(), ReplaceDatasetBindingsError> {
        validate_unique_bindings(dataset_id, resource_uids)?;

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let stack_dataset_id_as_str = stack_dataset_id.as_str();

        sqlx::query!(
            r#"
            DELETE FROM config_dataset_secret_set_bindings
            WHERE dataset_id = $1
            "#,
            stack_dataset_id_as_str,
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        if resource_uids.is_empty() {
            return Ok(());
        }

        let mut query_builder = QueryBuilder::<Sqlite>::new(
            r#"
            INSERT INTO config_dataset_secret_set_bindings(dataset_id, resource_uid, binding_order)
            "#,
        );

        query_builder.push_values(
            resource_uids.iter().enumerate(),
            |mut b, (binding_order, resource_uid)| {
                b.push_bind(stack_dataset_id.as_str());
                b.push_bind(resource_uid.as_ref());
                b.push_bind(i64::try_from(binding_order).unwrap());
            },
        );

        query_builder
            .build()
            .execute(&mut *connection_mut)
            .await
            .map_err(|e| match e {
                sqlx::Error::Database(e) if e.is_unique_violation() => {
                    ReplaceDatasetBindingsError::Duplicate(DatasetResourceBindingDuplicateError {
                        dataset_id: dataset_id.clone(),
                        resource_uid: resource_uids[0],
                    })
                }
                _ => ReplaceDatasetBindingsError::Internal(e.int_err()),
            })?;

        Ok(())
    }

    async fn delete_bindings_for_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let stack_dataset_id_as_str = stack_dataset_id.as_str();

        sqlx::query!(
            r#"
            DELETE FROM config_dataset_secret_set_bindings
                WHERE dataset_id = $1
            "#,
            stack_dataset_id_as_str,
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn validate_unique_bindings(
    dataset_id: &odf::DatasetID,
    resource_uids: &[kamu_resources::ResourceUID],
) -> Result<(), ReplaceDatasetBindingsError> {
    let mut seen = HashSet::new();

    for resource_uid in resource_uids {
        if !seen.insert(*resource_uid) {
            return Err(DatasetResourceBindingDuplicateError {
                dataset_id: dataset_id.clone(),
                resource_uid: *resource_uid,
            }
            .into());
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
