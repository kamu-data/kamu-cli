// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use database_common::{TransactionRefT, sqlite_generate_placeholders_list};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_datasets::*;
use sqlx::{QueryBuilder, Sqlite};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetDependencyRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
pub struct SqliteDatasetDependencyRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetDependencyRepository for SqliteDatasetDependencyRepository {
    async fn stores_any_dependencies(&self) -> Result<bool, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let has_data = sqlx::query_scalar!(
            r#"
            SELECT EXISTS (SELECT * FROM dataset_dependencies LIMIT 1) as has_data
            "#,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(has_data != 0)
    }

    fn list_all_dependencies(&self) -> DatasetDependenciesIDStream {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut query_stream = sqlx::query_as!(
                DatasetDependencyEntryRowModel,
                r#"
                SELECT
                    downstream_dataset_id as "downstream_dataset_id: _",
                    upstream_dataset_id as "upstream_dataset_id: _"
                FROM dataset_dependencies
                ORDER BY downstream_dataset_id, upstream_dataset_id
                "#,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            use futures::TryStreamExt;

            let mut maybe_last_downstream_id: Option<odf::DatasetID> = None;
            let mut current_upstreams = Vec::new();

            while let Some(entry) = query_stream.try_next().await? {
                if let Some(last_downstream_id) = &maybe_last_downstream_id {
                    if *last_downstream_id == entry.downstream_dataset_id {
                        current_upstreams.push(entry.upstream_dataset_id);
                        continue;
                    }

                    yield Ok(DatasetDependencies {
                        downstream_dataset_id: last_downstream_id.clone(),
                        upstream_dataset_ids: current_upstreams,
                    });

                    current_upstreams = Vec::new();
                }

                maybe_last_downstream_id = Some(entry.downstream_dataset_id);
                current_upstreams.push(entry.upstream_dataset_id);
            }

            if !current_upstreams.is_empty() {
                let last_downstream_id = maybe_last_downstream_id.expect("last downstream id to be present");
                    yield Ok(DatasetDependencies {
                        downstream_dataset_id: last_downstream_id,
                        upstream_dataset_ids: current_upstreams,
                    });
            }
        })
    }

    async fn add_upstream_dependencies(
        &self,
        downstream_dataset_id: &odf::DatasetID,
        new_upstream_dataset_ids: &[&odf::DatasetID],
    ) -> Result<(), AddDependenciesError> {
        if new_upstream_dataset_ids.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let mut query_builder = QueryBuilder::<Sqlite>::new(
            r#"
            INSERT INTO dataset_dependencies(downstream_dataset_id, upstream_dataset_id)
            "#,
        );

        query_builder.push_values(new_upstream_dataset_ids, |mut b, upsteam_dataset_id| {
            b.push_bind(downstream_dataset_id.as_did_str().to_string());
            b.push_bind(upsteam_dataset_id.as_did_str().to_string());
        });

        let query_result = query_builder.build().execute(connection_mut).await;
        if let Err(e) = query_result {
            return Err(match e {
                sqlx::Error::Database(e) if e.is_unique_violation() => {
                    AddDependencyDuplicateError {
                        downstream_dataset_id: downstream_dataset_id.clone(),
                    }
                    .into()
                }
                _ => AddDependenciesError::Internal(e.int_err()),
            });
        }

        Ok(())
    }

    async fn remove_upstream_dependencies(
        &self,
        downstream_dataset_id: &odf::DatasetID,
        upstream_dataset_ids: &[&odf::DatasetID],
    ) -> Result<(), RemoveDependenciesError> {
        if upstream_dataset_ids.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let query_str = format!(
            r#"
            DELETE FROM dataset_dependencies
            WHERE
                downstream_dataset_id = $1 AND
                upstream_dataset_id IN ({})
            "#,
            sqlite_generate_placeholders_list(
                upstream_dataset_ids.len(),
                NonZeroUsize::new(2).unwrap()
            )
        );

        // ToDo replace it by macro once sqlx will support it
        // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
        let mut query = sqlx::query(&query_str);

        let stack_downstream_dataset_id = downstream_dataset_id.as_did_str().to_stack_string();
        query = query.bind(stack_downstream_dataset_id.as_str());

        for upstream_dataset_id in upstream_dataset_ids {
            query = query.bind(upstream_dataset_id.to_string());
        }

        let delete_result = query.execute(&mut *connection_mut).await.int_err()?;
        if delete_result.rows_affected() == 0 {
            return Err(RemoveDependencyMissingError {
                downstream_dataset_id: downstream_dataset_id.clone(),
            }
            .into());
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRemovalListener for SqliteDatasetDependencyRepository {
    async fn on_dataset_entry_removed(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let stack_dataset_id_as_str = stack_dataset_id.as_str();

        sqlx::query!(
            r#"
            DELETE FROM dataset_dependencies WHERE downstream_dataset_id = $1 OR upstream_dataset_id = $1
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
