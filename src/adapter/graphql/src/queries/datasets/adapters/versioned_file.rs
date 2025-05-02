// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain;

use super::{FileVersion, VersionedFileEntry, VersionedFileEntryConnection};
use crate::prelude::*;
use crate::queries::DatasetRequestStateWithOwner;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VersionedFile {
    state: DatasetRequestStateWithOwner,
}

impl VersionedFile {
    pub fn new(state: DatasetRequestStateWithOwner) -> Self {
        Self { state }
    }

    pub async fn get_entry(
        &self,
        ctx: &Context<'_>,
        as_of_version: Option<FileVersion>,
        as_of_block_hash: Option<Multihash<'static>>,
    ) -> Result<Option<VersionedFileEntry>> {
        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        let query_result = if let Some(block_hash) = as_of_block_hash {
            query_svc
                .tail(
                    &self.state.dataset_handle().as_local_ref(),
                    0,
                    1,
                    domain::GetDataOptions {
                        block_hash: Some(block_hash.into()),
                    },
                )
                .await
        } else if let Some(version) = as_of_version {
            use datafusion::logical_expr::{col, lit};

            query_svc
                .get_data(
                    &self.state.dataset_handle().as_local_ref(),
                    domain::GetDataOptions::default(),
                )
                .await
                .map(|res| domain::GetDataResponse {
                    df: if res.df.schema().columns().is_empty() {
                        res.df
                    } else {
                        res.df.filter(col("version").eq(lit(version))).unwrap()
                    },
                    dataset_handle: res.dataset_handle,
                    block_hash: res.block_hash,
                })
        } else {
            query_svc
                .tail(
                    &self.state.dataset_handle().as_local_ref(),
                    0,
                    1,
                    domain::GetDataOptions::default(),
                )
                .await
        };

        let query_response = match query_result {
            Ok(res) => res,
            Err(domain::QueryError::DatasetSchemaNotAvailable(_)) => {
                return Ok(None);
            }
            Err(e) => return Err(e.int_err().into()),
        };

        let records = query_response.df.collect_json_aos().await.int_err()?;
        if records.is_empty() {
            return Ok(None);
        } else {
            assert_eq!(records.len(), 1);
            let entry = VersionedFileEntry::from_json(
                self.state.resolved_dataset(ctx).await?.clone(),
                records.into_iter().next().unwrap(),
            );
            Ok(Some(entry))
        }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl VersionedFile {
    /// Returns list of versions in reverse chronological order
    #[tracing::instrument(level = "info", name = VersionedFile_versions, skip_all)]
    pub async fn versions(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Upper version bound (inclusive)")] max_version: Option<FileVersion>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<VersionedFileEntryConnection> {
        use datafusion::logical_expr::{col, lit};

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(100);

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        let query_res = match query_svc
            .get_data(
                &self.state.dataset_handle().as_local_ref(),
                domain::GetDataOptions::default(),
            )
            .await
        {
            Ok(res) if res.df.schema().columns().is_empty() => {
                return Ok(VersionedFileEntryConnection::new(
                    Vec::new(),
                    page,
                    per_page,
                    0,
                ))
            }
            Ok(res) => res,
            Err(domain::QueryError::DatasetSchemaNotAvailable(_)) => {
                return Ok(VersionedFileEntryConnection::new(
                    Vec::new(),
                    page,
                    per_page,
                    0,
                ))
            }
            Err(err) => return Err(err.int_err().into()),
        };

        let df = query_res.df;

        let df = if let Some(max_version) = max_version {
            df.filter(col("version").lt_eq(lit(max_version)))
                .int_err()?
        } else {
            df
        };

        let total_count = df.clone().count().await.int_err()?;

        let df = df
            .sort(vec![col("version").sort(false, false)])
            .int_err()?
            .limit(page * per_page, Some(per_page))
            .int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        let dataset = self.state.resolved_dataset(ctx).await?;
        let nodes = records
            .into_iter()
            .map(|r| VersionedFileEntry::from_json(dataset.clone(), r))
            .collect();

        Ok(VersionedFileEntryConnection::new(
            nodes,
            page,
            per_page,
            total_count,
        ))
    }

    /// Returns the latest version entry, if any
    #[tracing::instrument(level = "info", name = VersionedFile_latest, skip_all)]
    pub async fn latest(&self, ctx: &Context<'_>) -> Result<Option<VersionedFileEntry>> {
        self.get_entry(ctx, None, None).await
    }

    /// Returns the specified entry by block or version number
    #[tracing::instrument(level = "info", name = VersionedFile_as_of, skip_all)]
    pub async fn as_of(
        &self,
        ctx: &Context<'_>,
        version: Option<FileVersion>,
        block_hash: Option<Multihash<'static>>,
    ) -> Result<Option<VersionedFileEntry>> {
        self.get_entry(ctx, version, block_hash).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
