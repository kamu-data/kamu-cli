// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_datasets::{
    FindVersionedFileVersionError,
    FindVersionedFileVersionUseCase,
    ReadCheckedDataset,
    ViewVersionedFileHistoryError,
    ViewVersionedFileHistoryUseCase,
};

use super::{FileVersion, VersionedFileEntry, VersionedFileEntryConnection};
use crate::prelude::*;
use crate::queries::DatasetRequestState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VersionedFile<'a> {
    readable_state: &'a DatasetRequestState,
}

impl<'a> VersionedFile<'a> {
    pub fn new(readable_state: &'a DatasetRequestState) -> Self {
        Self { readable_state }
    }

    pub fn dataset_snapshot(
        alias: odf::DatasetAlias,
        extra_columns: Vec<ColumnInput>,
        extra_events: Vec<odf::MetadataEvent>,
    ) -> Result<odf::DatasetSnapshot, odf::schema::InvalidSchema> {
        use kamu_datasets::{DatasetColumn, DatasetSnapshots};

        DatasetSnapshots::versioned_file(
            alias,
            extra_columns
                .into_iter()
                .map(|c| DatasetColumn {
                    name: c.name,
                    data_type_ddl: c.data_type.ddl,
                })
                .collect(),
            extra_events,
        )
    }

    pub async fn get_entry(
        &self,
        ctx: &Context<'_>,
        as_of_version: Option<FileVersion>,
        as_of_block_hash: Option<Multihash<'static>>,
    ) -> Result<Option<VersionedFileEntry>> {
        let readable_file_dataset = self.readable_state.resolved_dataset(ctx).await?;

        let find_versioned_file_version = from_catalog_n!(ctx, dyn FindVersionedFileVersionUseCase);
        let maybe_entry = find_versioned_file_version
            .execute(
                ReadCheckedDataset(readable_file_dataset),
                as_of_version,
                as_of_block_hash.map(Into::into),
            )
            .await
            .map_err(|e| match e {
                FindVersionedFileVersionError::Internal(err) => err.int_err(),
            })?
            .map(|entry| VersionedFileEntry::new(readable_file_dataset.clone(), entry));

        Ok(maybe_entry)
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl VersionedFile<'_> {
    const DEFAULT_VERSIONS_PER_PAGE: usize = 100;

    /// Returns list of versions in reverse chronological order
    #[tracing::instrument(level = "info", name = VersionedFile_versions, skip_all)]
    pub async fn versions(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Upper version bound (inclusive)")] max_version: Option<FileVersion>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<VersionedFileEntryConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_VERSIONS_PER_PAGE);

        let file_dataset = self.readable_state.resolved_dataset(ctx).await?;

        let view_versioned_file_history = from_catalog_n!(ctx, dyn ViewVersionedFileHistoryUseCase);
        let history_page = view_versioned_file_history
            .execute(
                ReadCheckedDataset(file_dataset),
                max_version,
                Some(PaginationOpts {
                    offset: page * per_page,
                    limit: per_page,
                }),
            )
            .await
            .map_err(|e| match e {
                ViewVersionedFileHistoryError::Internal(err) => err.int_err(),
            })?;

        let nodes = history_page
            .list
            .into_iter()
            .map(|r| VersionedFileEntry::new(file_dataset.clone(), r))
            .collect();

        Ok(VersionedFileEntryConnection::new(
            nodes,
            page,
            per_page,
            history_page.total_count,
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
