// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu::domain;
use kamu_datasets::{ViewCollectionEntriesError, ViewCollectionEntriesUseCase};

use super::{CollectionEntry, CollectionEntryConnection};
use crate::prelude::*;
use crate::queries::DatasetRequestState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Collection<'a> {
    state: &'a DatasetRequestState,
}

impl Collection<'_> {
    pub fn dataset_snapshot(
        alias: odf::DatasetAlias,
        extra_columns: Vec<ColumnInput>,
        extra_events: Vec<odf::MetadataEvent>,
    ) -> Result<odf::DatasetSnapshot, odf::schema::InvalidSchema> {
        use kamu_datasets::{CollectionEntity, DatasetColumn};

        CollectionEntity::dataset_snapshot(
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
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> Collection<'a> {
    #[graphql(skip)]
    pub fn new(state: &'a DatasetRequestState) -> Self {
        Self { state }
    }

    /// Latest state projection of the state of a collection
    #[tracing::instrument(level = "info", name = Collection_latest, skip_all)]
    pub async fn latest(&self) -> CollectionProjection<'a> {
        CollectionProjection::new(self.state, None)
    }

    /// State projection of the state of a collection at the specified point in
    /// time
    #[tracing::instrument(level = "info", name = Collection_as_of, skip_all)]
    pub async fn as_of(&self, block_hash: Multihash<'static>) -> CollectionProjection<'a> {
        CollectionProjection::new(self.state, Some(block_hash.into()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CollectionProjection<'a> {
    readable_state: &'a DatasetRequestState,
    as_of: Option<odf::Multihash>,
}

impl<'a> CollectionProjection<'a> {
    pub fn new(readable_state: &'a DatasetRequestState, as_of: Option<odf::Multihash>) -> Self {
        Self {
            readable_state,
            as_of,
        }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl CollectionProjection<'_> {
    const DEFAULT_ENTRIES_PER_PAGE: usize = 100;

    /// Returns an entry at the specified path
    #[tracing::instrument(level = "info", name = CollectionProjection_entry, skip_all)]
    pub async fn entry(
        &self,
        ctx: &Context<'_>,
        path: CollectionPath,
    ) -> Result<Option<CollectionEntry>> {
        use datafusion::logical_expr::{col, lit};

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);
        let readable_dataset = self.readable_state.resolved_dataset(ctx).await?;

        let Some(df) = query_svc
            .get_data(
                readable_dataset.clone(),
                domain::GetDataOptions {
                    block_hash: self.as_of.clone(),
                },
            )
            .await
            .int_err()?
            .df
        else {
            return Ok(None);
        };

        // Apply filters
        // Note: we are still working with a changelog here in hope to narrow down the
        // record set before projecting
        let df = df.filter(col("path").eq(lit(path.to_string()))).int_err()?;

        // Project changelog into a state
        let df = odf::utils::data::changelog::project(
            df,
            &["path".to_string()],
            &odf::metadata::DatasetVocabulary::default(),
        )
        .int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        if records.is_empty() {
            return Ok(None);
        }

        assert_eq!(records.len(), 1);
        let record = records.into_iter().next().unwrap();
        let entry = CollectionEntry::from_json(record)?;

        Ok(Some(entry))
    }

    /// Returns the state of entries as they existed at a specified point in
    /// time
    #[tracing::instrument(level = "info", name = CollectionProjection_entries, skip_all)]
    pub async fn entries(
        &self,
        ctx: &Context<'_>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<CollectionEntryConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ENTRIES_PER_PAGE);

        let view_collection_entries = from_catalog_n!(ctx, dyn ViewCollectionEntriesUseCase);
        let entries_listing = view_collection_entries
            .execute(
                self.readable_state.dataset_handle(),
                self.as_of.clone(),
                path_prefix.map(|p| p.to_string()),
                max_depth,
                Some(PaginationOpts {
                    offset: page * per_page,
                    limit: per_page,
                }),
            )
            .await
            .map_err(|e| match e {
                ViewCollectionEntriesError::Access(e) => GqlError::Access(e),
                e @ ViewCollectionEntriesError::Internal(_) => e.int_err().into(),
            })?;

        let nodes = entries_listing
            .entries
            .into_iter()
            .map(CollectionEntry::new)
            .collect::<Vec<_>>();

        Ok(CollectionEntryConnection::new(
            nodes,
            page,
            per_page,
            entries_listing.total_count,
        ))
    }

    /// Find entries that link to specified DIDs
    #[tracing::instrument(level = "info", name = CollectionProjection_entries_by_ref, skip_all, fields(refs))]
    pub async fn entries_by_ref(
        &self,
        ctx: &Context<'_>,
        refs: Vec<DatasetID<'_>>,
    ) -> Result<Vec<CollectionEntry>> {
        use datafusion::logical_expr::{col, lit};

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);
        let readable_dataset = self.readable_state.resolved_dataset(ctx).await?;

        let Some(df) = query_svc
            .get_data(
                readable_dataset.clone(),
                domain::GetDataOptions {
                    block_hash: self.as_of.clone(),
                },
            )
            .await
            .int_err()?
            .df
        else {
            return Ok(Vec::new());
        };

        // Apply filters
        // Note: we are still working with a changelog here in hope to narrow down the
        // record set before projecting
        let df = df
            .filter(col("ref").in_list(
                refs.into_iter().map(|r| lit(r.to_string())).collect(),
                false,
            ))
            .int_err()?;

        // Project changelog into a state
        let df = odf::utils::data::changelog::project(
            df,
            &["path".to_string()],
            &odf::metadata::DatasetVocabulary::default(),
        )
        .int_err()?;

        let df = df.sort(vec![col("path").sort(true, false)]).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        let nodes = records
            .into_iter()
            .map(CollectionEntry::from_json)
            .collect::<Result<_, _>>()?;

        Ok(nodes)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
