// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain;

use super::{CollectionEntry, CollectionEntryConnection};
use crate::prelude::*;
use crate::queries::DatasetRequestStateWithOwner;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Collection {
    state: DatasetRequestStateWithOwner,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl Collection {
    #[graphql(skip)]
    pub fn new(state: DatasetRequestStateWithOwner) -> Self {
        Self { state }
    }

    /// Latest state projection of the state of collection
    #[tracing::instrument(level = "info", name = Collection_latest, skip_all)]
    pub async fn latest(&self) -> CollectionProjection {
        CollectionProjection::new(self.state.clone(), None)
    }

    /// State projection of the state of collection at the specified point in
    /// time
    #[tracing::instrument(level = "info", name = Collection_as_of, skip_all)]
    pub async fn as_of(&self, block_hash: Multihash<'static>) -> CollectionProjection {
        CollectionProjection::new(self.state.clone(), Some(block_hash.into()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CollectionProjection {
    state: DatasetRequestStateWithOwner,
    as_of: Option<odf::Multihash>,
}

impl CollectionProjection {
    pub fn new(state: DatasetRequestStateWithOwner, as_of: Option<odf::Multihash>) -> Self {
        Self { state, as_of }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl CollectionProjection {
    const DEFAULT_ENTRIES_PER_PAGE: usize = 100;

    /// Returns the state of entries as they existed at specified point in time
    #[tracing::instrument(level = "info", name = CollectionProjection_entries, skip_all)]
    pub async fn entries(
        &self,
        ctx: &Context<'_>,
        path_prefix: Option<String>,
        max_depth: Option<usize>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<CollectionEntryConnection> {
        use datafusion::logical_expr::{col, lit};

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ENTRIES_PER_PAGE);

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        let df = query_svc
            .get_data(
                &self.state.dataset_handle().as_local_ref(),
                domain::GetDataOptions {
                    block_hash: self.as_of.clone(),
                },
            )
            .await
            .int_err()?
            .df;

        let Some(df) = df else {
            return Ok(CollectionEntryConnection::new(Vec::new(), 0, 0, 0));
        };

        // Apply filters
        // Note: we are still working with a changelog here in hope to narrow down the
        // record set before projecting
        let df = match path_prefix {
            None => df,
            Some(path_prefix) => df
                .filter(
                    datafusion::functions::string::starts_with()
                        .call(vec![col("path"), lit(path_prefix)]),
                )
                .int_err()?,
        };

        let df = match max_depth {
            None => df,
            Some(_) => unimplemented!(),
        };

        // Project changelog into a state
        let df = odf::utils::data::changelog::project(
            df,
            &["path".to_string()],
            &odf::metadata::DatasetVocabulary::default(),
        )
        .int_err()?;

        let total_count = df.clone().count().await.int_err()?;
        let df = df.sort(vec![col("path").sort(true, false)]).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        let dataset = self.state.resolved_dataset(ctx).await?;
        let nodes = records
            .into_iter()
            .map(|r| CollectionEntry::from_json(dataset.clone(), r))
            .collect();

        Ok(CollectionEntryConnection::new(
            nodes,
            page,
            per_page,
            total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
