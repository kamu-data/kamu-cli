// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::ResultIntoInternal;
use kamu_core::{GetDataOptions, QueryService};
use kamu_datasets::{
    CollectionEntry,
    CollectionEntryListing,
    CollectionPath,
    ReadCheckedDataset,
    ViewCollectionEntriesError,
    ViewCollectionEntriesUseCase,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ViewCollectionEntriesUseCase)]
pub struct ViewCollectionEntriesUseCaseImpl {
    query_svc: Arc<dyn QueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl ViewCollectionEntriesUseCase for ViewCollectionEntriesUseCaseImpl {
    #[tracing::instrument(
        name = ViewCollectionEntriesUseCaseImpl_execute,
        skip_all,
    )]
    async fn execute(
        &self,
        collection_dataset: ReadCheckedDataset<'_>,
        as_of: Option<odf::Multihash>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        pagination: Option<PaginationOpts>,
    ) -> Result<CollectionEntryListing, ViewCollectionEntriesError> {
        use datafusion::logical_expr::{col, lit};

        let df = self
            .query_svc
            .get_data(
                (*collection_dataset).clone(),
                GetDataOptions { block_hash: as_of },
            )
            .await
            .int_err()?
            .df;

        let Some(df) = df else {
            return Ok(CollectionEntryListing::default());
        };

        // Apply filters
        // Note: we are still working with a changelog here in the hope to narrow down
        // the record set before projecting
        let df = match path_prefix {
            None => df,
            Some(path_prefix) => df
                .filter(
                    datafusion::functions::string::starts_with()
                        .call(vec![col("path"), lit(path_prefix.as_str())]),
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

        let df = if let Some(pagination) = pagination {
            df.limit(pagination.offset, Some(pagination.limit))
                .int_err()?
        } else {
            df
        };

        let records = df.collect_json_aos().await.int_err()?;

        let entries = records
            .into_iter()
            .map(CollectionEntry::from_json)
            .collect::<Result<_, _>>()?;

        Ok(CollectionEntryListing {
            list: entries,
            total_count,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
