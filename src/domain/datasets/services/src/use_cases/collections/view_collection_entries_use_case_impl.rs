// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::ResultIntoInternal;
use kamu_core::{GetDataOptions, QueryService};
use kamu_datasets::{
    CollectionEntry,
    CollectionEntryListing,
    CollectionPath,
    ExtraDataFieldFilter,
    ExtraDataFieldsFilter,
    ReadCheckedDataset,
    UnknownExtraDataFieldFilterNamesError,
    ViewCollectionEntriesError,
    ViewCollectionEntriesUseCase,
};
use odf::utils::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ViewCollectionEntriesUseCase)]
pub struct ViewCollectionEntriesUseCaseImpl {
    query_svc: Arc<dyn QueryService>,
}

impl ViewCollectionEntriesUseCaseImpl {
    fn validate_requested_extra_data_fields(
        df: &DataFrameExt,
        filter: &ExtraDataFieldsFilter,
    ) -> Result<(), ViewCollectionEntriesError> {
        let available_fields = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<HashSet<_>>();
        let requested_fields = filter.iter().map(|f| &f.field_name).collect::<HashSet<_>>();

        let missing_fields = requested_fields
            .difference(&available_fields)
            .map(|f| (*f).clone())
            .collect::<Vec<_>>();

        if !missing_fields.is_empty() {
            Err(UnknownExtraDataFieldFilterNamesError {
                field_names: missing_fields,
            }
            .into())
        } else {
            Ok(())
        }
    }

    fn apply_requested_extra_data_fields_filter(
        df: DataFrameExt,
        filter: ExtraDataFieldsFilter,
    ) -> Result<DataFrameExt, ViewCollectionEntriesError> {
        use datafusion::logical_expr::{Expr, col, lit};

        Self::validate_requested_extra_data_fields(&df, &filter)?;

        let filter_expr = filter
            .into_iter()
            .map(|ExtraDataFieldFilter { field_name, values }| {
                let values_as_lits = values.into_iter().map(lit).collect();
                // field1 in [1, 2, 3]
                col(field_name).in_list(values_as_lits, false)
            })
            // ((field1 in [1, 2, 3] AND field2 in [4, 5, 6]) AND field3 in [7, 8, 9])
            .reduce(Expr::and)
            // Safety: we use the NonEmpty<T>, so we will always have elements.
            .unwrap();

        let df = df.filter(filter_expr).int_err()?;

        Ok(df)
    }
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
        filter: Option<ExtraDataFieldsFilter>,
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
        let df = if let Some(extra_data_fields_filter) = filter {
            Self::apply_requested_extra_data_fields_filter(df, extra_data_fields_filter)?
        } else {
            df
        };

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
