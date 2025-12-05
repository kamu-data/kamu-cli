// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_core::{GetDataOptions, QueryService};
use kamu_datasets::{
    CollectionEntry,
    CollectionPath,
    FindCollectionEntriesError,
    FindCollectionEntriesUseCase,
    ReadCheckedDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn FindCollectionEntriesUseCase)]
pub struct FindCollectionEntriesUseCaseImpl {
    query_svc: Arc<dyn QueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl FindCollectionEntriesUseCase for FindCollectionEntriesUseCaseImpl {
    #[tracing::instrument(
        name = FindCollectionEntriesUseCaseImpl_execute_find_by_path,
        skip_all,
        fields(as_of = ?as_of, path = %path)
    )]
    async fn execute_find_by_path(
        &self,
        collection_dataset: ReadCheckedDataset<'_>,
        as_of: Option<odf::Multihash>,
        path: CollectionPath,
    ) -> Result<Option<CollectionEntry>, FindCollectionEntriesError> {
        use datafusion::logical_expr::{col, lit};

        let Some(df) = self
            .query_svc
            .get_data(
                collection_dataset.clone(),
                GetDataOptions {
                    block_hash: as_of.clone(),
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

    #[tracing::instrument(
        name = FindCollectionEntriesUseCaseImpl_execute_find_by_ref,
        skip_all,
        fields(as_of = ?as_of, r#ref = ?r#ref)
    )]
    async fn execute_find_by_ref(
        &self,
        collection_dataset: ReadCheckedDataset<'_>,
        as_of: Option<odf::Multihash>,
        r#ref: &[&odf::DatasetID],
    ) -> Result<Option<CollectionEntry>, FindCollectionEntriesError> {
        let mut results = self
            .execute_find_multi_by_refs(collection_dataset, as_of, r#ref)
            .await?;
        Ok(results.pop())
    }

    #[tracing::instrument(
        name = FindCollectionEntriesUseCaseImpl_execute_find_multi_by_refs,
        skip_all,
        fields(as_of = ?as_of, refs = ?refs)
    )]
    async fn execute_find_multi_by_refs(
        &self,
        collection_dataset: ReadCheckedDataset<'_>,
        as_of: Option<odf::Multihash>,
        refs: &[&odf::DatasetID],
    ) -> Result<Vec<CollectionEntry>, FindCollectionEntriesError> {
        use datafusion::logical_expr::{col, lit};

        let Some(df) = self
            .query_svc
            .get_data(
                collection_dataset.clone(),
                GetDataOptions {
                    block_hash: as_of.clone(),
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
            .filter(col("ref").in_list(refs.iter().map(|r| lit(r.to_string())).collect(), false))
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
