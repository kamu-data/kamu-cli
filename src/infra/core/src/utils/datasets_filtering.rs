// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use futures::stream::Chain;
use futures::{future, StreamExt, TryStreamExt};
use kamu_core::{DatasetRepository, GetDatasetError, SearchOptions, SearchResult, SearchService};
use opendatafabric::{
    DatasetHandle,
    DatasetRefAny,
    DatasetRefPatternAny,
    DatasetRefPatternLocal,
    RepoName,
};
use tokio_stream::Stream;

type FilteredDatasetHandleStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetHandle, GetDatasetError>> + Send + 'a>>;

type FilteredDatasetRefAnyStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetRefAny, GetDatasetError>> + Send + 'a>>;

pub fn filter_datasets_by_local_pattern(
    dataset_repo: &dyn DatasetRepository,
    dataset_ref_patterns: Vec<DatasetRefPatternLocal>,
) -> FilteredDatasetHandleStream<'_> {
    // We assume here that resolving specific references one by one is more
    // efficient than iterating all datasets, so we iterate only if one of the
    // inputs is a glob pattern
    if !dataset_ref_patterns
        .iter()
        .any(DatasetRefPatternLocal::is_pattern)
    {
        Box::pin(async_stream::try_stream! {
            for dataset_ref_pattern in &dataset_ref_patterns {
                // TODO: PERF: Create a batch version of `resolve_dataset_ref`
                yield dataset_repo.resolve_dataset_ref(dataset_ref_pattern.as_dataset_ref().unwrap()).await?;
            }
        })
    } else {
        dataset_repo
            .get_all_datasets()
            .try_filter(move |dataset_handle| {
                future::ready(
                    dataset_ref_patterns
                        .iter()
                        .any(|dataset_ref_pattern| dataset_ref_pattern.is_match(dataset_handle)),
                )
            })
            .map_err(Into::into)
            .boxed()
    }
}

pub fn filter_datasets_by_any_pattern(
    dataset_repo: &dyn DatasetRepository,
    search_svc: Arc<dyn SearchService>,
    dataset_ref_patterns: Vec<DatasetRefPatternAny>,
) -> Chain<FilteredDatasetRefAnyStream<'_>, FilteredDatasetRefAnyStream<'_>> {
    let clone_dataset_ref_patterns = dataset_ref_patterns.clone();

    let static_stream = Box::pin(async_stream::try_stream! {
        for dataset_ref_pattern in &dataset_ref_patterns.clone() {
            if !dataset_ref_pattern.is_pattern() {
                yield dataset_ref_pattern
                    .as_dataset_ref_any()
                    .unwrap()
                    .clone();
            } else if let Some(repo_pattern) = dataset_ref_pattern.repo_pattern() {
                let search_options = if repo_pattern.contains('%') {
                    SearchOptions {
                        repository_names: vec![],
                    }
                } else {
                    SearchOptions {
                        repository_names: vec![RepoName::from_str(repo_pattern.as_ref()).unwrap()],
                    }
                };

                let dataset_name_pattern_to_search = dataset_ref_pattern.name_static_pattern().unwrap().to_owned();
                let dataset_name_to_search = dataset_name_pattern_to_search.replace('%', "");

                let remote_datasets = search_svc.search(Some(dataset_name_to_search.as_str()), search_options).await.unwrap_or(SearchResult {
                    datasets: vec![],
                });

                for dataset_alias in &remote_datasets.datasets {
                    if dataset_ref_pattern.is_match_remote(dataset_alias) {
                        yield dataset_alias.as_any_ref();
                    }
                }
            }
        };
    }) as FilteredDatasetRefAnyStream<'_>;

    let result = static_stream.chain(
        dataset_repo
            .get_all_datasets()
            .try_filter(move |dataset_handle| {
                future::ready(
                    clone_dataset_ref_patterns
                        .iter()
                        .any(|dataset_ref_pattern| {
                            dataset_ref_pattern.is_match_local(dataset_handle)
                        }),
                )
            })
            .map_ok(|dataset_handle| dataset_handle.as_any_ref())
            .map_err(Into::into)
            .boxed(),
    );

    result
}
