// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;

use futures::stream::Chain;
use futures::{future, StreamExt, TryStreamExt};
use kamu_core::{DatasetRepository, GetDatasetError, SearchOptions, SearchResult, SearchService};
use opendatafabric::{DatasetHandle, DatasetRefAny, DatasetRefAnyPattern, DatasetRefPattern};
use tokio_stream::Stream;

type FilteredDatasetHandleStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetHandle, GetDatasetError>> + Send + 'a>>;

type FilteredDatasetRefAnyStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetRefAny, GetDatasetError>> + Send + 'a>>;

pub fn filter_datasets_by_local_pattern(
    dataset_repo: &dyn DatasetRepository,
    dataset_ref_patterns: Vec<DatasetRefPattern>,
) -> FilteredDatasetHandleStream<'_> {
    // We assume here that resolving specific references one by one is more
    // efficient than iterating all datasets, so we iterate only if one of the
    // inputs is a glob pattern
    if !dataset_ref_patterns
        .iter()
        .any(DatasetRefPattern::is_pattern)
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
    dataset_ref_patterns: Vec<DatasetRefAnyPattern>,
) -> Chain<FilteredDatasetRefAnyStream<'_>, FilteredDatasetRefAnyStream<'_>> {
    let clone_dataset_ref_patterns = dataset_ref_patterns.clone();

    let static_stream = Box::pin(async_stream::try_stream! {
        for dataset_ref_pattern in &dataset_ref_patterns.clone() {
            if !dataset_ref_pattern.is_pattern() {
                yield dataset_ref_pattern
                    .as_dataset_ref_any()
                    .unwrap()
                    .clone();
            } else if dataset_ref_pattern.is_remote() {
                let search_options = SearchOptions {
                    repository_names: vec![dataset_ref_pattern.pattern_repo_name().unwrap()],
                };
                let search_result = search_svc.search(None, search_options).await.unwrap_or(SearchResult {
                    datasets: vec![]
                });

                for remote_dataset in &search_result.datasets {
                    if dataset_ref_pattern.is_match_remote(&remote_dataset.alias) {
                        yield remote_dataset.alias.as_any_ref();
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
                            dataset_ref_pattern.is_match_local(&dataset_handle)
                        }),
                )
            })
            .map_ok(|dataset_handle| dataset_handle.as_any_ref())
            .map_err(Into::into)
            .boxed(),
    );

    result
}
