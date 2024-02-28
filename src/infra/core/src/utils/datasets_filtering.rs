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
use kamu_core::{DatasetRepository, GetDatasetError, RemoteRepositoryRegistry};
use opendatafabric::{
    DatasetHandle,
    DatasetRefAny,
    DatasetRefAnyPattern,
    DatasetRefPattern,
    RepoName,
};
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
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
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
                let matched_repos: Vec<RepoName> = remote_repo_reg
                    .get_all_repositories()
                    .filter(|repo_name| dataset_ref_pattern.is_match_repo_name(repo_name))
                    .collect();

                for repo_name in &matched_repos {
                    let static_dataset_pattern = dataset_ref_pattern.as_string_with_static_repo(repo_name);
                    if !static_dataset_pattern.contains('%') {
                        yield DatasetRefAny::from_str(static_dataset_pattern.as_str()).unwrap();
                    }
                }

                // This block will allow us to support wildcarding like rep%/acc%/net.ex%
                // and validate its existing
                // but it requires full scan of each matched repository

                // let search_options = SearchOptions {
                //     repository_names: matched_repos,
                // };
                // let search_result = search_svc.search(None, search_options).await.unwrap_or(SearchResult {
                //     datasets: vec![]
                // });
                // println!("search_result: {:?}", search_result);

                // for remote_dataset in &search_result.datasets {
                //     let dataset_ref_any = remote_dataset.alias.as_any_ref();
                //     println!("dataset_ref_any: {:?}", dataset_ref_any);
                //     if dataset_ref_pattern.is_match(&dataset_ref_any, true) {
                //         yield dataset_ref_any;
                //     }
                // }
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
                            dataset_ref_pattern.is_match(&dataset_handle.as_any_ref(), false)
                        }),
                )
            })
            .map_ok(|dataset_handle| dataset_handle.as_any_ref())
            .map_err(Into::into)
            .boxed(),
    );

    result
}
