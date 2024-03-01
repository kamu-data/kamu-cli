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

use futures::{future, StreamExt, TryStreamExt};
use kamu_core::{DatasetRepository, GetDatasetError, SearchOptions, SearchResult, SearchService};
use opendatafabric::{
    AccountName,
    DatasetAliasRemote,
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
    search_svc: Arc<dyn SearchService>,
    dataset_ref_patterns: Vec<DatasetRefAnyPattern>,
) -> FilteredDatasetRefAnyStream<'_> {
    let (global_patterns, static_refs): (Vec<_>, Vec<_>) = dataset_ref_patterns
        .into_iter()
        .partition(DatasetRefAnyPattern::is_pattern);

    let (remote_patterns, local_patterns): (Vec<_>, Vec<_>) = global_patterns
        .into_iter()
        .partition(|pattern| pattern.is_remote(dataset_repo.is_multi_tenant()));

    let static_stream = get_static_datasets_stream(static_refs);

    let result = static_stream
        .chain(get_remote_datasets_stream(
            search_svc,
            remote_patterns,
            dataset_repo.is_multi_tenant(),
        ))
        .chain(get_local_datasets_stream(dataset_repo, local_patterns))
        .boxed();

    result
}

fn get_remote_datasets_stream(
    search_svc: Arc<dyn SearchService>,
    remote_ref_patterns: Vec<DatasetRefAnyPattern>,
    is_multitenant: bool,
) -> FilteredDatasetRefAnyStream<'static> {
    Box::pin(async_stream::try_stream! {
        for remote_ref_pattern in &remote_ref_patterns.clone() {
            // ToDo low perfomance solution will always full scan remote repo
            // should be handled after search will support wildcarding
            let search_options = SearchOptions {
                repository_names: vec![remote_ref_pattern.pattern_repo_name(is_multitenant).unwrap()],
            };
            let search_result = search_svc.search(None, search_options).await.unwrap_or(SearchResult {
                datasets: vec![]
            });

            for remote_dataset in &search_result.datasets {
                if match_remote_dataset(remote_ref_pattern, &remote_dataset.alias) {
                    yield remote_dataset.alias.as_any_ref();
                }
            }
        };
    })
}

fn get_static_datasets_stream(
    static_refs: Vec<DatasetRefAnyPattern>,
) -> FilteredDatasetRefAnyStream<'static> {
    Box::pin(async_stream::try_stream! {
        for static_ref in &static_refs.clone() {
            yield static_ref
                .as_dataset_ref_any()
                .unwrap()
                .clone();
        }
    })
}

pub fn match_remote_dataset(
    remote_ref_pattern: &DatasetRefAnyPattern,
    dataset_alias_remote: &DatasetAliasRemote,
) -> bool {
    match remote_ref_pattern {
        DatasetRefAnyPattern::Ref(_) | DatasetRefAnyPattern::Local(_) => false,
        DatasetRefAnyPattern::AmbiguousAlias(repo_name, dataset_name_pattern) => {
            (RepoName::from_str(&repo_name.pattern).unwrap() == dataset_alias_remote.repo_name)
                && dataset_name_pattern.is_match(&dataset_alias_remote.dataset_name)
        }
        DatasetRefAnyPattern::RemoteAlias(repo_name, account_name, dataset_name_pattern) => {
            repo_name == &dataset_alias_remote.repo_name
                && (dataset_alias_remote.account_name.is_some()
                    && account_name == dataset_alias_remote.account_name.as_ref().unwrap())
                && dataset_name_pattern.is_match(&dataset_alias_remote.dataset_name)
        }
    }
}

fn get_local_datasets_stream(
    dataset_repo: &dyn DatasetRepository,
    dataset_ref_patterns: Vec<DatasetRefAnyPattern>,
) -> FilteredDatasetRefAnyStream<'_> {
    dataset_repo
        .get_all_datasets()
        .try_filter(move |dataset_handle| {
            future::ready(dataset_ref_patterns.iter().any(|dataset_ref_pattern| {
                match_local_dataset(dataset_ref_pattern, dataset_handle)
            }))
        })
        .map_ok(|dataset_handle| dataset_handle.as_any_ref())
        .map_err(Into::into)
        .boxed()
}

pub fn match_local_dataset(
    local_ref_pattern: &DatasetRefAnyPattern,
    dataset_handle: &DatasetHandle,
) -> bool {
    match local_ref_pattern {
        DatasetRefAnyPattern::Ref(_) | DatasetRefAnyPattern::RemoteAlias(_, _, _) => false,
        DatasetRefAnyPattern::Local(dataset_name_pattern) => {
            dataset_name_pattern.is_match(&dataset_handle.alias.dataset_name)
        }
        DatasetRefAnyPattern::AmbiguousAlias(account_name, dataset_name_pattern) => {
            (Some(AccountName::from_str(&account_name.pattern).unwrap())
                == dataset_handle.alias.account_name)
                && dataset_name_pattern.is_match(&dataset_handle.alias.dataset_name)
        }
    }
}
