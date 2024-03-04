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
use kamu_core::{
    DatasetRepository,
    GetDatasetError,
    InternalError,
    SearchError,
    SearchOptions,
    SearchService,
};
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

////////////////////////////////////////////////////////////////////////////////

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
                        .any(|dataset_ref_pattern| dataset_ref_pattern.matches(dataset_handle)),
                )
            })
            .map_err(Into::into)
            .boxed()
    }
}

////////////////////////////////////////////////////////////////////////////////

pub fn filter_datasets_by_any_pattern(
    dataset_repo: &dyn DatasetRepository,
    search_svc: Arc<dyn SearchService>,
    dataset_ref_any_patterns: Vec<DatasetRefAnyPattern>,
) -> FilteredDatasetRefAnyStream {
    let is_multitenant_mode = dataset_repo.is_multi_tenant();

    let (all_ref_patterns, static_refs): (Vec<_>, Vec<_>) = dataset_ref_any_patterns
        .into_iter()
        .partition(DatasetRefAnyPattern::is_pattern);

    let (remote_ref_patterns, local_ref_patterns): (Vec<_>, Vec<_>) = all_ref_patterns
        .into_iter()
        .partition(|pattern| pattern.is_remote_pattern(is_multitenant_mode));

    let static_datasets_stream = get_static_datasets_stream(static_refs);
    let remote_patterns_stream =
        get_remote_datasets_stream(search_svc, remote_ref_patterns, is_multitenant_mode);
    let local_patterns_stream = get_local_datasets_stream(dataset_repo, local_ref_patterns);

    static_datasets_stream
        .chain(remote_patterns_stream)
        .chain(local_patterns_stream)
        .boxed()
}

////////////////////////////////////////////////////////////////////////////////

fn get_static_datasets_stream(
    static_refs: Vec<DatasetRefAnyPattern>,
) -> impl Stream<Item = Result<DatasetRefAny, GetDatasetError>> + 'static {
    async_stream::try_stream! {
        for static_ref in static_refs {
            yield static_ref
                .into_dataset_ref_any()
                .unwrap();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

fn get_remote_datasets_stream(
    search_svc: Arc<dyn SearchService>,
    remote_ref_patterns: Vec<DatasetRefAnyPattern>,
    is_multitenant_mode: bool,
) -> impl Stream<Item = Result<DatasetRefAny, GetDatasetError>> + 'static {
    async_stream::try_stream! {
        for remote_ref_pattern in &remote_ref_patterns {
            // TODO: potentially low performance solution,as it will always fully scan a remote repo.
            // Should be improved after search will support wildcarding.
            let repo_name = remote_ref_pattern.pattern_repo_name(is_multitenant_mode).expect("Invalid repository name");

            let search_options = SearchOptions {
                repository_names: vec![repo_name],
            };

            let remote_datasets: Vec<_> = match search_svc.search(None, search_options).await {
                Err(err) => match err {
                    SearchError::RepositoryNotFound(_) => vec![],
                    _ => Err(GetDatasetError::Internal(InternalError::new(err)))?,
                },
                Ok(result) => result.datasets,
            };

            for remote_dataset in &remote_datasets {
                if matches_remote_ref_pattern(remote_ref_pattern, &remote_dataset.alias) {
                    yield remote_dataset.alias.as_any_ref();
                }
            }
        };
    }
}

pub fn matches_remote_ref_pattern(
    remote_ref_pattern: &DatasetRefAnyPattern,
    dataset_alias_remote: &DatasetAliasRemote,
) -> bool {
    match remote_ref_pattern {
        DatasetRefAnyPattern::Ref(_) | DatasetRefAnyPattern::PatternLocal(_) => unreachable!(),
        DatasetRefAnyPattern::PatternAmbiguous(repo_name, dataset_name_pattern) => {
            let repo_name = RepoName::from_str(&repo_name.pattern).unwrap();
            repo_name == dataset_alias_remote.repo_name
                && dataset_name_pattern.matches(&dataset_alias_remote.dataset_name)
        }
        DatasetRefAnyPattern::PatternRemote(repo_name, account_name, dataset_name_pattern) => {
            repo_name == &dataset_alias_remote.repo_name
                && (dataset_alias_remote.account_name.is_some()
                    && account_name == dataset_alias_remote.account_name.as_ref().unwrap())
                && dataset_name_pattern.matches(&dataset_alias_remote.dataset_name)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

fn get_local_datasets_stream(
    dataset_repo: &dyn DatasetRepository,
    dataset_ref_patterns: Vec<DatasetRefAnyPattern>,
) -> impl Stream<Item = Result<DatasetRefAny, GetDatasetError>> + '_ {
    dataset_repo
        .get_all_datasets()
        .try_filter(move |dataset_handle| {
            future::ready(dataset_ref_patterns.iter().any(|dataset_ref_pattern| {
                matches_local_ref_pattern(dataset_ref_pattern, dataset_handle)
            }))
        })
        .map_ok(|dataset_handle| dataset_handle.as_any_ref())
        .map_err(Into::into)
}

pub fn matches_local_ref_pattern(
    local_ref_pattern: &DatasetRefAnyPattern,
    dataset_handle: &DatasetHandle,
) -> bool {
    match local_ref_pattern {
        DatasetRefAnyPattern::Ref(_) | DatasetRefAnyPattern::PatternRemote(_, _, _) => {
            unreachable!()
        }
        DatasetRefAnyPattern::PatternLocal(dataset_name_pattern) => {
            dataset_name_pattern.matches(&dataset_handle.alias.dataset_name)
        }
        DatasetRefAnyPattern::PatternAmbiguous(account_name, dataset_name_pattern) => {
            let account_name = AccountName::from_str(&account_name.pattern).unwrap();
            Some(account_name) == dataset_handle.alias.account_name
                && dataset_name_pattern.matches(&dataset_handle.alias.dataset_name)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
