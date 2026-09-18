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

use futures::{StreamExt, TryStreamExt, future};
use internal_error::InternalError;
use kamu_core::{SearchRemoteError, SearchRemoteOpts, SearchServiceRemote, TenancyConfig};
use kamu_datasets::DatasetRegistry;
use tokio_stream::Stream;

type FilteredDatasetHandleStream<'a> = Pin<
    Box<dyn Stream<Item = Result<odf::DatasetHandle, odf::DatasetRefUnresolvedError>> + Send + 'a>,
>;

type FilteredDatasetRefAnyStream<'a> = Pin<
    Box<dyn Stream<Item = Result<odf::DatasetRefAny, odf::DatasetRefUnresolvedError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn filter_datasets_by_local_pattern(
    dataset_registry: &dyn DatasetRegistry,
    dataset_ref_patterns: Vec<odf::DatasetRefPattern>,
) -> FilteredDatasetHandleStream<'_> {
    filter_datasets_by_local_pattern_with_unmatched_handler(
        dataset_registry,
        dataset_ref_patterns,
        move |dataset_ref_pattern, maybe_error| {
            // Preserve historical behavior:
            // - exact refs that cannot be resolved are errors
            // - patterns that cannot be resolved are not errors
            if !dataset_ref_pattern.is_pattern()
                && let Some(error) = maybe_error
            {
                return Err(error);
            }

            Ok(())
        },
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn filter_datasets_by_local_pattern_with_unmatched_handler<'a, F>(
    dataset_registry: &'a dyn DatasetRegistry,
    dataset_ref_patterns: Vec<odf::DatasetRefPattern>,
    on_unmatched_pattern: F,
) -> FilteredDatasetHandleStream<'a>
where
    F: FnMut(
            &odf::DatasetRefPattern,
            Option<odf::DatasetRefUnresolvedError>,
        ) -> Result<(), odf::DatasetRefUnresolvedError>
        + Send
        + 'a,
{
    let has_glob_patterns = dataset_ref_patterns
        .iter()
        .any(odf::DatasetRefPattern::is_pattern);

    if !has_glob_patterns {
        Box::pin(async_stream::try_stream! {
            let mut on_unmatched_pattern = on_unmatched_pattern;

            // TODO: PERF: This could be optimized by using batch resolution
            // `resolve_dataset_handles_by_refs`, but the batch API currently returns
            // response keys that are re-normalized (e.g. `DatasetRef::Handle` instead of
            // the input `DatasetRef::Alias`), so we cannot reliably correlate results back
            // to the original input patterns without per-ref resolution.
            for dataset_ref_pattern in &dataset_ref_patterns {
                let dataset_ref = dataset_ref_pattern.as_dataset_ref().unwrap();
                match dataset_registry
                    .resolve_dataset_handle_by_ref(dataset_ref)
                    .await
                {
                    Ok(dataset_handle) => yield dataset_handle,
                    Err(error @ odf::DatasetRefUnresolvedError::NotFound(_)) => {
                        on_unmatched_pattern(dataset_ref_pattern, Some(error))?;
                    }
                    Err(error @ odf::DatasetRefUnresolvedError::Internal(_)) => {
                        Err::<(), _>(error)?;
                    }
                }
            }
        })
    } else {
        Box::pin(async_stream::try_stream! {
            let mut on_unmatched_pattern = on_unmatched_pattern;
            let mut matched_patterns = vec![false; dataset_ref_patterns.len()];
            let mut all_dataset_handles_stream = dataset_registry.all_dataset_handles();

            while let Some(dataset_handle_result) = all_dataset_handles_stream.next().await {
                let dataset_handle = dataset_handle_result
                    .map_err(odf::DatasetRefUnresolvedError::Internal)?;

                let mut has_match = false;

                for (idx, pattern) in dataset_ref_patterns.iter().enumerate() {
                    if pattern.matches(&dataset_handle) {
                        matched_patterns[idx] = true;
                        has_match = true;
                    }
                }

                if has_match {
                    yield dataset_handle;
                }
            }

            for (idx, pattern) in dataset_ref_patterns.iter().enumerate() {
                if !matched_patterns[idx] {
                    let maybe_error = if pattern.is_pattern() {
                        None
                    } else {
                        Some(odf::DatasetRefUnresolvedError::NotFound(
                            odf::DatasetNotFoundError {
                                dataset_ref: pattern.as_dataset_ref().unwrap().clone(),
                            },
                        ))
                    };

                    on_unmatched_pattern(pattern, maybe_error)?;
                }
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn filter_datasets_by_any_pattern<'a>(
    dataset_registry: &'a dyn DatasetRegistry,
    search_svc: Arc<dyn SearchServiceRemote>,
    dataset_ref_any_patterns: Vec<odf::DatasetRefAnyPattern>,
    current_account_name: &odf::AccountName,
    tenancy_config: TenancyConfig,
) -> FilteredDatasetRefAnyStream<'a> {
    let (all_ref_patterns, static_refs): (Vec<_>, Vec<_>) = dataset_ref_any_patterns
        .into_iter()
        .partition(odf::DatasetRefAnyPattern::is_pattern);

    let (remote_ref_patterns, local_ref_patterns): (Vec<_>, Vec<_>) =
        all_ref_patterns.into_iter().partition(|pattern| {
            pattern.is_remote_pattern(tenancy_config == TenancyConfig::MultiTenant)
        });

    let static_datasets_stream = get_static_datasets_stream(static_refs);
    let remote_patterns_stream = get_remote_datasets_stream(
        search_svc,
        remote_ref_patterns,
        tenancy_config == TenancyConfig::MultiTenant,
    );
    let local_patterns_stream =
        get_local_datasets_stream(dataset_registry, local_ref_patterns, current_account_name);

    static_datasets_stream
        .chain(remote_patterns_stream)
        .chain(local_patterns_stream)
        .boxed()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_static_datasets_stream(
    static_refs: Vec<odf::DatasetRefAnyPattern>,
) -> impl Stream<Item = Result<odf::DatasetRefAny, odf::DatasetRefUnresolvedError>> + 'static {
    async_stream::try_stream! {
        for static_ref in static_refs {
            yield static_ref
                .into_dataset_ref_any()
                .unwrap();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_remote_datasets_stream(
    search_svc: Arc<dyn SearchServiceRemote>,
    remote_ref_patterns: Vec<odf::DatasetRefAnyPattern>,
    is_multitenant_mode: bool,
) -> impl Stream<Item = Result<odf::DatasetRefAny, odf::DatasetRefUnresolvedError>> + 'static {
    async_stream::try_stream! {
        for remote_ref_pattern in &remote_ref_patterns {
            // TODO: potentially low performance solution,as it will always fully scan a remote repo.
            // Should be improved after search will support wildcarding.
            let repo_name = remote_ref_pattern.pattern_repo_name(is_multitenant_mode).expect("Invalid repository name");

            let search_options = SearchRemoteOpts {
                repository_names: vec![repo_name],
            };

            let remote_datasets: Vec<_> = match search_svc.search(None, search_options).await {
                Err(err) => match err {
                    SearchRemoteError::RepositoryNotFound(_) => vec![],
                    _ => Err(odf::DatasetRefUnresolvedError::Internal(InternalError::new(err)))?,
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
    remote_ref_pattern: &odf::DatasetRefAnyPattern,
    dataset_alias_remote: &odf::DatasetAliasRemote,
) -> bool {
    match remote_ref_pattern {
        odf::DatasetRefAnyPattern::Ref(_) | odf::DatasetRefAnyPattern::PatternLocal(_) => {
            unreachable!()
        }
        odf::DatasetRefAnyPattern::PatternAmbiguous(repo_name, dataset_name_pattern) => {
            let repo_name = odf::RepoName::new_unchecked(&repo_name.pattern);
            repo_name == dataset_alias_remote.repo_name
                && dataset_name_pattern.matches(&dataset_alias_remote.dataset_name)
        }
        odf::DatasetRefAnyPattern::PatternRemote(repo_name, account_name, dataset_name_pattern) => {
            repo_name == &dataset_alias_remote.repo_name
                && (dataset_alias_remote.account_name.is_some()
                    && account_name == dataset_alias_remote.account_name.as_ref().unwrap())
                && dataset_name_pattern.matches(&dataset_alias_remote.dataset_name)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_local_datasets_stream<'a>(
    dataset_registry: &'a dyn DatasetRegistry,
    dataset_ref_patterns: Vec<odf::DatasetRefAnyPattern>,
    current_account_name: &odf::AccountName,
) -> impl Stream<Item = Result<odf::DatasetRefAny, odf::DatasetRefUnresolvedError>> + 'a {
    dataset_registry
        .all_dataset_handles_by_owner_name(current_account_name)
        .try_filter(move |dataset_handle| {
            future::ready(dataset_ref_patterns.iter().any(|dataset_ref_pattern| {
                matches_local_ref_pattern(dataset_ref_pattern, dataset_handle)
            }))
        })
        .map_ok(|dataset_handle| dataset_handle.as_any_ref())
        .map_err(Into::into)
}

pub fn matches_local_ref_pattern(
    local_ref_pattern: &odf::DatasetRefAnyPattern,
    dataset_handle: &odf::DatasetHandle,
) -> bool {
    match local_ref_pattern {
        odf::DatasetRefAnyPattern::Ref(_) | odf::DatasetRefAnyPattern::PatternRemote(_, _, _) => {
            unreachable!()
        }
        odf::DatasetRefAnyPattern::PatternLocal(dataset_name_pattern) => {
            dataset_name_pattern.matches(&dataset_handle.alias.dataset_name)
        }
        odf::DatasetRefAnyPattern::PatternAmbiguous(account_name, dataset_name_pattern) => {
            let account_name = odf::AccountName::new_unchecked(&account_name.pattern);
            (dataset_handle.alias.account_name.is_some()
                && &account_name == dataset_handle.alias.account_name.as_ref().unwrap())
                && dataset_name_pattern.matches(&dataset_handle.alias.dataset_name)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A utility that allows you to filter a ` DatasetHandleStream ` without
/// changing its type (like `StreamExt::filter_ok()` does).
pub fn filter_dataset_handle_stream<'a, F>(
    mut stream: odf::dataset::DatasetHandleStream<'a>,
    predicate: F,
) -> odf::dataset::DatasetHandleStream<'a>
where
    F: Fn(&odf::DatasetHandle) -> bool,
    F: Send + 'a,
{
    Box::pin(async_stream::stream! {
        while let Some(item) = stream.next().await {
            if let Ok(dataset_handle) = &item {
                if predicate(dataset_handle) {
                    yield item;
                }
            } else {
                // In case of an iteration error, it is not our responsibility to handle the error here.
                yield item;
            }
        }
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
