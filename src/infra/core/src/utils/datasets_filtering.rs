// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;

use futures::{future, StreamExt, TryStreamExt};
use kamu_core::{DatasetRepository, GetDatasetError};
use opendatafabric::{DatasetHandle, DatasetRefAny, DatasetRefPatternAny, DatasetRefPatternLocal};
use tokio_stream::{self as stream, Stream, StreamMap};

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
    dataset_ref_patterns: Vec<DatasetRefPatternAny>,
) -> StreamMap<&'_ str, FilteredDatasetRefAnyStream<'_>> {
    let mut is_pattern = false;
    let mut static_dataset_refs = vec![];
    for dataset_ref_pattern in &dataset_ref_patterns {
        if dataset_ref_pattern.is_pattern() {
            is_pattern = true;
        } else {
            static_dataset_refs.push(Ok(dataset_ref_pattern
                .as_dataset_ref_any()
                .unwrap()
                .clone()));
        }
    }

    let mut result = StreamMap::new();

    result.insert(
        "static_dataset_refs",
        Box::pin(stream::iter(static_dataset_refs)) as FilteredDatasetRefAnyStream,
    );

    if is_pattern {
        result.insert(
            "filtered_dataset_refs",
            dataset_repo
                .get_all_datasets()
                .try_filter(move |dataset_handle| {
                    future::ready(
                        dataset_ref_patterns.iter().any(|dataset_ref_pattern| {
                            dataset_ref_pattern.is_match(dataset_handle)
                        }),
                    )
                })
                .map_ok(|dataset_handle| dataset_handle.as_any_ref())
                .map_err(Into::into)
                .boxed(),
        );
    }

    result
}
