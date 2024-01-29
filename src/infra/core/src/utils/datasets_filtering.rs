// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use kamu_core::{DatasetHandleStream, DatasetRepository};
use opendatafabric::DatasetRefPattern;

pub fn filter_datasets_by_pattern(
    dataset_repo: &dyn DatasetRepository,
    dataset_ref_patterns: Vec<DatasetRefPattern>,
) -> DatasetHandleStream<'_> {
    use futures::{future, StreamExt, TryStreamExt};
    let glob_dataset_ref_patterns: Vec<_> = dataset_ref_patterns
        .iter()
        .filter(|dataset_ref_pattern| match dataset_ref_pattern {
            DatasetRefPattern::Ref(_) => false,
            DatasetRefPattern::Pattern(_, _) => true,
        })
        .collect();

    if glob_dataset_ref_patterns.is_empty() {
        Box::pin(async_stream::try_stream! {
            for dataset_ref_pattern in dataset_ref_patterns.iter() {
                // Check references exist
                // TODO: PERF: Create a batch version of `resolve_dataset_ref`
                match dataset_repo.resolve_dataset_ref(dataset_ref_pattern.dataset_ref().unwrap()).await {
                    Ok(dataset_handle) => yield dataset_handle,
                    Err(_) => continue,
                };
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
            .boxed()
    }
}
