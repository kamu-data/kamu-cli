// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::DatasetHandleStream;
use opendatafabric::DatasetRefPattern;

pub fn filter_dataset_stream(
    dhs: DatasetHandleStream,
    dataset_ref_pattern: DatasetRefPattern,
) -> DatasetHandleStream<'_> {
    use futures::{future, StreamExt, TryStreamExt};
    dhs.try_filter(move |dsh| {
        future::ready(
            dataset_ref_pattern
                .is_match(&dsh.as_local_ref())
                .unwrap_or(false),
        )
    })
    .boxed()
}
