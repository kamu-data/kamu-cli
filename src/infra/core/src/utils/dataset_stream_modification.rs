// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::DatasetHandleStream;
use opendatafabric::{DatasetNamePattern, DatasetRefPattern};

pub fn filter_dataset_stream(
    dhs: DatasetHandleStream,
    dataset_name_pattern: DatasetNamePattern,
) -> DatasetHandleStream<'_> {
    use futures::{future, StreamExt, TryStreamExt};
    dhs.try_filter(move |dsh| {
        future::ready(
            DatasetRefPattern::match_pattern(
                dsh.to_string().as_str(),
                dataset_name_pattern.as_str(),
            )
            .unwrap_or(false),
        )
    })
    .boxed()
}
