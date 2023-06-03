// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::Stream;
use kamu_domain::{DatasetRepository, GetSummaryOpts, InternalError, ResultIntoInternal};
use opendatafabric::{DatasetHandle, DatasetRef};
use tokio_stream::StreamExt;

/////////////////////////////////////////////////////////////////////////////////////////

pub fn get_staging_name() -> String {
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    let mut name = String::with_capacity(16);
    name.push_str(".pending-");
    name.extend(
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from),
    );

    name
}

/////////////////////////////////////////////////////////////////////////////////////////

pub fn get_downstream_dependencies_impl<'s>(
    repo: &'s dyn DatasetRepository,
    dataset_ref: &'s DatasetRef,
) -> impl Stream<Item = Result<DatasetHandle, InternalError>> + 's {
    async_stream::try_stream! {
        let dataset_handle = repo.resolve_dataset_ref(dataset_ref).await.int_err()?;

        let mut dataset_handles = repo.get_all_datasets();
        while let Some(hdl) = dataset_handles.try_next().await? {
            if hdl.id == dataset_handle.id {
                continue;
            }

            let summary = repo
                .get_dataset(&hdl.as_local_ref())
                .await
                .int_err()?
                .get_summary(GetSummaryOpts::default())
                .await
                .int_err()?;

            if summary
                .dependencies
                .iter()
                .any(|d| d.id.as_ref() == Some(&dataset_handle.id))
            {
                yield hdl;
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
