// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_core::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DependencyGraphRepositoryInMemory {
    dataset_repo: Arc<dyn DatasetRepository>,
}

#[dill::component(pub)]
#[dill::interface(dyn DependencyGraphRepository)]
impl DependencyGraphRepositoryInMemory {
    pub fn new(dataset_repo: Arc<dyn DatasetRepository>) -> Self {
        Self { dataset_repo }
    }
}

impl DependencyGraphRepository for DependencyGraphRepositoryInMemory {
    #[tracing::instrument(level = "debug", skip_all)]
    fn list_dependencies_of_all_datasets(&self) -> DatasetDependenciesIDStream {
        use tokio_stream::StreamExt;

        Box::pin(async_stream::try_stream! {
            let mut datasets_stream = self.dataset_repo.get_all_datasets();
            while let Some(Ok(dataset_handle)) = datasets_stream.next().await {
                let dataset_span = tracing::debug_span!("Scanning dataset dependencies", dataset=%dataset_handle);
                let _ = dataset_span.enter();

                let summary = self
                    .dataset_repo
                    .get_dataset(&dataset_handle.as_local_ref())
                    .await
                    .int_err()?
                    .get_summary(GetSummaryOpts::default())
                    .await
                    .int_err()?;

                for input_id in summary.dependencies {
                    yield (dataset_handle.id.clone(), input_id);
                }
            }
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
