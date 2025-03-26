// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface, meta};
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::DatasetRegistry;
use kamu_datasets::DatasetDependencyRepository;

use crate::{
    extract_modified_dependencies_in_interval,
    DependencyChange,
    DependencyGraphServiceImpl,
    JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    JOB_KAMU_DATASETS_DEPENDENCY_GRAPH_INDEXER,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DependencyGraphIndexer {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dependency_graph_service: Arc<DependencyGraphServiceImpl>,
    dataset_dependency_repo: Arc<dyn DatasetDependencyRepository>,
}

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_DATASETS_DEPENDENCY_GRAPH_INDEXER,
    depends_on: &[
        JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    ],
    requires_transaction: true,
})]
impl DependencyGraphIndexer {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dependency_graph_service: Arc<DependencyGraphServiceImpl>,
        dataset_dependency_repo: Arc<dyn DatasetDependencyRepository>,
    ) -> Self {
        Self {
            dataset_registry,
            dependency_graph_service,
            dataset_dependency_repo,
        }
    }

    async fn was_indexed(&self) -> Result<bool, InternalError> {
        self.dataset_dependency_repo
            .stores_any_dependencies()
            .await
            .int_err()
    }

    async fn index_dependencies_from_storage(&self) -> Result<(), InternalError> {
        use tokio_stream::StreamExt;
        use tracing::Instrument;

        let mut datasets_stream = self.dataset_registry.all_dataset_handles();

        while let Some(Ok(dataset_handle)) = datasets_stream.next().await {
            let span =
                tracing::debug_span!("Scanning dataset dependencies", dataset = %dataset_handle);

            let dataset = self
                .dataset_registry
                .get_dataset_by_handle(&dataset_handle)
                .instrument(span)
                .await;

            // Important: read from storage, not from database cache!
            let head = dataset
                .as_metadata_chain()
                .as_uncached_ref_repo()
                .get(odf::BlockRef::Head.as_str())
                .await
                .int_err()?;

            // Compute if there are some dependencies
            if let DependencyChange::Changed(upstream_ids) =
                extract_modified_dependencies_in_interval(dataset.as_metadata_chain(), &head, None)
                    .await?
            {
                self.dataset_dependency_repo
                    .add_upstream_dependencies(
                        &dataset_handle.id,
                        &(upstream_ids.iter().collect::<Vec<_>>()),
                    )
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for DependencyGraphIndexer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DependencyGraphIndexer::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        if self.was_indexed().await? {
            tracing::debug!("Skip initialization: dependency graph was already indexed");
        } else {
            self.index_dependencies_from_storage().await?;
        }

        self.dependency_graph_service
            .load_dependency_graph(
                self.dataset_registry.as_ref(),
                self.dataset_dependency_repo.as_ref(),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
