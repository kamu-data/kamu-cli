// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::ingest::*;
use crate::domain::*;
use crate::infra::*;
use dill::*;
use opendatafabric::*;
use tracing::info;

use std::sync::Arc;

pub struct IngestServiceImpl {
    workspace_layout: Arc<WorkspaceLayout>,
    local_repo: Arc<dyn LocalDatasetRepository>,
    engine_provisioner: Arc<dyn EngineProvisioner>,
}

#[component(pub)]
impl IngestServiceImpl {
    pub fn new(
        workspace_layout: Arc<WorkspaceLayout>,
        local_repo: Arc<dyn LocalDatasetRepository>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
    ) -> Self {
        Self {
            workspace_layout,
            local_repo,
            engine_provisioner,
        }
    }

    // TODO: Introduce intermediate structs to avoid full unpacking
    fn merge_results(
        combined_result: Option<IngestResult>,
        new_result: IngestResult,
    ) -> IngestResult {
        match (combined_result, new_result) {
            (None, n) => n,
            (Some(IngestResult::UpToDate { .. }), n) => n,
            (
                Some(IngestResult::Updated {
                    old_head,
                    new_head,
                    num_blocks,
                    ..
                }),
                IngestResult::UpToDate {
                    has_more,
                    uncacheable,
                },
            ) => IngestResult::Updated {
                old_head,
                new_head,
                num_blocks,
                has_more,
                uncacheable,
            },
            (
                Some(IngestResult::Updated {
                    old_head: prev_old_head,
                    num_blocks: prev_num_blocks,
                    ..
                }),
                IngestResult::Updated {
                    new_head,
                    num_blocks,
                    has_more,
                    uncacheable,
                    ..
                },
            ) => IngestResult::Updated {
                old_head: prev_old_head,
                new_head,
                num_blocks: num_blocks + prev_num_blocks,
                has_more,
                uncacheable,
            },
        }
    }

    async fn do_ingest(
        &self,
        dataset_ref: &DatasetRefLocal,
        options: IngestOptions,
        fetch_override: Option<FetchStep>,
        get_listener: impl FnOnce(&DatasetHandle) -> Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        let dataset_handle = self.local_repo.resolve_dataset_ref(&dataset_ref).await?;

        // TODO: This service should not know the dataset layout specifics
        // Consider getting layout from LocalDatasetRepository
        let layout = self.workspace_layout.dataset_layout(&dataset_handle.name);

        let dataset = self
            .local_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let engine_provisioner = self.engine_provisioner.clone();

        let listener =
            get_listener(&dataset_handle).unwrap_or_else(|| Arc::new(NullIngestListener));

        let ingest_task = IngestTask::new(
            dataset_handle.clone(),
            dataset,
            options.clone(),
            layout,
            fetch_override,
            listener,
            engine_provisioner,
        )
        .await?;

        Self::poll_until_exhausted(ingest_task, options).await
    }

    async fn poll_until_exhausted(
        mut task: IngestTask,
        options: IngestOptions,
    ) -> Result<IngestResult, IngestError> {
        let mut combined_result = None;

        loop {
            match task.ingest().await {
                Ok(res) => {
                    combined_result = Some(Self::merge_results(combined_result, res));

                    let has_more = match combined_result {
                        Some(IngestResult::UpToDate { has_more, .. }) => has_more,
                        Some(IngestResult::Updated { has_more, .. }) => has_more,
                        None => unreachable!(),
                    };

                    if !has_more || !options.exhaust_sources {
                        break;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(combined_result.unwrap())
    }
}

#[async_trait::async_trait(?Send)]
impl IngestService for IngestServiceImpl {
    async fn ingest(
        &self,
        dataset_ref: &DatasetRefLocal,
        options: IngestOptions,
        maybe_listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        info!(%dataset_ref, "Ingesting single dataset");
        self.do_ingest(dataset_ref, options, None, |_| maybe_listener)
            .await
    }

    async fn ingest_from(
        &self,
        dataset_ref: &DatasetRefLocal,
        fetch: FetchStep,
        options: IngestOptions,
        maybe_listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        info!(%dataset_ref, ?fetch, "Ingesting single dataset from overriden source");
        self.do_ingest(dataset_ref, options, Some(fetch), |_| maybe_listener)
            .await
    }

    async fn ingest_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefLocal>,
        options: IngestOptions,
        maybe_multi_listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetRefLocal, Result<IngestResult, IngestError>)> {
        self.ingest_multi_ext(
            &mut dataset_refs.map(|r| IngestRequest {
                dataset_ref: r,
                fetch_override: None,
            }),
            options,
            maybe_multi_listener,
        )
        .await
    }

    async fn ingest_multi_ext(
        &self,
        requests: &mut dyn Iterator<Item = IngestRequest>,
        options: IngestOptions,
        maybe_multi_listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetRefLocal, Result<IngestResult, IngestError>)> {
        let multi_listener =
            maybe_multi_listener.unwrap_or_else(|| Arc::new(NullIngestMultiListener));

        let requests: Vec<_> = requests.collect();
        info!(?requests, "Ingesting multiple datasets");

        let futures: Vec<_> = requests
            .iter()
            .map(|req| {
                self.do_ingest(
                    &req.dataset_ref,
                    options.clone(),
                    req.fetch_override.clone(),
                    |hdl| multi_listener.begin_ingest(hdl),
                )
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        requests
            .into_iter()
            .map(|r| r.dataset_ref)
            .zip(results)
            .collect()
    }
}
