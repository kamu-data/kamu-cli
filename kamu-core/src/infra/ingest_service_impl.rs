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
    volume_layout: VolumeLayout,
    dataset_reg: Arc<dyn DatasetRegistry>,
    engine_provisioner: Arc<dyn EngineProvisioner>,
}

#[component(pub)]
impl IngestServiceImpl {
    pub fn new(
        volume_layout: &VolumeLayout,
        dataset_reg: Arc<dyn DatasetRegistry>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
    ) -> Self {
        Self {
            volume_layout: volume_layout.clone(),
            dataset_reg,
            engine_provisioner,
        }
    }

    // TODO: error handling
    fn get_dataset_layout(&self, dataset_handle: &DatasetHandle) -> DatasetLayout {
        DatasetLayout::create(&self.volume_layout, &dataset_handle.name).unwrap()
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
        let dataset_handle = self.dataset_reg.resolve_dataset_ref(&dataset_ref)?;

        let layout = self.get_dataset_layout(&dataset_handle);

        let meta_chain = self
            .dataset_reg
            .get_metadata_chain(&dataset_handle.as_local_ref())?;

        let engine_provisioner = self.engine_provisioner.clone();

        let listener =
            get_listener(&dataset_handle).unwrap_or_else(|| Arc::new(NullIngestListener));

        let ingest_task = IngestTask::new(
            dataset_handle.clone(),
            options.clone(),
            layout,
            meta_chain,
            fetch_override,
            listener,
            engine_provisioner,
        );

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
        let multi_listener =
            maybe_multi_listener.unwrap_or_else(|| Arc::new(NullIngestMultiListener));

        let dataset_refs: Vec<_> = dataset_refs.collect();
        info!(datasets = ?dataset_refs, "Ingesting multiple datasets");

        let futures: Vec<_> = dataset_refs
            .iter()
            .map(|dataset_ref| {
                self.do_ingest(dataset_ref, options.clone(), None, |hdl| {
                    multi_listener.begin_ingest(hdl)
                })
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        dataset_refs.into_iter().zip(results).collect()
    }
}
