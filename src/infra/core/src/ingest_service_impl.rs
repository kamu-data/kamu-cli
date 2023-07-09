// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use container_runtime::ContainerRuntime;
use dill::*;
use kamu_core::*;
use opendatafabric::*;

use super::ingest::*;

pub struct IngestServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    engine_provisioner: Arc<dyn EngineProvisioner>,
    container_runtime: Arc<ContainerRuntime>,
    run_info_dir: PathBuf,
    cache_dir: PathBuf,
}

#[component(pub)]
impl IngestServiceImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
        container_runtime: Arc<ContainerRuntime>,
        run_info_dir: PathBuf,
        cache_dir: PathBuf,
    ) -> Self {
        Self {
            dataset_repo,
            engine_provisioner,
            container_runtime,
            run_info_dir,
            cache_dir,
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
                    no_polling_source: _,
                    uncacheable,
                },
            ) => IngestResult::Updated {
                old_head,
                new_head,
                num_blocks,
                has_more: false,
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
        dataset_ref: &DatasetRef,
        options: IngestOptions,
        fetch_override: Option<FetchStep>,
        get_listener: impl FnOnce(&DatasetHandle) -> Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        let dataset_handle = self.dataset_repo.resolve_dataset_ref(&dataset_ref).await?;

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let listener =
            get_listener(&dataset_handle).unwrap_or_else(|| Arc::new(NullIngestListener));

        let request = self
            .prepare_ingest_request(dataset_handle, dataset.clone())
            .await?;

        // TODO: create via DI to avoid passing through all dependencies
        let ingest_task = IngestTask::new(
            dataset,
            request,
            options.clone(),
            fetch_override,
            listener,
            self.engine_provisioner.clone(),
            self.container_runtime.clone(),
            &self.run_info_dir,
            &self.cache_dir,
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
                        Some(IngestResult::UpToDate { .. }) => false,
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

    async fn prepare_ingest_request(
        &self,
        dataset_handle: DatasetHandle,
        dataset: Arc<dyn Dataset>,
    ) -> Result<IngestRequest, InternalError> {
        // TODO: PERF: This is expensive and could be cached
        let mut polling_source = None;
        let mut prev_source_state = None;
        let mut prev_checkpoint = None;
        let mut prev_watermark = None;
        let mut vocab = None;
        let mut next_offset = None;

        {
            use futures::stream::TryStreamExt;
            let mut block_stream = dataset.as_metadata_chain().iter_blocks();
            while let Some((_, block)) = block_stream.try_next().await.int_err()? {
                match block.event {
                    MetadataEvent::AddData(add_data) => {
                        if next_offset.is_none() {
                            if let Some(output_data) = &add_data.output_data {
                                next_offset = Some(output_data.interval.end + 1);
                            }
                        }
                        if prev_checkpoint.is_none() {
                            prev_checkpoint =
                                Some(add_data.output_checkpoint.map(|cp| cp.physical_hash));
                        }
                        if prev_watermark.is_none() {
                            prev_watermark = Some(add_data.output_watermark);
                        }
                        if prev_source_state.is_none() {
                            // TODO: Should we check that this is polling source?
                            prev_source_state = Some(add_data.source_state);
                        }
                    }
                    MetadataEvent::SetWatermark(set_wm) => {
                        if prev_watermark.is_none() {
                            prev_watermark = Some(Some(set_wm.output_watermark));
                        }
                    }
                    MetadataEvent::SetPollingSource(src) => {
                        if polling_source.is_none() {
                            polling_source = Some(src);
                        }
                    }
                    MetadataEvent::SetVocab(set_vocab) => {
                        vocab = Some(set_vocab.into());
                    }
                    MetadataEvent::Seed(_) => {
                        if next_offset.is_none() {
                            next_offset = Some(0);
                        }
                    }
                    MetadataEvent::ExecuteQuery(_) => unreachable!(),
                    MetadataEvent::SetAttachments(_)
                    | MetadataEvent::SetInfo(_)
                    | MetadataEvent::SetLicense(_)
                    | MetadataEvent::SetTransform(_) => (),
                }

                if next_offset.is_some()
                    && polling_source.is_some()
                    && vocab.is_some()
                    && prev_checkpoint.is_some()
                    && prev_watermark.is_some()
                {
                    break;
                }
            }
        }

        Ok(IngestRequest {
            dataset_handle,
            polling_source,
            system_time: Utc::now(),
            event_time: None,
            next_offset: next_offset.unwrap_or_default(),
            vocab: vocab.unwrap_or_default(),
            prev_checkpoint: prev_checkpoint.unwrap_or_default(),
            prev_watermark: prev_watermark.unwrap_or_default(),
            prev_source_state: prev_source_state.clone().unwrap_or_default(),
        })
    }
}

#[async_trait::async_trait]
impl IngestService for IngestServiceImpl {
    async fn ingest(
        &self,
        dataset_ref: &DatasetRef,
        options: IngestOptions,
        maybe_listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        tracing::info!(%dataset_ref, "Ingesting single dataset");
        self.do_ingest(dataset_ref, options, None, |_| maybe_listener)
            .await
    }

    async fn ingest_from(
        &self,
        dataset_ref: &DatasetRef,
        fetch: FetchStep,
        options: IngestOptions,
        maybe_listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        tracing::info!(%dataset_ref, ?fetch, "Ingesting single dataset from overriden source");
        self.do_ingest(dataset_ref, options, Some(fetch), |_| maybe_listener)
            .await
    }

    async fn ingest_multi(
        &self,
        dataset_refs: Vec<DatasetRef>,
        options: IngestOptions,
        maybe_multi_listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetRef, Result<IngestResult, IngestError>)> {
        self.ingest_multi_ext(
            dataset_refs
                .into_iter()
                .map(|r| IngestParams {
                    dataset_ref: r,
                    fetch_override: None,
                })
                .collect(),
            options,
            maybe_multi_listener,
        )
        .await
    }

    async fn ingest_multi_ext(
        &self,
        requests: Vec<IngestParams>,
        options: IngestOptions,
        maybe_multi_listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetRef, Result<IngestResult, IngestError>)> {
        let multi_listener =
            maybe_multi_listener.unwrap_or_else(|| Arc::new(NullIngestMultiListener));

        tracing::info!(?requests, "Ingesting multiple datasets");

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
