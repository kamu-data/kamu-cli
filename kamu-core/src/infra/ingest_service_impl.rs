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
    metadata_repo: Arc<dyn MetadataRepository>,
    engine_provisioner: Arc<dyn EngineProvisioner>,
}

#[component(pub)]
impl IngestServiceImpl {
    pub fn new(
        volume_layout: &VolumeLayout,
        metadata_repo: Arc<dyn MetadataRepository>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
    ) -> Self {
        Self {
            volume_layout: volume_layout.clone(),
            metadata_repo,
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
        if let None = combined_result {
            return new_result;
        }

        if let IngestResult::UpToDate { .. } = new_result {
            return combined_result.unwrap();
        }

        if let Some(IngestResult::Updated {
            old_head: prev_old_head,
            new_head: _,
            num_blocks: prev_num_blocks,
            has_more: _,
            uncacheable: _,
        }) = combined_result
        {
            if let IngestResult::Updated {
                old_head: _,
                new_head: new_new_head,
                num_blocks: new_num_blocks,
                has_more: new_has_more,
                uncacheable: new_uncacheable,
            } = new_result
            {
                return IngestResult::Updated {
                    old_head: prev_old_head,
                    new_head: new_new_head,
                    num_blocks: prev_num_blocks + new_num_blocks,
                    has_more: new_has_more,
                    uncacheable: new_uncacheable,
                };
            }
        }

        unreachable!()
    }

    // TODO: Improve error handling
    fn spawn_ingest_task(
        &self,
        dataset_ref: &DatasetRefLocal,
        options: IngestOptions,
        multi_listener: &Arc<dyn IngestMultiListener>,
    ) -> std::thread::JoinHandle<Result<IngestResult, IngestError>> {
        let dataset_handle = self
            .metadata_repo
            .resolve_dataset_ref(&dataset_ref)
            .unwrap();

        let layout = self.get_dataset_layout(&dataset_handle);

        let meta_chain = self
            .metadata_repo
            .get_metadata_chain(&dataset_handle.as_local_ref())
            .unwrap();

        let engine_provisioner = self.engine_provisioner.clone();

        let null_listener = Arc::new(NullIngestListener {});
        let listener = multi_listener
            .begin_ingest(&dataset_handle)
            .unwrap_or(null_listener);

        let thread_handle = std::thread::Builder::new()
            .name("ingest_multi".to_owned())
            .spawn(move || {
                let exhaust_sources = options.exhaust_sources;

                let mut ingest_task = IngestTask::new(
                    dataset_handle.clone(),
                    options,
                    layout,
                    meta_chain,
                    None,
                    listener,
                    engine_provisioner,
                );

                let mut combined_result = None;
                loop {
                    match ingest_task.ingest() {
                        Ok(res) => {
                            combined_result = Some(Self::merge_results(combined_result, res));

                            if let Some(IngestResult::Updated { has_more, .. }) = combined_result {
                                if has_more && exhaust_sources {
                                    continue;
                                }
                            }
                        }
                        Err(e) => return Err(e),
                    }
                    break;
                }
                Ok(combined_result.unwrap())
            })
            .unwrap();

        thread_handle
    }
}

impl IngestService for IngestServiceImpl {
    fn ingest(
        &self,
        dataset_ref: &DatasetRefLocal,
        options: IngestOptions,
        maybe_listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        let null_listener = Arc::new(NullIngestListener {});
        let listener = maybe_listener.unwrap_or(null_listener);

        info!(%dataset_ref, "Ingesting single dataset");

        let dataset_handle = self.metadata_repo.resolve_dataset_ref(dataset_ref)?;
        let meta_chain = self
            .metadata_repo
            .get_metadata_chain(&dataset_handle.as_local_ref())
            .unwrap();

        let layout = self.get_dataset_layout(&dataset_handle);

        let mut ingest_task = IngestTask::new(
            dataset_handle,
            options,
            layout,
            meta_chain,
            None,
            listener,
            self.engine_provisioner.clone(),
        );

        ingest_task.ingest()
    }

    fn ingest_from(
        &self,
        dataset_ref: &DatasetRefLocal,
        fetch: FetchStep,
        options: IngestOptions,
        maybe_listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        let null_listener = Arc::new(NullIngestListener {});
        let listener = maybe_listener.unwrap_or(null_listener);

        info!(%dataset_ref, ?fetch, "Ingesting single dataset from overriden source");

        let dataset_handle = self.metadata_repo.resolve_dataset_ref(dataset_ref)?;
        let meta_chain = self
            .metadata_repo
            .get_metadata_chain(&dataset_handle.as_local_ref())
            .unwrap();

        let layout = self.get_dataset_layout(&dataset_handle);

        let mut ingest_task = IngestTask::new(
            dataset_handle,
            options,
            layout,
            meta_chain,
            Some(fetch),
            listener,
            self.engine_provisioner.clone(),
        );

        ingest_task.ingest()
    }

    fn ingest_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefLocal>,
        options: IngestOptions,
        maybe_multi_listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetRefLocal, Result<IngestResult, IngestError>)> {
        let null_multi_listener: Arc<dyn IngestMultiListener> =
            Arc::new(NullIngestMultiListener {});
        let multi_listener = maybe_multi_listener.unwrap_or(null_multi_listener);

        let dataset_refs: Vec<_> = dataset_refs.collect();
        info!(datasets = ?dataset_refs, "Ingesting multiple datasets");

        let thread_handles: Vec<_> = dataset_refs
            .into_iter()
            .map(|dataset_ref| {
                (
                    dataset_ref.clone(),
                    self.spawn_ingest_task(&dataset_ref, options.clone(), &multi_listener),
                )
            })
            .collect();

        let results: Vec<_> = thread_handles
            .into_iter()
            .map(|(r, h)| (r, h.join().unwrap()))
            .collect();

        results
    }
}
