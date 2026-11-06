// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use internal_error::*;
use kamu_core::*;
use kamu_datasets::ResolvedDataset;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CompactionPlanner)]
pub struct CompactionPlannerImpl {}

#[common_macros::method_names_consts]
impl CompactionPlannerImpl {
    #[tracing::instrument(
        name = CompactionPlannerImpl_plan_dataset_compaction,
        level = "debug",
        skip_all,
        fields(
            target=%target.get_handle(),
            max_slice_size,
            max_slice_records,
            keep_metadata_only
        )
    )]
    async fn plan_dataset_compaction(
        &self,
        target: ResolvedDataset,
        max_slice_size: u64,
        max_slice_records: u64,
        keep_metadata_only: bool,
        listener: Arc<dyn CompactionListener>,
    ) -> Result<CompactionPlan, CompactionPlanningError> {
        listener.begin_phase(CompactionPhase::GatherChainInfo);

        let mut old_num_blocks: usize = 0;
        let mut maybe_seed: Option<odf::Multihash> = None;
        let mut vocab_event: Option<odf::metadata::SetVocab> = None;
        let mut data_slice_batches: Vec<CompactionDataSliceBatch> = vec![];
        let mut batch = BatchBuilder::default();

        let chain = target.as_metadata_chain();
        let head = chain.resolve_ref(&odf::BlockRef::Head).await?;

        let object_data_repo = target.as_data_repo();

        {
            use futures::TryStreamExt;
            let mut block_stream = chain.iter_blocks_interval((&head).into(), None, false);
            while let Some((block_hash, block)) = block_stream.try_next().await? {
                old_num_blocks += 1;
                match block.event {
                    // Compacting to metadata only drops every data block
                    odf::MetadataEvent::AddData(_) | odf::MetadataEvent::ExecuteTransform(_)
                        if keep_metadata_only => {}
                    odf::MetadataEvent::AddData(add_data_event) => {
                        let maybe_data_slice_url = match &add_data_event.new_data {
                            Some(output_slice) => {
                                if batch.overflows_with(
                                    output_slice,
                                    max_slice_size,
                                    max_slice_records,
                                ) {
                                    batch.close(&mut data_slice_batches);
                                }
                                Some(
                                    object_data_repo
                                        .get_internal_url(&output_slice.physical_hash)
                                        .await,
                                )
                            }
                            None => None,
                        };
                        batch.add(block_hash, &add_data_event, maybe_data_slice_url)?;
                    }
                    odf::MetadataEvent::Seed(_) => maybe_seed = Some(block_hash),
                    event @ (odf::MetadataEvent::ExecuteTransform(_)
                    | odf::MetadataEvent::SetPollingSource(_)
                    | odf::MetadataEvent::SetTransform(_)
                    | odf::MetadataEvent::SetVocab(_)
                    | odf::MetadataEvent::SetAttachments(_)
                    | odf::MetadataEvent::SetInfo(_)
                    | odf::MetadataEvent::SetLicense(_)
                    | odf::MetadataEvent::SetDataSchema(_)
                    | odf::MetadataEvent::AddPushSource(_)
                    | odf::MetadataEvent::DisablePushSource(_)
                    | odf::MetadataEvent::DisablePollingSource(_)) => {
                        if let odf::MetadataEvent::SetVocab(set_vocab_event) = event {
                            vocab_event = Some(set_vocab_event);
                        }
                        batch.close(&mut data_slice_batches);
                        data_slice_batches.push(CompactionDataSliceBatch::SingleBlock(block_hash));
                    }
                }
            }
            batch.close(&mut data_slice_batches);
        }

        let vocab: odf::metadata::DatasetVocabulary = vocab_event.unwrap_or_default().into();

        Ok(CompactionPlan {
            data_slice_batches,
            old_head: head,
            offset_column_name: vocab.offset_column().into(),
            seed: maybe_seed.expect("Seed must be present"),
            old_num_blocks,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl CompactionPlanner for CompactionPlannerImpl {
    #[tracing::instrument(name = CompactionPlannerImpl_plan_compaction, level = "debug", skip_all, fields(target=%target.get_handle(), ?options))]
    async fn plan_compaction(
        &self,
        target: ResolvedDataset,
        options: CompactionOptions,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<CompactionPlan, CompactionPlanningError> {
        if !options.keep_metadata_only && target.get_kind() != odf::DatasetKind::Root {
            return Err(CompactionPlanningError::InvalidDatasetKind(
                InvalidDatasetKindError {
                    dataset_alias: target.get_alias().clone(),
                },
            ));
        }

        let listener = maybe_listener.unwrap_or(Arc::new(NullCompactionListener {}));

        let max_slice_size = options.max_slice_size.unwrap_or(DEFAULT_MAX_SLICE_SIZE);
        let max_slice_records = options
            .max_slice_records
            .unwrap_or(DEFAULT_MAX_SLICE_RECORDS);

        let plan = self
            .plan_dataset_compaction(
                target,
                max_slice_size,
                max_slice_records,
                options.keep_metadata_only,
                listener.clone(),
            )
            .await?;
        listener.end_phase(CompactionPhase::GatherChainInfo);
        listener.plan_success(&plan);
        Ok(plan)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The `AddData` blocks gathered for one batch, visited newest first
#[derive(Default)]
struct BatchBuilder {
    info: CompactionDataSliceBatchInfo,
    /// Every block of the batch, with or without a data slice, newest first
    block_hashes: Vec<odf::Multihash>,
    size: u64,
    records: u64,
}

impl BatchBuilder {
    /// Whether adding the slice would take a batch that already has data past
    /// a limit
    fn overflows_with(&self, slice: &odf::DataSlice, max_size: u64, max_records: u64) -> bool {
        !self.info.data_slices_batch.is_empty()
            && (self.size + slice.size > max_size
                || self.records + slice.num_records() > max_records)
    }

    fn add(
        &mut self,
        block_hash: odf::Multihash,
        add_data: &odf::metadata::AddData,
        maybe_data_slice_url: Option<Url>,
    ) -> Result<(), InternalError> {
        // Each block carries the checkpoint, watermark and source state forward, so
        // the newest one holds what the whole batch leaves behind, an absent value
        // included
        if self.block_hashes.is_empty() {
            self.info.upper_bound = CompactionDataSliceBatchUpperBound {
                new_source_state: add_data.new_source_state.clone(),
                new_watermark: add_data.new_watermark,
                new_checkpoint: add_data.new_checkpoint.clone(),
                end_offset: 0,
            };
        }
        self.block_hashes.push(block_hash);
        self.info
            .lower_bound
            .prev_checkpoint
            .clone_from(&add_data.prev_checkpoint);
        self.info.lower_bound.prev_offset = add_data.prev_offset;

        if let (Some(slice), Some(data_slice_url)) = (&add_data.new_data, maybe_data_slice_url) {
            if self.info.data_slices_batch.is_empty() {
                self.info.upper_bound.end_offset = slice.offset_interval.end;
            }
            self.info.data_slices_batch.push(data_slice_url);
            self.info.lower_bound.start_offset = slice.offset_interval.start;
            self.size += slice.size;
            self.records += slice.num_records();
        }

        if let Some(extra) = &add_data.extra
            && let Some(attr) = extra
                .get::<odf::schema::ext::AttrLinkedObjects>()
                .int_err()?
        {
            let linked_objects = self.info.linked_objects.get_or_insert_default();
            linked_objects.num_objects_naive += attr.linked_objects.num_objects_naive;
            linked_objects.size_naive += attr.linked_objects.size_naive;
        }
        Ok(())
    }

    /// Adds the batch to the plan and starts a new one. Only two or more data
    /// slices are merged; any other batch keeps its blocks as they are
    fn close(&mut self, data_slice_batches: &mut Vec<CompactionDataSliceBatch>) {
        let batch = std::mem::take(self);
        if batch.info.data_slices_batch.len() > 1 {
            data_slice_batches.push(CompactionDataSliceBatch::CompactedBatch(batch.info));
        } else {
            data_slice_batches.extend(
                batch
                    .block_hashes
                    .into_iter()
                    .map(CompactionDataSliceBatch::SingleBlock),
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
