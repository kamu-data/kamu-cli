// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::sync::Arc;

use dill::{component, interface};
use internal_error::*;
use kamu_core::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CompactionPlanner)]
pub struct CompactionPlannerImpl {}

impl CompactionPlannerImpl {
    #[tracing::instrument(
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

        // Declare mut values for result

        let mut old_num_blocks: usize = 0;
        let mut maybe_seed: Option<odf::Multihash> = None;

        let mut current_hash: Option<odf::Multihash> = None;
        let mut vocab_event: Option<odf::metadata::SetVocab> = None;
        let mut data_slice_batch_info = CompactionDataSliceBatchInfo::default();
        let mut data_slice_batches: Vec<CompactionDataSliceBatch> = vec![];
        let (mut batch_size, mut batch_records) = (0u64, 0u64);

        ////////////////////////////////////////////////////////////////////////////////

        let chain = target.as_metadata_chain();
        let head = chain.resolve_ref(&odf::BlockRef::Head).await?;

        let object_data_repo = target.as_data_repo();

        {
            use futures::TryStreamExt;
            use odf::dataset::MetadataChainExt;
            let mut block_stream = chain.iter_blocks_interval(&head, None, false);
            while let Some((block_hash, block)) = block_stream.try_next().await? {
                old_num_blocks += 1;
                match block.event {
                    odf::MetadataEvent::AddData(add_data_event) => {
                        if !keep_metadata_only && let Some(output_slice) = &add_data_event.new_data
                        {
                            let data_slice_url = object_data_repo
                                .get_internal_url(&output_slice.physical_hash)
                                .await;

                            // Setting the end offset interval needs to be here because we
                            // have to get it at the beginning of iteration unlike
                            // other values which will be set at the end of iteration
                            if data_slice_batch_info.data_slices_batch.is_empty() {
                                data_slice_batch_info.upper_bound.end_offset =
                                    output_slice.offset_interval.end;
                            }

                            let current_records = output_slice.num_records();

                            if batch_size + output_slice.size > max_slice_size
                                || batch_records + current_records > max_slice_records
                            {
                                let is_appended = self.append_add_data_batch_to_chain_info(
                                    &mut data_slice_batches,
                                    current_hash.as_ref(),
                                    &mut data_slice_batch_info,
                                );
                                if is_appended {
                                    // Reset values for next batch
                                    data_slice_batch_info = CompactionDataSliceBatchInfo::default();
                                    data_slice_batch_info.upper_bound.end_offset =
                                        output_slice.offset_interval.end;
                                }

                                data_slice_batch_info.data_slices_batch = vec![data_slice_url];
                                batch_size = output_slice.size;
                                batch_records = current_records;
                            } else {
                                data_slice_batch_info.data_slices_batch.push(data_slice_url);
                                batch_size += output_slice.size;
                                batch_records += current_records;
                            }

                            // Set lower bound values
                            data_slice_batch_info.lower_bound.prev_checkpoint =
                                add_data_event.prev_checkpoint;
                            data_slice_batch_info.lower_bound.prev_offset =
                                add_data_event.prev_offset;
                            data_slice_batch_info.lower_bound.start_offset =
                                output_slice.offset_interval.start;
                            current_hash = Some(block_hash);
                        }
                        // Set upper bound values
                        if data_slice_batch_info.upper_bound.new_checkpoint.is_none() {
                            data_slice_batch_info.upper_bound.new_checkpoint =
                                add_data_event.new_checkpoint;
                        }
                        if data_slice_batch_info.upper_bound.new_source_state.is_none() {
                            data_slice_batch_info.upper_bound.new_source_state =
                                add_data_event.new_source_state;
                        }
                        if data_slice_batch_info.upper_bound.new_watermark.is_none() {
                            data_slice_batch_info.upper_bound.new_watermark =
                                add_data_event.new_watermark;
                        }
                        let extra = add_data_event.extra.unwrap_or_default();
                        if let Some(attr) = extra
                            .get::<odf::schema::ext::AttrLinkedObjects>()
                            .int_err()?
                        {
                            let mut linked_objects =
                                data_slice_batch_info.linked_objects.unwrap_or_default();
                            linked_objects.num_objects_naive +=
                                attr.linked_objects.num_objects_naive;
                            linked_objects.size_naive += attr.linked_objects.size_naive;
                            data_slice_batch_info.linked_objects = Some(linked_objects);
                        }
                    }
                    odf::MetadataEvent::Seed(_) => maybe_seed = Some(block_hash),
                    odf::MetadataEvent::ExecuteTransform(_) if keep_metadata_only => {}
                    event => {
                        if let odf::MetadataEvent::SetVocab(set_vocab_event) = event {
                            vocab_event = Some(set_vocab_event);
                        }
                        let is_appended = self.append_add_data_batch_to_chain_info(
                            &mut data_slice_batches,
                            current_hash.as_ref(),
                            &mut data_slice_batch_info,
                        );
                        data_slice_batches
                            .push(CompactionDataSliceBatch::SingleBlock(block_hash.clone()));
                        if is_appended {
                            data_slice_batch_info = CompactionDataSliceBatchInfo::default();
                        }
                    }
                }
            }
        }

        let vocab: odf::metadata::DatasetVocabulary = vocab_event.unwrap_or_default().into();

        Ok(CompactionPlan {
            data_slice_batches,
            old_head: head,
            offset_column_name: vocab.offset_column,
            seed: maybe_seed.expect("Seed must be present"),
            old_num_blocks,
        })
    }

    fn append_add_data_batch_to_chain_info(
        &self,
        data_slice_batches: &mut Vec<CompactionDataSliceBatch>,
        hash: Option<&odf::Multihash>,
        data_slice_batch_info: &mut CompactionDataSliceBatchInfo,
    ) -> bool {
        match data_slice_batch_info.data_slices_batch.len().cmp(&1) {
            Ordering::Equal => {
                data_slice_batches
                    .push(CompactionDataSliceBatch::SingleBlock(hash.unwrap().clone()));
                true
            }
            Ordering::Greater => {
                data_slice_batches.push(CompactionDataSliceBatch::CompactedBatch(
                    data_slice_batch_info.clone(),
                ));
                true
            }
            _ => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl CompactionPlanner for CompactionPlannerImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(target=%target.get_handle(), ?options))]
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

        match self
            .plan_dataset_compaction(
                target,
                max_slice_size,
                max_slice_records,
                options.keep_metadata_only,
                listener.clone(),
            )
            .await
        {
            Ok(plan) => {
                listener.plan_success(&plan);
                Ok(plan)
            }
            Err(err) => {
                listener.plan_error(&err);
                Err(err)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
