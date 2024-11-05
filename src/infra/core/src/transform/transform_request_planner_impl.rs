// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::*;
use engine::TransformRequestExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use opendatafabric::{
    AsTypedBlock,
    DatasetVocabulary,
    ExecuteTransform,
    MetadataBlock,
    MetadataBlockTyped,
    MetadataEventTypeFlags,
    Multihash,
    SetDataSchema,
    SetTransform,
};
use random_names::get_random_name;
use time_source::SystemTimeSource;

use super::build_preliminary_request_ext;
use crate::get_transform_input_from_query_input;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformRequestPlannerImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    time_source: Arc<dyn SystemTimeSource>,
}

#[component(pub)]
#[interface(dyn TransformRequestPlanner)]
impl TransformRequestPlannerImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            dataset_registry,
            time_source,
        }
    }

    // TODO: PERF: Avoid multiple passes over metadata chain
    #[tracing::instrument(level = "info", skip_all)]
    async fn get_next_operation(
        &self,
        target: ResolvedDataset,
        system_time: DateTime<Utc>,
    ) -> Result<TransformPreliminaryPlan, TransformPlanError> {
        // Build prelmiinary request
        let preliminary_request =
            build_preliminary_request_ext(target.clone(), system_time).await?;

        // Pre-fill datasets that is used in the operation
        let mut datasets_map = WorkingDatasetsMap::default();
        datasets_map.register(&target.handle, target.dataset);
        for (input_decl, _) in &preliminary_request.input_states {
            let hdl = self
                .dataset_registry
                .resolve_dataset_handle_by_ref(&input_decl.dataset_ref)
                .await
                .int_err()?;
            datasets_map
                .register_with(&hdl, |hdl| self.dataset_registry.get_dataset_by_handle(hdl));
        }

        Ok(TransformPreliminaryPlan {
            preliminary_request,
            datasets_map,
        })
    }

    // TODO: Avoid iterating through output chain multiple times
    async fn get_vocab(&self, dataset: &dyn Dataset) -> Result<DatasetVocabulary, InternalError> {
        Ok(dataset
            .as_metadata_chain()
            .accept_one(SearchSetVocabVisitor::new())
            .await
            .int_err()?
            .into_event()
            .unwrap_or_default()
            .into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TransformRequestPlanner for TransformRequestPlannerImpl {
    /// Returns an active transform, if any
    async fn get_active_transform(
        &self,
        target: ResolvedDataset,
    ) -> Result<Option<(Multihash, MetadataBlockTyped<SetTransform>)>, InternalError> {
        // TODO: Support transform evolution
        Ok(target
            .dataset
            .as_metadata_chain()
            .accept_one(SearchSetTransformVisitor::new())
            .await
            .int_err()?
            .into_hashed_block())
    }

    async fn build_transform_preliminary_plan(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformPreliminaryPlan, TransformPlanError> {
        // TODO: There might be more operations to do
        self.get_next_operation(target.clone(), self.time_source.now())
            .await
    }

    async fn build_transform_verification_plan(
        &self,
        target: ResolvedDataset,
        block_range: (Option<Multihash>, Option<Multihash>),
    ) -> Result<VerifyTransformOperation, VerifyTransformPlanError> {
        let metadata_chain = target.dataset.as_metadata_chain();

        let head = match block_range.1 {
            None => metadata_chain.resolve_ref(&BlockRef::Head).await?,
            Some(hash) => hash,
        };
        let tail = block_range.0;
        let tail_sequence_number = match tail.as_ref() {
            Some(tail) => {
                let block = metadata_chain.get_block(tail).await?;

                Some(block.sequence_number)
            }
            None => None,
        };

        let (source, set_vocab, schema, blocks, finished_range) = {
            // TODO: Support dataset evolution
            let mut set_transform_visitor = SearchSetTransformVisitor::new();
            let mut set_vocab_visitor = SearchSetVocabVisitor::new();
            let mut set_data_schema_visitor = SearchSetDataSchemaVisitor::new();

            type Flag = MetadataEventTypeFlags;
            type Decision = MetadataVisitorDecision;

            struct ExecuteTransformCollectorVisitor {
                tail_sequence_number: Option<u64>,
                blocks: Vec<(Multihash, MetadataBlock)>,
                finished_range: bool,
            }

            let mut execute_transform_collector_visitor = GenericCallbackVisitor::new(
                ExecuteTransformCollectorVisitor {
                    tail_sequence_number,
                    blocks: Vec::new(),
                    finished_range: false,
                },
                Decision::NextOfType(Flag::EXECUTE_TRANSFORM),
                |state, hash, block| {
                    if Some(block.sequence_number) < state.tail_sequence_number {
                        state.finished_range = true;

                        return Decision::Stop;
                    };

                    let block_flag = Flag::from(&block.event);

                    if Flag::EXECUTE_TRANSFORM.contains(block_flag) {
                        state.blocks.push((hash.clone(), block.clone()));
                    };

                    if Some(block.sequence_number) == state.tail_sequence_number {
                        state.finished_range = true;

                        Decision::Stop
                    } else {
                        Decision::NextOfType(Flag::EXECUTE_TRANSFORM)
                    }
                },
            );

            metadata_chain
                .accept(&mut [
                    &mut set_transform_visitor,
                    &mut set_vocab_visitor,
                    &mut set_data_schema_visitor,
                    &mut execute_transform_collector_visitor,
                ])
                .await
                .int_err()?;

            let ExecuteTransformCollectorVisitor {
                blocks,
                finished_range,
                ..
            } = execute_transform_collector_visitor.into_state();

            (
                set_transform_visitor.into_event(),
                set_vocab_visitor.into_event(),
                set_data_schema_visitor
                    .into_event()
                    .as_ref()
                    .map(SetDataSchema::schema_as_arrow)
                    .transpose() // Option<Result<SchemaRef, E>> -> Result<Option<SchemaRef>, E>
                    .int_err()?,
                blocks,
                finished_range,
            )
        };

        // Ensure start_block was found if specified
        if tail.is_some() && !finished_range {
            return Err(InvalidIntervalError {
                head,
                tail: tail.unwrap(),
            }
            .into());
        }

        let source = source.ok_or(
            "Expected a derivative dataset but SetTransform block was not found".int_err(),
        )?;

        // Fill table of working datasets
        let mut datasets_map = WorkingDatasetsMap::default();
        datasets_map.register(&target.handle, target.dataset.clone());
        for input in &source.inputs {
            let hdl = self
                .dataset_registry
                .resolve_dataset_handle_by_ref(&input.dataset_ref)
                .await
                .int_err()?;
            let dataset = self.dataset_registry.get_dataset_by_handle(&hdl);
            datasets_map.register(&hdl, dataset);
        }

        // TODO: Replace maps with access by index, as ODF guarantees same order of
        // inputs in ExecuteTransform as in SetTransform
        use futures::{StreamExt, TryStreamExt};
        let dataset_vocabs: BTreeMap<_, _> = futures::stream::iter(&source.inputs)
            .map(|input| input.dataset_ref.id().cloned().unwrap())
            .then(|input_id| async {
                use futures::TryFutureExt;
                let input_dataset = datasets_map.get_by_id(&input_id);
                self.get_vocab(input_dataset.as_ref())
                    .map_ok(|vocab| (input_id, vocab))
                    .await
            })
            .try_collect()
            .await?;

        let input_aliases: BTreeMap<_, _> = source
            .inputs
            .iter()
            .map(|i| {
                (
                    i.dataset_ref.id().cloned().unwrap(),
                    i.alias.clone().unwrap(),
                )
            })
            .collect();

        let mut steps = Vec::new();

        for (block_hash, block) in blocks.into_iter().rev() {
            let block_t = block.as_typed::<ExecuteTransform>().unwrap();

            let inputs = futures::stream::iter(&block_t.event.query_inputs)
                .then(|slice| {
                    let alias = input_aliases.get(&slice.dataset_id).unwrap();

                    let vocab = dataset_vocabs.get(&slice.dataset_id).cloned().unwrap();

                    get_transform_input_from_query_input(
                        slice.clone(),
                        alias.clone(),
                        Some(vocab),
                        &datasets_map,
                    )
                })
                .try_collect()
                .await
                .map_err(Into::<VerifyTransformPlanError>::into)?;

            let step = VerifyTransformStep {
                request: TransformRequestExt {
                    operation_id: get_random_name(None, 10),
                    dataset_handle: target.handle.clone(),
                    block_ref: BlockRef::Head,
                    head: block_t.prev_block_hash.unwrap().clone(),
                    transform: source.transform.clone(),
                    system_time: block.system_time,
                    schema: schema.clone(),
                    prev_offset: block_t.event.prev_offset,
                    inputs,
                    vocab: set_vocab.clone().unwrap_or_default().into(),
                    prev_checkpoint: block_t.event.prev_checkpoint.clone(),
                },
                expected_block: block,
                expected_hash: block_hash,
            };

            steps.push(step);
        }

        Ok(VerifyTransformOperation {
            steps,
            datasets_map,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
