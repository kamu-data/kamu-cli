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

use dill::*;
use engine::TransformRequestExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use random_strings::get_random_name;

use super::build_preliminary_request_ext;
use crate::{
    GetTransformQueryInputError,
    get_transform_input_from_query_input,
    get_transform_query_input,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TransformRequestPlanner)]
pub struct TransformRequestPlannerImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
}

impl TransformRequestPlannerImpl {
    // TODO: PERF: Avoid multiple passes over metadata chain
    #[tracing::instrument(level = "info", skip_all)]
    async fn get_next_operation(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformPreliminaryPlan, TransformPlanError> {
        // Build preliminary request
        let preliminary_request = build_preliminary_request_ext(target.clone()).await?;

        // Pre-fill datasets that are used in the operation
        let mut datasets_map = ResolvedDatasetsMap::default();
        datasets_map.register(target);
        for (input_decl, _) in &preliminary_request.input_states {
            let hdl = self
                .dataset_registry
                .resolve_dataset_handle_by_ref(&input_decl.dataset_ref)
                .await
                .int_err()?;
            let resolved_dataset = self.dataset_registry.get_dataset_by_handle(&hdl).await;
            datasets_map.register(resolved_dataset);
        }

        Ok(TransformPreliminaryPlan {
            preliminary_request,
            datasets_map,
        })
    }

    // TODO: Avoid iterating through output chain multiple times
    async fn get_vocab(
        &self,
        dataset: &dyn odf::Dataset,
    ) -> Result<odf::metadata::DatasetVocabulary, InternalError> {
        use odf::dataset::MetadataChainExt;
        Ok(dataset
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetVocabVisitor::new())
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
    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle()))]
    async fn build_transform_preliminary_plan(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformPreliminaryPlan, TransformPlanError> {
        // TODO: There might be more operations to do
        self.get_next_operation(target.clone()).await
    }

    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle(), ?block_range))]
    async fn build_transform_verification_plan(
        &self,
        target: ResolvedDataset,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
    ) -> Result<VerifyTransformOperation, VerifyTransformPlanError> {
        let metadata_chain = target.as_metadata_chain();

        let head = match block_range.1 {
            None => metadata_chain.resolve_ref(&odf::BlockRef::Head).await?,
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
            use odf::dataset::*;

            let mut set_transform_visitor = SearchSetTransformVisitor::new(target.get_kind());
            let mut set_vocab_visitor = SearchSetVocabVisitor::new();
            let mut set_data_schema_visitor = SearchSetDataSchemaVisitor::new();

            type Flag = odf::metadata::MetadataEventTypeFlags;
            type Decision = MetadataVisitorDecision;

            struct ExecuteTransformCollectorVisitor {
                tail_sequence_number: Option<u64>,
                blocks: Vec<(odf::Multihash, odf::MetadataBlock)>,
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
                    }

                    let block_flag = Flag::from(&block.event);

                    if Flag::EXECUTE_TRANSFORM.contains(block_flag) {
                        state.blocks.push((hash.clone(), block.clone()));
                    }

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
                set_transform_visitor.into_inner().into_event(),
                set_vocab_visitor.into_event(),
                set_data_schema_visitor
                    .into_event()
                    .as_ref()
                    .map(|e| e.schema_as_arrow(&odf::metadata::ToArrowSettings::default()))
                    .transpose() // Option<Result<SchemaRef, E>> -> Result<Option<SchemaRef>, E>
                    .int_err()?
                    .map(Arc::new),
                blocks,
                finished_range,
            )
        };

        // Ensure start_block was found if specified
        if let Some(tail) = tail
            && !finished_range
        {
            return Err(odf::dataset::InvalidIntervalError { head, tail }.into());
        }

        let source = source.ok_or(
            "Expected a derivative dataset but SetTransform block was not found".int_err(),
        )?;

        // Fill table of working datasets
        let mut datasets_map = ResolvedDatasetsMap::default();
        datasets_map.register(target.clone());
        for input in &source.inputs {
            let hdl = self
                .dataset_registry
                .resolve_dataset_handle_by_ref(&input.dataset_ref)
                .await
                .int_err()?;
            let resolved_input = self.dataset_registry.get_dataset_by_handle(&hdl).await;
            datasets_map.register(resolved_input);
        }

        // TODO: Replace maps with access by index, as ODF guarantees same order of
        // inputs in ExecuteTransform as in SetTransform
        use futures::{StreamExt, TryStreamExt};
        let dataset_vocabs: BTreeMap<_, _> = futures::stream::iter(&source.inputs)
            .map(|input| input.dataset_ref.id().cloned().unwrap())
            .then(|input_id| async {
                use futures::TryFutureExt;
                let resolved_input = datasets_map.get_by_id(&input_id);
                self.get_vocab(resolved_input.as_ref())
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
            use odf::metadata::AsTypedBlock;
            let block_t = block.as_typed::<odf::metadata::ExecuteTransform>().unwrap();

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
                    dataset_handle: target.get_handle().clone(),
                    block_ref: odf::BlockRef::Head,
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

    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle()))]
    async fn evaluate_transform_status(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformStatus, TransformStatusError> {
        // Build a plan
        let plan = self.get_next_operation(target.clone()).await?;

        // Resolve query inputs
        use futures::{StreamExt, TryStreamExt};
        let query_inputs: Vec<_> = futures::stream::iter(plan.preliminary_request.input_states)
            .then(|(input_decl, input_state)| {
                get_transform_query_input(input_decl, input_state, &plan.datasets_map)
            })
            .try_collect()
            .await
            .map_err(|e| match e {
                GetTransformQueryInputError::Internal(e) => TransformStatusError::Internal(e),
            })?;

        // Filter out query inputs that have no changes
        let filtered_query_inputs: Vec<_> = query_inputs
            .into_iter()
            .filter(|input| {
                input
                    .new_offset
                    .is_some_and(|offset| offset != input.prev_offset.unwrap_or_default())
            })
            .collect();

        // If there is at least one query input with changes, transform is out of date
        if filtered_query_inputs.is_empty() {
            // No new data, so the transform is up-to-date
            Ok(TransformStatus::UpToDate)
        } else {
            // New data is available, so we need to run the transform
            Ok(TransformStatus::NewInputDataAvailable {
                input_advancements: filtered_query_inputs,
            })
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
