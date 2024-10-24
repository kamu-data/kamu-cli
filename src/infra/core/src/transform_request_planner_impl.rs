// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::*;
use engine::{TransformRequestExt, TransformRequestInputExt};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use opendatafabric::{
    DatasetVocabulary,
    ExecuteTransform,
    ExecuteTransformInput,
    IntoDataStreamBlock,
    SetDataSchema,
    TransformInput,
    Watermark,
};
use random_names::get_random_name;
use time_source::SystemTimeSource;

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

    async fn plan_transform(
        &self,
        target: ResolvedDataset,
        options: &TransformOptions,
    ) -> Result<TransformPlanItem, TransformPlanError> {
        // TODO: There might be more operations to do
        match self
            .get_next_operation(target.clone(), self.time_source.now())
            .await
        {
            Ok(Some(operation)) => Ok(TransformPlanItem::ReadyToLaunch(operation)),
            Ok(None) => Ok(TransformPlanItem::UpToDate),
            // TODO: Trapping the error to preserve old behavior - we should consider
            // surfacing it and handling on upper layers
            Err(TransformPlanError::InputSchemaNotDefined(e)) => {
                tracing::info!(
                    input = %e.dataset_handle,
                    "Not processing because one of the inputs was never pulled",
                );
                Ok(TransformPlanItem::UpToDate)
            }
            Err(TransformPlanError::InvalidInputInterval(e))
                if options.reset_derivatives_on_diverged_input =>
            {
                Ok(TransformPlanItem::RetryAfterCompacting(e))
            }
            Err(e) => Err(e),
        }
    }

    // TODO: PERF: Avoid multiple passes over metadata chain
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn get_next_operation(
        &self,
        target: ResolvedDataset,
        system_time: DateTime<Utc>,
    ) -> Result<Option<TransformRequestExt>, TransformPlanError> {
        let output_chain = target.dataset.as_metadata_chain();

        // TODO: externalize
        let block_ref = BlockRef::Head;
        let head = output_chain.resolve_ref(&block_ref).await.int_err()?;

        // TODO: PERF: Search for source, vocab, and data schema result in full scan
        let (source, schema, set_vocab, prev_query) = {
            // TODO: Support transform evolution
            let mut set_transform_visitor = SearchSetTransformVisitor::new();
            let mut set_vocab_visitor = SearchSetVocabVisitor::new();
            let mut set_data_schema_visitor = SearchSetDataSchemaVisitor::new();
            let mut execute_transform_visitor = SearchExecuteTransformVisitor::new();

            target
                .dataset
                .as_metadata_chain()
                .accept_by_hash(
                    &mut [
                        &mut set_transform_visitor,
                        &mut set_vocab_visitor,
                        &mut set_data_schema_visitor,
                        &mut execute_transform_visitor,
                    ],
                    &head,
                )
                .await
                .int_err()?;

            (
                set_transform_visitor.into_event(),
                set_data_schema_visitor
                    .into_event()
                    .as_ref()
                    .map(SetDataSchema::schema_as_arrow)
                    .transpose() // Option<Result<SchemaRef, E>> -> Result<Option<SchemaRef>, E>
                    .int_err()?,
                set_vocab_visitor.into_event(),
                execute_transform_visitor.into_event(),
            )
        };

        let Some(source) = source else {
            return Err(TransformNotDefinedError {}.into());
        };
        tracing::debug!(?source, "Transforming using source");

        // Prepare inputs
        use itertools::Itertools;
        let input_states: Vec<(&TransformInput, Option<&ExecuteTransformInput>)> =
            if let Some(query) = &prev_query {
                source
                    .inputs
                    .iter()
                    .zip_eq(query.query_inputs.iter().map(Some))
                    .collect()
            } else {
                source.inputs.iter().map(|i| (i, None)).collect()
            };

        use futures::{StreamExt, TryStreamExt};
        let inputs: Vec<_> = futures::stream::iter(input_states)
            .then(|(input_decl, input_state)| self.get_transform_input(input_decl, input_state))
            .try_collect()
            .await?;

        // Nothing to do?
        // Note that we're considering a schema here, as even if there is no data to
        // process we would like to run the transform to establish the schema of the
        // output.
        //
        // TODO: Detect the situation where inputs only had source updates and skip
        // running the engine
        if inputs
            .iter()
            .all(|i| i.data_slices.is_empty() && i.explicit_watermarks.is_empty())
            && schema.is_some()
        {
            return Ok(None);
        }

        Ok(Some(TransformRequestExt {
            operation_id: get_random_name(None, 10),
            dataset_handle: target.handle.clone(),
            block_ref,
            head,
            transform: source.transform,
            system_time,
            schema,
            prev_offset: prev_query.as_ref().and_then(ExecuteTransform::last_offset),
            vocab: set_vocab.unwrap_or_default().into(),
            inputs,
            prev_checkpoint: prev_query.and_then(|q| q.new_checkpoint.map(|c| c.physical_hash)),
        }))
    }

    async fn get_transform_input(
        &self,
        input_decl: &TransformInput,
        input_state: Option<&ExecuteTransformInput>,
    ) -> Result<TransformRequestInputExt, TransformPlanError> {
        let dataset_id = input_decl.dataset_ref.id().unwrap();
        if let Some(input_state) = input_state {
            assert_eq!(*dataset_id, input_state.dataset_id);
        }

        let hdl = self
            .dataset_registry
            .resolve_dataset_handle_by_ref(&dataset_id.as_local_ref())
            .await
            .int_err()?;
        let dataset = self.dataset_registry.get_dataset_by_handle(&hdl);
        let input_chain = dataset.as_metadata_chain();

        // Determine last processed input block and offset
        let last_processed_block = input_state.and_then(|i| i.last_block_hash());
        let last_processed_offset = input_state.and_then(ExecuteTransformInput::last_offset);

        // Determine unprocessed block and offset range
        let last_unprocessed_block = input_chain.resolve_ref(&BlockRef::Head).await.int_err()?;
        let last_unprocessed_offset = input_chain
            .accept_one_by_hash(
                &last_unprocessed_block,
                SearchSingleDataBlockVisitor::next(),
            )
            .await
            .int_err()?
            .into_event()
            .and_then(|event| event.last_offset())
            .or(last_processed_offset);

        let query_input = ExecuteTransformInput {
            dataset_id: dataset_id.clone(),
            prev_block_hash: last_processed_block.cloned(),
            new_block_hash: if Some(&last_unprocessed_block) != last_processed_block {
                Some(last_unprocessed_block)
            } else {
                None
            },
            prev_offset: last_processed_offset,
            new_offset: if last_unprocessed_offset != last_processed_offset {
                last_unprocessed_offset
            } else {
                None
            },
        };

        self.get_transform_input_from_query_input(
            query_input,
            input_decl.alias.clone().unwrap(),
            None,
        )
        .await
    }

    async fn get_transform_input_from_query_input(
        &self,
        query_input: ExecuteTransformInput,
        alias: String,
        vocab_hint: Option<DatasetVocabulary>,
    ) -> Result<TransformRequestInputExt, TransformPlanError> {
        let hdl = self
            .dataset_registry
            .resolve_dataset_handle_by_ref(&query_input.dataset_id.as_local_ref())
            .await
            .int_err()?;

        let dataset = self.dataset_registry.get_dataset_by_handle(&hdl);
        let input_chain = dataset.as_metadata_chain();

        // Find schema
        // TODO: Make single-pass via multi-visitor
        let schema = dataset
            .as_metadata_chain()
            .accept_one(SearchSetDataSchemaVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map(|e| e.schema_as_arrow())
            .transpose()
            .int_err()?
            .ok_or_else(|| InputSchemaNotDefinedError {
                dataset_handle: hdl.clone(),
            })?;

        // Collect unprocessed input blocks
        use futures::TryStreamExt;
        let blocks_unprocessed = if let Some(new_block_hash) = &query_input.new_block_hash {
            input_chain
                .iter_blocks_interval(new_block_hash, query_input.prev_block_hash.as_ref(), false)
                .try_collect()
                .await
                .map_err(|chain_err| match chain_err {
                    IterBlocksError::InvalidInterval(err) => {
                        TransformPlanError::InvalidInputInterval(InvalidInputIntervalError {
                            head: err.head,
                            tail: err.tail,
                            input_dataset_id: hdl.id.clone(),
                        })
                    }
                    _ => TransformPlanError::Internal(chain_err.int_err()),
                })?
        } else {
            Vec::new()
        };

        let mut data_slices = Vec::new();
        let mut explicit_watermarks = Vec::new();
        for block in blocks_unprocessed
            .iter()
            .rev()
            .filter_map(|(_, b)| b.as_data_stream_block())
        {
            if let Some(slice) = block.event.new_data {
                data_slices.push(slice.physical_hash.clone());
            }

            if let Some(wm) = block.event.new_watermark {
                explicit_watermarks.push(Watermark {
                    system_time: *block.system_time,
                    event_time: *wm,
                });
            }
        }

        let vocab = match vocab_hint {
            Some(v) => v,
            None => self.get_vocab(dataset.as_ref()).await?,
        };

        let is_empty = data_slices.is_empty() && explicit_watermarks.is_empty();

        let input = TransformRequestInputExt {
            dataset_handle: hdl,
            alias,
            vocab,
            prev_block_hash: query_input.prev_block_hash,
            new_block_hash: query_input.new_block_hash,
            prev_offset: query_input.prev_offset,
            new_offset: query_input.new_offset,
            data_slices,
            schema,
            explicit_watermarks,
        };

        tracing::info!(?input, is_empty, "Computed transform input");

        Ok(input)
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
    async fn collect_transform_plan(
        &self,
        targets: Vec<ResolvedDataset>,
        options: TransformOptions,
    ) -> (Vec<TransformPlanItem>, Vec<TransformPlanError>) {
        let mut planned_items = Vec::new();
        let mut errors = Vec::new();

        for target in targets {
            match self.plan_transform(target, &options).await {
                Ok(plan_item) => planned_items.push(plan_item),
                Err(e) => errors.push(e),
            }
        }

        (planned_items, errors)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
