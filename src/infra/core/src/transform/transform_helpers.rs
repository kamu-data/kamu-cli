// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::engine::TransformRequestInputExt;
use kamu_core::{
    BlockRef,
    Dataset,
    InputSchemaNotDefinedError,
    InvalidInputIntervalError,
    IterBlocksError,
    MetadataChainExt,
    ResolvedDataset,
    ResolvedDatasetsMap,
    SearchExecuteTransformVisitor,
    SearchSetDataSchemaVisitor,
    SearchSetTransformVisitor,
    SearchSetVocabVisitor,
    TransformElaborateError,
    TransformNotDefinedError,
    TransformPlanError,
    TransformPreliminaryRequestExt,
    VerifyTransformPlanError,
};
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
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "info", skip_all)]
pub async fn build_preliminary_request_ext(
    target: ResolvedDataset,
    system_time: DateTime<Utc>,
) -> Result<TransformPreliminaryRequestExt, BuildPreliminaryTransformRequestError> {
    let output_chain = target.as_metadata_chain();

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
    let input_states: Vec<(TransformInput, Option<ExecuteTransformInput>)> =
        if let Some(query) = &prev_query {
            source
                .inputs
                .iter()
                .cloned()
                .zip_eq(query.query_inputs.iter().cloned().map(Some))
                .collect()
        } else {
            source.inputs.iter().map(|i| (i.clone(), None)).collect()
        };

    // Build preliminary transform request
    Ok(TransformPreliminaryRequestExt {
        operation_id: get_random_name(None, 10),
        dataset_handle: target.get_handle().clone(),
        block_ref,
        head,
        transform: source.transform,
        system_time,
        schema,
        prev_offset: prev_query.as_ref().and_then(ExecuteTransform::last_offset),
        vocab: set_vocab.unwrap_or_default().into(),
        input_states,
        prev_checkpoint: prev_query.and_then(|q| q.new_checkpoint.map(|c| c.physical_hash)),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_transform_input_from_query_input(
    query_input: ExecuteTransformInput,
    alias: String,
    vocab_hint: Option<DatasetVocabulary>,
    datasets_map: &ResolvedDatasetsMap,
) -> Result<TransformRequestInputExt, GetTransformInputError> {
    let resolved_input = datasets_map.get_by_id(&query_input.dataset_id);
    let input_chain = resolved_input.as_metadata_chain();

    // Find schema
    // TODO: Make single-pass via multi-visitor
    let schema = resolved_input
        .as_metadata_chain()
        .accept_one(SearchSetDataSchemaVisitor::new())
        .await
        .int_err()?
        .into_event()
        .map(|e| e.schema_as_arrow())
        .transpose()
        .int_err()?
        .ok_or_else(|| InputSchemaNotDefinedError {
            dataset_handle: resolved_input.get_handle().clone(),
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
                    GetTransformInputError::InvalidInputInterval(InvalidInputIntervalError {
                        head: err.head,
                        tail: err.tail,
                        input_dataset_id: query_input.dataset_id,
                    })
                }
                _ => GetTransformInputError::Internal(chain_err.int_err()),
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
        None => get_vocab(resolved_input.as_ref()).await?,
    };

    let is_empty = data_slices.is_empty() && explicit_watermarks.is_empty();

    let input = TransformRequestInputExt {
        dataset_handle: resolved_input.get_handle().clone(),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Avoid iterating through output chain multiple times
async fn get_vocab(dataset: &dyn Dataset) -> Result<DatasetVocabulary, InternalError> {
    Ok(dataset
        .as_metadata_chain()
        .accept_one(SearchSetVocabVisitor::new())
        .await
        .int_err()?
        .into_event()
        .unwrap_or_default()
        .into())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub(crate) enum BuildPreliminaryTransformRequestError {
    #[error(transparent)]
    TransformNotDefined(
        #[from]
        #[backtrace]
        TransformNotDefinedError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<BuildPreliminaryTransformRequestError> for TransformPlanError {
    fn from(value: BuildPreliminaryTransformRequestError) -> Self {
        match value {
            BuildPreliminaryTransformRequestError::TransformNotDefined(e) => {
                Self::TransformNotDefined(e)
            }
            BuildPreliminaryTransformRequestError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub(crate) enum GetTransformInputError {
    #[error(transparent)]
    InputSchemaNotDefined(
        #[from]
        #[backtrace]
        InputSchemaNotDefinedError,
    ),
    #[error(transparent)]
    InvalidInputInterval(
        #[from]
        #[backtrace]
        InvalidInputIntervalError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetTransformInputError> for TransformElaborateError {
    fn from(value: GetTransformInputError) -> Self {
        match value {
            GetTransformInputError::InputSchemaNotDefined(e) => Self::InputSchemaNotDefined(e),
            GetTransformInputError::InvalidInputInterval(e) => Self::InvalidInputInterval(e),
            GetTransformInputError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<GetTransformInputError> for VerifyTransformPlanError {
    fn from(value: GetTransformInputError) -> Self {
        match value {
            GetTransformInputError::InputSchemaNotDefined(e) => Self::InputSchemaNotDefined(e),
            GetTransformInputError::InvalidInputInterval(e) => Self::InvalidInputInterval(e),
            GetTransformInputError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
