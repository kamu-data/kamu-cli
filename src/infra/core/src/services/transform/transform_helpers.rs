// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::engine::TransformRequestInputExt;
use kamu_core::{
    InputSchemaNotDefinedError,
    InvalidInputIntervalError,
    ResolvedDataset,
    ResolvedDatasetsMap,
    TransformElaborateError,
    TransformNotDefinedError,
    TransformPlanError,
    TransformPreliminaryRequestExt,
    VerifyTransformPlanError,
};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "info", skip_all)]
pub async fn build_preliminary_request_ext(
    target: ResolvedDataset,
) -> Result<TransformPreliminaryRequestExt, BuildPreliminaryTransformRequestError> {
    let output_chain = target.as_metadata_chain();

    // TODO: externalize
    let block_ref = odf::BlockRef::Head;
    let head = output_chain.resolve_ref(&block_ref).await.int_err()?;

    // TODO: PERF: Search for source, vocab, and data schema result in full scan
    let (source, schema, set_vocab, prev_query) = {
        // TODO: Support transform evolution
        let mut set_transform_visitor = odf::dataset::SearchSetTransformVisitor::new();
        let mut set_vocab_visitor = odf::dataset::SearchSetVocabVisitor::new();
        let mut set_data_schema_visitor = odf::dataset::SearchSetDataSchemaVisitor::new();
        let mut execute_transform_visitor = odf::dataset::SearchExecuteTransformVisitor::new();

        use odf::dataset::MetadataChainExt;
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
                .map(|e| e.schema_as_arrow(&odf::metadata::ToArrowSettings::default()))
                .transpose() // Option<Result<Schema, E>> -> Result<Option<Schema>, E>
                .int_err()?
                .map(Arc::new),
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
    let input_states: Vec<(
        odf::metadata::TransformInput,
        Option<odf::metadata::ExecuteTransformInput>,
    )> = if let Some(query) = &prev_query {
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
        dataset_handle: target.get_handle().clone(),
        block_ref,
        head,
        transform: source.transform,
        schema,
        prev_offset: prev_query
            .as_ref()
            .and_then(odf::metadata::ExecuteTransform::last_offset),
        vocab: set_vocab.unwrap_or_default().into(),
        input_states,
        prev_checkpoint: prev_query.and_then(|q| q.new_checkpoint.map(|c| c.physical_hash)),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_transform_query_input(
    input_decl: odf::metadata::TransformInput,
    input_state: Option<odf::metadata::ExecuteTransformInput>,
    datasets_map: &ResolvedDatasetsMap,
) -> Result<odf::metadata::ExecuteTransformInput, GetTransformQueryInputError> {
    let dataset_id = input_decl.dataset_ref.id().unwrap();
    if let Some(input_state) = &input_state {
        assert_eq!(*dataset_id, input_state.dataset_id);
    }

    let target = datasets_map.get_by_id(dataset_id);
    let input_chain = target.as_metadata_chain();

    // Determine last processed input block and offset
    let last_processed_block = input_state.as_ref().and_then(|i| i.last_block_hash());
    let last_processed_offset = input_state
        .as_ref()
        .and_then(odf::metadata::ExecuteTransformInput::last_offset);

    // Determine unprocessed block and offset range
    let last_unprocessed_block = input_chain
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .int_err()?;

    use odf::dataset::MetadataChainExt;
    let last_unprocessed_offset = input_chain
        .accept_one_by_hash(
            &last_unprocessed_block,
            odf::dataset::SearchSingleDataBlockVisitor::next(),
        )
        .await
        .int_err()?
        .into_event()
        .and_then(|event| event.last_offset())
        .or(last_processed_offset);

    Ok(odf::metadata::ExecuteTransformInput {
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
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_transform_input_from_query_input(
    query_input: odf::metadata::ExecuteTransformInput,
    alias: String,
    vocab_hint: Option<odf::metadata::DatasetVocabulary>,
    datasets_map: &ResolvedDatasetsMap,
) -> Result<TransformRequestInputExt, GetTransformInputError> {
    let resolved_input = datasets_map.get_by_id(&query_input.dataset_id);
    let input_chain = resolved_input.as_metadata_chain();

    // Find schema
    // TODO: PERF: Make single-pass via multi-visitor
    use odf::dataset::MetadataChainExt;
    let schema = resolved_input
        .as_metadata_chain()
        .accept_one(odf::dataset::SearchSetDataSchemaVisitor::new())
        .await
        .int_err()?
        .into_event()
        .map(|e| e.schema_as_arrow(&odf::metadata::ToArrowSettings::default()))
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
                odf::IterBlocksError::InvalidInterval(err) => {
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

    use odf::metadata::IntoDataStreamBlock;
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
            explicit_watermarks.push(odf::metadata::Watermark {
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
        schema: Arc::new(schema),
        explicit_watermarks,
    };

    tracing::info!(?input, is_empty, "Computed transform input");

    Ok(input)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Avoid iterating through output chain multiple times
async fn get_vocab(
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
pub(crate) enum GetTransformQueryInputError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetTransformQueryInputError> for TransformElaborateError {
    fn from(value: GetTransformQueryInputError) -> Self {
        match value {
            GetTransformQueryInputError::Internal(e) => Self::Internal(e),
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
