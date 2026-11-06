// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::engine::TransformRequestInputExt;
use kamu_core::{
    InputNotPulledError,
    InvalidInputIntervalError,
    TransformElaborateError,
    TransformInputsChangedError,
    TransformNotDefinedError,
    TransformStatusError,
    VerifyTransformPlanError,
};
use kamu_datasets::{ResolvedDataset, ResolvedDatasetsMap};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The derivative's side of a transform, read from its chain, with the
/// inputs it consumes resolved
pub(crate) struct TransformPreliminaryPlan {
    pub preliminary_request: TransformPreliminaryRequestExt,
    pub datasets_map: ResolvedDatasetsMap,
}

#[derive(Debug, Clone)]
pub(crate) struct TransformPreliminaryRequestExt {
    /// Identifies the output dataset
    pub dataset_handle: odf::DatasetHandle,
    /// Block reference to advance upon commit
    pub block_ref: odf::BlockRef,
    /// Head the plan is based on (for concurrency control)
    pub head: odf::Multihash,
    /// Transformation that will be applied to produce new data
    pub transform: odf::metadata::Transform,
    /// Hash of the `SetTransform` block that defines the transformation
    pub transform_block: odf::Multihash,
    /// Expected data schema (if already defined)
    pub schema: Option<SchemaRef>,
    /// Preceding record offset, if any
    pub prev_offset: Option<u64>,
    /// Each input with what the last transform consumed of it, if anything
    pub input_states: Vec<(
        odf::metadata::TransformInput,
        Option<odf::metadata::ExecuteTransformInput>,
    )>,
    /// Output dataset's vocabulary
    pub vocab: odf::metadata::DatasetVocabulary,
    /// Previous checkpoint, if any
    pub prev_checkpoint: Option<odf::Multihash>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds the request as of `maybe_head`, or as of the current head
#[tracing::instrument(level = "info", skip_all)]
pub(crate) async fn build_preliminary_request_ext(
    target: ResolvedDataset,
    maybe_head: Option<&odf::Multihash>,
) -> Result<TransformPreliminaryRequestExt, BuildPreliminaryTransformRequestError> {
    let dataset_kind = target.get_kind();
    if dataset_kind != odf::DatasetKind::Derivative {
        return Err(TransformNotDefinedError {}.into());
    }

    let output_chain = target.as_metadata_chain();

    let block_ref = odf::BlockRef::Head;
    let head = match maybe_head {
        Some(head) => head.clone(),
        None => output_chain.resolve_ref(&block_ref).await.int_err()?,
    };

    // TODO: PERF: Search for source, vocab, and data schema result in full scan
    let (source, schema, set_vocab, prev_query) = {
        // TODO: Support transform evolution
        let mut set_transform_visitor = odf::dataset::SearchSetTransformVisitor::new(dataset_kind);
        let mut set_vocab_visitor = odf::dataset::SearchSetVocabVisitor::new();
        let mut set_data_schema_visitor = odf::dataset::SearchSetDataSchemaVisitor::new();
        let mut execute_transform_visitor =
            odf::dataset::SearchExecuteTransformVisitor::new(dataset_kind);

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
            set_transform_visitor.into_inner().into_hashed_block(),
            set_data_schema_visitor
                .into_event()
                .as_ref()
                .map(|e| e.schema_as_arrow(&odf::metadata::ToArrowSettings::default()))
                .transpose() // Option<Result<Schema, E>> -> Result<Option<Schema>, E>
                .int_err()?
                .map(Arc::new),
            set_vocab_visitor.into_event(),
            execute_transform_visitor.into_inner().into_event(),
        )
    };

    let Some((transform_block, source)) = source.map(|(hash, block)| (hash, block.event)) else {
        return Err(TransformNotDefinedError {}.into());
    };
    tracing::debug!(?source, "Transforming using source");

    // Pair each input with what the last transform consumed of it
    let input_states: Vec<(
        odf::metadata::TransformInput,
        Option<odf::metadata::ExecuteTransformInput>,
    )> = if let Some(query) = &prev_query {
        let declared_ids = source.inputs.iter().map(|i| i.dataset_ref.id());
        let consumed_ids = query.query_inputs.iter().map(|i| Some(&i.dataset_id));
        if !declared_ids.eq(consumed_ids) {
            return Err(TransformInputsChangedError {
                dataset_handle: target.get_handle().clone(),
            }
            .into());
        }

        source
            .inputs
            .iter()
            .cloned()
            .zip(query.query_inputs.iter().cloned().map(Some))
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
        transform_block,
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

/// Computes the input interval up to `maybe_input_head`, or up to the input's
/// current head
pub(crate) async fn get_transform_query_input(
    input_decl: odf::metadata::TransformInput,
    input_state: Option<odf::metadata::ExecuteTransformInput>,
    maybe_input_head: Option<&odf::Multihash>,
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
    let last_unprocessed_block = match maybe_input_head {
        Some(input_head) => input_head.clone(),
        None => input_chain
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .int_err()?,
    };

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

/// Collects the input blocks the next transform would consume: from the
/// input's new head back to, but excluding, the block consumed by the last
/// transform. Fails with `InvalidInputInterval` when the new head no longer
/// descends from that block, i.e. the input history was rewritten.
pub(crate) async fn collect_unprocessed_input_blocks(
    query_input: &odf::metadata::ExecuteTransformInput,
    input: &ResolvedDataset,
) -> Result<Vec<(odf::Multihash, odf::MetadataBlock)>, CollectUnprocessedInputBlocksError> {
    let Some(new_block_hash) = &query_input.new_block_hash else {
        return Ok(Vec::new());
    };

    use futures::TryStreamExt;
    input
        .as_metadata_chain()
        .iter_blocks_interval(
            new_block_hash.into(),
            query_input.prev_block_hash.as_ref().map(Into::into),
            false,
        )
        .try_collect()
        .await
        .map_err(|chain_err| match chain_err {
            odf::IterBlocksError::InvalidInterval(err) => {
                CollectUnprocessedInputBlocksError::InvalidInputInterval(
                    InvalidInputIntervalError {
                        head: err.head,
                        tail: err.tail,
                        input_dataset: input.get_handle().clone(),
                    },
                )
            }
            odf::IterBlocksError::RefNotFound(_)
            | odf::IterBlocksError::BlockNotFound(_)
            | odf::IterBlocksError::BlockVersion(_)
            | odf::IterBlocksError::BlockMalformed(_)
            | odf::IterBlocksError::Access(_)
            | odf::IterBlocksError::Internal(_) => {
                CollectUnprocessedInputBlocksError::Internal(chain_err.int_err())
            }
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_transform_input_from_query_input(
    query_input: odf::metadata::ExecuteTransformInput,
    alias: String,
    datasets_map: &ResolvedDatasetsMap,
) -> Result<TransformRequestInputExt, GetTransformInputError> {
    let resolved_input = datasets_map.get_by_id(&query_input.dataset_id);
    let input_chain = resolved_input.as_metadata_chain();

    // Schema and vocabulary are read as of the last consumed block, so that a
    // request rebuilt later does not see metadata the input gained since
    let consumed_head = match query_input
        .new_block_hash
        .as_ref()
        .or(query_input.prev_block_hash.as_ref())
    {
        Some(consumed_head) => consumed_head.clone(),
        None => input_chain
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .int_err()?,
    };

    // Find schema
    // TODO: PERF: Make single-pass via multi-visitor
    use odf::dataset::MetadataChainExt;
    let schema = input_chain
        .accept_one_by_hash(
            &consumed_head,
            odf::dataset::SearchSetDataSchemaVisitor::new(),
        )
        .await
        .int_err()?
        .into_event()
        .map(|e| e.schema_as_arrow(&odf::metadata::ToArrowSettings::default()))
        .transpose()
        .int_err()?
        .ok_or_else(|| InputNotPulledError {
            dataset_handle: resolved_input.get_handle().clone(),
        })?;

    // Collect unprocessed input blocks
    let blocks_unprocessed = collect_unprocessed_input_blocks(&query_input, resolved_input).await?;

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

    let vocab = get_vocab(input_chain, &consumed_head).await?;

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
    chain: &dyn odf::MetadataChain,
    head: &odf::Multihash,
) -> Result<odf::metadata::DatasetVocabulary, InternalError> {
    use odf::dataset::MetadataChainExt;
    Ok(chain
        .accept_one_by_hash(head, odf::dataset::SearchSetVocabVisitor::new())
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
    InputsChanged(
        #[from]
        #[backtrace]
        TransformInputsChangedError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<BuildPreliminaryTransformRequestError> for TransformElaborateError {
    fn from(value: BuildPreliminaryTransformRequestError) -> Self {
        match value {
            BuildPreliminaryTransformRequestError::TransformNotDefined(e) => {
                Self::TransformNotDefined(e)
            }
            BuildPreliminaryTransformRequestError::InputsChanged(e) => Self::InputsChanged(e),
            BuildPreliminaryTransformRequestError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<BuildPreliminaryTransformRequestError> for TransformStatusError {
    fn from(value: BuildPreliminaryTransformRequestError) -> Self {
        match value {
            BuildPreliminaryTransformRequestError::TransformNotDefined(e) => {
                Self::TransformNotDefined(e)
            }
            BuildPreliminaryTransformRequestError::InputsChanged(e) => Self::InputsChanged(e),
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
pub(crate) enum CollectUnprocessedInputBlocksError {
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

impl From<CollectUnprocessedInputBlocksError> for GetTransformInputError {
    fn from(value: CollectUnprocessedInputBlocksError) -> Self {
        match value {
            CollectUnprocessedInputBlocksError::InvalidInputInterval(e) => {
                Self::InvalidInputInterval(e)
            }
            CollectUnprocessedInputBlocksError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub(crate) enum GetTransformInputError {
    #[error(transparent)]
    InputNotPulled(
        #[from]
        #[backtrace]
        InputNotPulledError,
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
            GetTransformInputError::InputNotPulled(e) => Self::InputNotPulled(e),
            GetTransformInputError::InvalidInputInterval(e) => Self::InvalidInputInterval(e),
            GetTransformInputError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<GetTransformInputError> for VerifyTransformPlanError {
    fn from(value: GetTransformInputError) -> Self {
        match value {
            GetTransformInputError::InputNotPulled(e) => Self::InputNotPulled(e),
            GetTransformInputError::InvalidInputInterval(e) => Self::InvalidInputInterval(e),
            GetTransformInputError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
