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
    InputNotPulledError,
    InvalidInputIntervalError,
    TransformElaborateError,
    TransformStatusError,
    VerifyTransformPlanError,
};
use kamu_datasets::{ResolvedDataset, ResolvedDatasetsMap};
use thiserror::Error;

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

impl From<GetTransformQueryInputError> for TransformStatusError {
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
