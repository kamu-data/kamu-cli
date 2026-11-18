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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::{
    TransformElaborateError,
    TransformInputsChangedError,
    TransformNotDefinedError,
    TransformStatusError,
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
