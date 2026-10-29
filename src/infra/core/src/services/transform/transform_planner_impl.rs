// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::*;
use engine::{TransformRequestExt, TransformRequestInputExt};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::{DatasetRegistry, ResolvedDataset, ResolvedDatasetsMap};
use time_source::SystemTimeSource;

use super::{
    BuildPreliminaryTransformRequestError,
    CollectUnprocessedInputBlocksError,
    GetTransformQueryInputError,
    TransformPreliminaryPlan,
    TransformPreliminaryRequestExt,
    build_preliminary_request_ext,
    collect_unprocessed_input_blocks,
    get_transform_input_from_query_input,
    get_transform_query_input,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformPlannerImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    time_source: Arc<dyn SystemTimeSource>,
}

/// Fixes what elaboration would otherwise read at call time
struct ElaborationPins {
    system_time: DateTime<Utc>,
    input_heads: HashMap<odf::DatasetID, odf::Multihash>,
}

#[component(pub)]
#[interface(dyn TransformPlanner)]
impl TransformPlannerImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            dataset_registry,
            time_source,
        }
    }

    async fn resolve_input(
        &self,
        input: &odf::metadata::TransformInput,
    ) -> Result<ResolvedDataset, InternalError> {
        let handle = self
            .dataset_registry
            .resolve_dataset_handle_by_ref(&input.dataset_ref)
            .await
            .int_err()?;
        Ok(self.dataset_registry.get_dataset_by_handle(&handle).await)
    }

    /// Reads the derivative's side of the next transform as of `maybe_head`,
    /// or as of the current head, and resolves its inputs
    // TODO: PERF: Avoid multiple passes over metadata chain
    async fn plan_preliminary(
        &self,
        target: ResolvedDataset,
        maybe_head: Option<&odf::Multihash>,
    ) -> Result<TransformPreliminaryPlan, BuildPreliminaryTransformRequestError> {
        let preliminary_request = build_preliminary_request_ext(target.clone(), maybe_head).await?;

        let mut datasets_map = ResolvedDatasetsMap::default();
        datasets_map.register(target);
        for (input_decl, _) in &preliminary_request.input_states {
            datasets_map.register(self.resolve_input(input_decl).await?);
        }

        Ok(TransformPreliminaryPlan {
            preliminary_request,
            datasets_map,
        })
    }

    async fn elaborate_preliminary(
        &self,
        plan: TransformPreliminaryPlan,
        maybe_pins: Option<&ElaborationPins>,
    ) -> Result<TransformElaboration, TransformElaborateError> {
        let TransformPreliminaryPlan {
            preliminary_request,
            datasets_map,
        } = plan;

        match self
            .elaborate_preliminary_request(preliminary_request, maybe_pins, &datasets_map)
            .await?
        {
            Some(request) => Ok(TransformElaboration::Elaborated(TransformExecutionPlan {
                request,
                datasets_map,
            })),
            None => Ok(TransformElaboration::UpToDate),
        }
    }

    async fn elaborate_preliminary_request(
        &self,
        preliminary_request: TransformPreliminaryRequestExt,
        maybe_pins: Option<&ElaborationPins>,
        datasets_map: &ResolvedDatasetsMap,
    ) -> Result<Option<TransformRequestExt>, TransformElaborateError> {
        use futures::{StreamExt, TryStreamExt};
        let inputs: Vec<_> = futures::stream::iter(preliminary_request.input_states)
            .then(|(input_decl, input_state)| {
                Self::get_transform_input(input_decl, input_state, maybe_pins, datasets_map)
            })
            .try_collect()
            .await?;

        // Nothing to do?
        // Note that we're considering a schema here, as even if there is no data to
        // process we would like to run the transform to establish the schema of the
        // output.
        //
        // TODO: Detect the situation where inputs only had source updates and skip
        // running the engine
        if inputs.iter().all(|i| !i.has_new_data_or_watermarks())
            && preliminary_request.schema.is_some()
        {
            return Ok(None);
        }

        Ok(Some(TransformRequestExt {
            dataset_handle: preliminary_request.dataset_handle,
            block_ref: preliminary_request.block_ref,
            head: preliminary_request.head,
            transform: preliminary_request.transform,
            transform_block: preliminary_request.transform_block,
            system_time: match maybe_pins {
                Some(pins) => pins.system_time,
                None => self.time_source.now(),
            },
            schema: preliminary_request.schema,
            prev_offset: preliminary_request.prev_offset,
            vocab: preliminary_request.vocab,
            inputs,
            prev_checkpoint: preliminary_request.prev_checkpoint,
        }))
    }

    async fn get_transform_input(
        input_decl: odf::metadata::TransformInput,
        input_state: Option<odf::metadata::ExecuteTransformInput>,
        maybe_pins: Option<&ElaborationPins>,
        datasets_map: &ResolvedDatasetsMap,
    ) -> Result<TransformRequestInputExt, TransformElaborateError> {
        let input_alias = input_decl.alias.clone().unwrap();

        let maybe_input_head = match maybe_pins {
            Some(pins) => {
                let input_id = input_decl.dataset_ref.id().unwrap();
                let input_head = pins.input_heads.get(input_id).ok_or_else(|| {
                    format!("Stored transform plan has no head for input {input_id}").int_err()
                })?;
                Some(input_head)
            }
            None => None,
        };

        let query_input =
            get_transform_query_input(input_decl, input_state, maybe_input_head, datasets_map)
                .await?;

        get_transform_input_from_query_input(query_input, input_alias, datasets_map)
            .await
            .map_err(Into::into)
    }

    /// Inputs whose history was rewritten since the last transform consumed
    /// them
    async fn collect_breaking_inputs(
        query_inputs: &[odf::metadata::ExecuteTransformInput],
        datasets_map: &ResolvedDatasetsMap,
    ) -> Result<Vec<odf::metadata::ExecuteTransformInput>, InternalError> {
        let mut breaking_inputs = Vec::new();
        for query_input in query_inputs {
            // Nothing was consumed yet, so there is nothing to diverge from
            if query_input.prev_block_hash.is_none() {
                continue;
            }

            let input = datasets_map.get_by_id(&query_input.dataset_id);
            match collect_unprocessed_input_blocks(query_input, input).await {
                Ok(_) => {}
                Err(CollectUnprocessedInputBlocksError::InvalidInputInterval(_)) => {
                    breaking_inputs.push(query_input.clone());
                }
                Err(CollectUnprocessedInputBlocksError::Internal(e)) => return Err(e),
            }
        }
        Ok(breaking_inputs)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TransformPlanner for TransformPlannerImpl {
    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle()))]
    async fn describe_transform(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformDescription, TransformPlanError> {
        let dataset_kind = target.get_kind();
        if dataset_kind != odf::DatasetKind::Derivative {
            return Err(TransformNotDefinedError {}.into());
        }

        let chain = target.as_metadata_chain();
        let head = chain.resolve_ref(&odf::BlockRef::Head).await.int_err()?;

        use odf::dataset::MetadataChainExt;
        let Some((transform_block, set_transform)) = chain
            .accept_one_by_hash(
                &head,
                odf::dataset::SearchSetTransformVisitor::new(dataset_kind),
            )
            .await
            .int_err()?
            .into_inner()
            .into_hashed_block()
        else {
            return Err(TransformNotDefinedError {}.into());
        };

        let mut inputs = Vec::with_capacity(set_transform.event.inputs.len());
        for input in &set_transform.event.inputs {
            inputs.push(self.resolve_input(input).await?.take_handle());
        }

        Ok(TransformDescription {
            transform_block,
            transform: set_transform.event.transform,
            inputs,
        })
    }

    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle()))]
    async fn elaborate_transform(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformElaboration, TransformElaborateError> {
        let plan = self.plan_preliminary(target, None).await?;
        self.elaborate_preliminary(plan, None).await
    }

    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle()))]
    async fn rebuild_transform(
        &self,
        target: ResolvedDataset,
        stored: &TransformPlanPayload,
    ) -> Result<TransformExecutionPlan, TransformRebuildError> {
        let dataset_id = target.get_id().clone();
        let mismatch = |reason: String| TransformPlanMismatchError {
            dataset_id: dataset_id.clone(),
            reason,
        };

        if stored.dataset_id != dataset_id {
            return Err(mismatch(format!("the plan is for dataset {}", stored.dataset_id)).into());
        }

        let actual_head = target
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .int_err()?;
        if actual_head != stored.expected_head {
            return Err(TransformHeadMovedError {
                dataset_id,
                expected_head: stored.expected_head.clone(),
                actual_head,
            }
            .into());
        }

        let plan = match self
            .plan_preliminary(target, Some(&stored.expected_head))
            .await
        {
            Ok(plan) => plan,
            Err(BuildPreliminaryTransformRequestError::TransformNotDefined(e)) => {
                return Err(mismatch(e.to_string()).into());
            }
            Err(BuildPreliminaryTransformRequestError::InputsChanged(e)) => {
                return Err(mismatch(e.to_string()).into());
            }
            Err(BuildPreliminaryTransformRequestError::Internal(e)) => return Err(e.into()),
        };

        let pins = ElaborationPins {
            system_time: stored.system_time,
            input_heads: stored.input_heads(),
        };

        let plan = match self.elaborate_preliminary(plan, Some(&pins)).await {
            Ok(TransformElaboration::Elaborated(plan)) => plan,
            Ok(TransformElaboration::UpToDate) => {
                return Err(mismatch("the rebuilt transform is up to date".to_string()).into());
            }
            Err(
                e @ (TransformElaborateError::TransformNotDefined(_)
                | TransformElaborateError::InputsChanged(_)
                | TransformElaborateError::InputNotPulled(_)
                | TransformElaborateError::InvalidInputInterval(_)),
            ) => return Err(mismatch(e.to_string()).into()),
            Err(TransformElaborateError::Internal(e)) => return Err(e.into()),
        };

        if TransformPlanPayload::from_request(&plan.request) != *stored {
            tracing::warn!(?stored, request = ?plan.request, "Rebuilt transform differs from its stored plan");
            return Err(mismatch(
                "the rebuilt request does not map to the stored plan".to_string(),
            )
            .into());
        }

        Ok(plan)
    }

    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle()))]
    async fn evaluate_transform_status(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformStatus, TransformStatusError> {
        let plan = self.plan_preliminary(target, None).await?;

        // Offsets cannot be compared across a rewritten history, so divergence is
        // checked first and takes precedence over any new data
        let mut query_inputs = Vec::with_capacity(plan.preliminary_request.input_states.len());
        for (input_decl, input_state) in &plan.preliminary_request.input_states {
            query_inputs.push(
                get_transform_query_input(
                    input_decl.clone(),
                    input_state.clone(),
                    None,
                    &plan.datasets_map,
                )
                .await
                .map_err(|e| match e {
                    GetTransformQueryInputError::Internal(e) => TransformStatusError::Internal(e),
                })?,
            );
        }
        let breaking_inputs =
            Self::collect_breaking_inputs(&query_inputs, &plan.datasets_map).await?;
        if !breaking_inputs.is_empty() {
            return Ok(TransformStatus::InputBreakingChange { breaking_inputs });
        }

        // An input has advanced exactly when elaboration would consume something from it
        let output_has_schema = plan.preliminary_request.schema.is_some();
        let request = match self.elaborate_preliminary(plan, None).await {
            Ok(TransformElaboration::Elaborated(plan)) => plan.request,
            Ok(TransformElaboration::UpToDate) => return Ok(TransformStatus::UpToDate),
            // The derivative waits for its input
            Err(TransformElaborateError::InputNotPulled(_)) => {
                return Ok(TransformStatus::UpToDate);
            }
            Err(TransformElaborateError::TransformNotDefined(e)) => return Err(e.into()),
            Err(TransformElaborateError::InputsChanged(e)) => return Err(e.into()),
            Err(e @ TransformElaborateError::InvalidInputInterval(_)) => {
                return Err(e.int_err().into());
            }
            Err(TransformElaborateError::Internal(e)) => return Err(e.into()),
        };

        // Before the output has a schema the transform also runs on inputs without new
        // data, so any input with new blocks counts
        let input_advancements: Vec<_> = request
            .inputs
            .into_iter()
            .filter(|input| {
                input.has_new_data_or_watermarks()
                    || (!output_has_schema && input.new_block_hash.is_some())
            })
            .map(Into::into)
            .collect();

        if input_advancements.is_empty() {
            Ok(TransformStatus::UpToDate)
        } else {
            Ok(TransformStatus::NewInputDataAvailable { input_advancements })
        }
    }

    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle(), ?block_range))]
    async fn build_transform_verification_plan(
        &self,
        target: ResolvedDataset,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
    ) -> Result<VerifyTransformOperation, VerifyTransformPlanError> {
        let history = VerificationHistory::collect_range(&target, block_range).await?;

        let mut datasets_map = ResolvedDatasetsMap::default();
        datasets_map.register(target.clone());
        let mut resolved_inputs = HashSet::new();

        let mut steps = Vec::with_capacity(history.transforms.len());
        for (block_hash, block) in history.transforms.into_iter().rev() {
            // Each transform is replayed with the metadata in effect when it ran
            let (transform_block, source) =
                VerificationHistory::in_effect(&history.set_transforms, block.sequence_number)
                    .ok_or_else(|| {
                        "Expected a derivative dataset but SetTransform block was not found"
                            .int_err()
                    })?
                    .clone();
            let schema =
                VerificationHistory::in_effect(&history.set_schemas, block.sequence_number)
                    .map(|e| e.schema_as_arrow(&odf::metadata::ToArrowSettings::default()))
                    .transpose()
                    .int_err()?
                    .map(Arc::new);
            let vocab = VerificationHistory::in_effect(&history.set_vocabs, block.sequence_number)
                .cloned()
                .unwrap_or_default()
                .into();

            for input in &source.inputs {
                if resolved_inputs.insert(input.dataset_ref.id().unwrap().clone()) {
                    datasets_map.register(self.resolve_input(input).await?);
                }
            }

            use odf::metadata::AsTypedBlock;
            let block_t = block.as_typed::<odf::metadata::ExecuteTransform>().unwrap();

            let mut inputs = Vec::with_capacity(block_t.event.query_inputs.len());
            for slice in &block_t.event.query_inputs {
                let alias = source
                    .inputs
                    .iter()
                    .find(|i| i.dataset_ref.id() == Some(&slice.dataset_id))
                    .and_then(|i| i.alias.clone())
                    .ok_or_else(|| TransformInputsChangedError {
                        dataset_handle: target.get_handle().clone(),
                    })?;

                inputs.push(
                    get_transform_input_from_query_input(slice.clone(), alias, &datasets_map)
                        .await?,
                );
            }

            steps.push(VerifyTransformStep {
                request: TransformRequestExt {
                    dataset_handle: target.get_handle().clone(),
                    block_ref: odf::BlockRef::Head,
                    head: block_t.prev_block_hash.unwrap().clone(),
                    transform: source.transform,
                    transform_block,
                    system_time: block.system_time,
                    schema,
                    prev_offset: block_t.event.prev_offset,
                    inputs,
                    vocab,
                    prev_checkpoint: block_t.event.prev_checkpoint.clone(),
                },
                expected_block: block,
                expected_hash: block_hash,
            });
        }

        Ok(VerifyTransformOperation {
            steps,
            datasets_map,
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(target=%target.get_handle(), ?block_range))]
    async fn get_verification_inputs(
        &self,
        target: ResolvedDataset,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
    ) -> Result<Vec<odf::DatasetHandle>, VerifyTransformPlanError> {
        let history = VerificationHistory::collect_range(&target, block_range).await?;

        let mut seen_inputs = HashSet::new();
        let mut inputs = Vec::new();
        for (_, block) in history.transforms.iter().rev() {
            let (_, source) =
                VerificationHistory::in_effect(&history.set_transforms, block.sequence_number)
                    .ok_or_else(|| {
                        "Expected a derivative dataset but SetTransform block was not found"
                            .int_err()
                    })?;

            for input in &source.inputs {
                if seen_inputs.insert(input.dataset_ref.clone()) {
                    let handle = self
                        .dataset_registry
                        .resolve_dataset_handle_by_ref(&input.dataset_ref)
                        .await
                        .int_err()?;
                    inputs.push(handle);
                }
            }
        }

        Ok(inputs)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The `ExecuteTransform` blocks of a verified range, and every block that
/// sets metadata a transform depends on, all newest first
#[derive(Default)]
struct VerificationHistory {
    tail_sequence_number: Option<u64>,
    transforms: Vec<(odf::Multihash, odf::MetadataBlock)>,
    finished_range: bool,
    set_transforms: Vec<(u64, (odf::Multihash, odf::metadata::SetTransform))>,
    set_schemas: Vec<(u64, odf::metadata::SetDataSchema)>,
    set_vocabs: Vec<(u64, odf::metadata::SetVocab)>,
}

impl VerificationHistory {
    /// Collects the history of `block_range` of `target`; the range ends at
    /// `HEAD` unless given
    async fn collect_range(
        target: &ResolvedDataset,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
    ) -> Result<Self, VerifyTransformPlanError> {
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

        let history = Self::collect(metadata_chain, &head, tail_sequence_number)
            .await
            .int_err()?;

        // Ensure start_block was found if specified
        if let Some(tail) = tail
            && !history.finished_range
        {
            return Err(odf::dataset::InvalidIntervalError { head, tail }.into());
        }

        Ok(history)
    }

    async fn collect(
        metadata_chain: &dyn odf::MetadataChain,
        head: &odf::Multihash,
        tail_sequence_number: Option<u64>,
    ) -> Result<Self, InternalError> {
        use odf::dataset::*;
        type Flag = odf::metadata::MetadataEventTypeFlags;
        type Decision = MetadataVisitorDecision;

        let metadata_flags = Flag::SET_TRANSFORM | Flag::SET_DATA_SCHEMA | Flag::SET_VOCAB;

        let mut visitor = GenericCallbackVisitor::new(
            Self {
                tail_sequence_number,
                ..Self::default()
            },
            Decision::NextOfType(metadata_flags | Flag::EXECUTE_TRANSFORM),
            move |state, hash, block| {
                match &block.event {
                    odf::metadata::MetadataEvent::ExecuteTransform(_) => {
                        if Some(block.sequence_number) < state.tail_sequence_number {
                            state.finished_range = true;
                        } else {
                            state.transforms.push((hash.clone(), block.clone()));
                            if Some(block.sequence_number) == state.tail_sequence_number {
                                state.finished_range = true;
                            }
                        }
                    }
                    odf::metadata::MetadataEvent::SetTransform(e) => state
                        .set_transforms
                        .push((block.sequence_number, (hash.clone(), e.clone()))),
                    odf::metadata::MetadataEvent::SetDataSchema(e) => {
                        state.set_schemas.push((block.sequence_number, e.clone()));
                    }
                    odf::metadata::MetadataEvent::SetVocab(e) => {
                        state.set_vocabs.push((block.sequence_number, e.clone()));
                    }
                    odf::metadata::MetadataEvent::AddData(_)
                    | odf::metadata::MetadataEvent::Seed(_)
                    | odf::metadata::MetadataEvent::SetPollingSource(_)
                    | odf::metadata::MetadataEvent::SetAttachments(_)
                    | odf::metadata::MetadataEvent::SetInfo(_)
                    | odf::metadata::MetadataEvent::SetLicense(_)
                    | odf::metadata::MetadataEvent::AddPushSource(_)
                    | odf::metadata::MetadataEvent::DisablePushSource(_)
                    | odf::metadata::MetadataEvent::DisablePollingSource(_) => {}
                }

                // Past the range only metadata that older transforms depend on is needed
                if state.finished_range {
                    Decision::NextOfType(metadata_flags)
                } else {
                    Decision::NextOfType(metadata_flags | Flag::EXECUTE_TRANSFORM)
                }
            },
        );

        metadata_chain
            .accept_by_hash(&mut [&mut visitor], head)
            .await
            .int_err()?;

        Ok(visitor.into_state())
    }

    /// The newest of `events` set before the block `sequence_number`
    fn in_effect<T>(events: &[(u64, T)], sequence_number: u64) -> Option<&T> {
        events
            .iter()
            .find(|(event_sequence_number, _)| *event_sequence_number < sequence_number)
            .map(|(_, event)| event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
