// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use engine::{TransformRequestExt, TransformRequestInputExt};
use internal_error::ResultIntoInternal;
use kamu_core::*;
use time_source::SystemTimeSource;

use super::get_transform_input_from_query_input;
use crate::build_preliminary_request_ext;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformElaborationServiceImpl {
    compaction_planner: Arc<dyn CompactionPlanner>,
    compaction_executor: Arc<dyn CompactionExecutor>,
    time_source: Arc<dyn SystemTimeSource>,
}

#[component(pub)]
#[interface(dyn TransformElaborationService)]
impl TransformElaborationServiceImpl {
    pub fn new(
        compaction_planner: Arc<dyn CompactionPlanner>,
        compaction_executor: Arc<dyn CompactionExecutor>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            compaction_planner,
            compaction_executor,
            time_source,
        }
    }

    async fn elaborate_preliminary_request(
        &self,
        preliminary_request: TransformPreliminaryRequestExt,
        datasets_map: &ResolvedDatasetsMap,
    ) -> Result<Option<TransformRequestExt>, TransformElaborateError> {
        use futures::{StreamExt, TryStreamExt};
        let inputs: Vec<_> = futures::stream::iter(preliminary_request.input_states)
            .then(|(input_decl, input_state)| {
                self.get_transform_input(input_decl, input_state, datasets_map)
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
        if inputs
            .iter()
            .all(|i| i.data_slices.is_empty() && i.explicit_watermarks.is_empty())
            && preliminary_request.schema.is_some()
        {
            return Ok(None);
        }

        let final_request: TransformRequestExt = TransformRequestExt {
            operation_id: preliminary_request.operation_id,
            dataset_handle: preliminary_request.dataset_handle,
            block_ref: preliminary_request.block_ref,
            head: preliminary_request.head,
            transform: preliminary_request.transform,
            system_time: preliminary_request.system_time,
            schema: preliminary_request.schema,
            prev_offset: preliminary_request.prev_offset,
            vocab: preliminary_request.vocab,
            inputs,
            prev_checkpoint: preliminary_request.prev_checkpoint,
        };

        Ok(Some(final_request))
    }

    async fn get_transform_input(
        &self,
        input_decl: odf::metadata::TransformInput,
        input_state: Option<odf::metadata::ExecuteTransformInput>,
        datasets_map: &ResolvedDatasetsMap,
    ) -> Result<TransformRequestInputExt, TransformElaborateError> {
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

        let query_input = odf::metadata::ExecuteTransformInput {
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

        get_transform_input_from_query_input(
            query_input,
            input_decl.alias.clone().unwrap(),
            None,
            datasets_map,
        )
        .await
        .map_err(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TransformElaborationService for TransformElaborationServiceImpl {
    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle(), ?options))]
    async fn elaborate_transform(
        &self,
        target: ResolvedDataset,
        plan: TransformPreliminaryPlan,
        options: TransformOptions,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformElaboration, TransformElaborateError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullTransformListener));

        match self
            .elaborate_preliminary_request(plan.preliminary_request.clone(), &plan.datasets_map)
            .await
        {
            Ok(Some(request)) => Ok(TransformElaboration::Elaborated(TransformPlan {
                request,
                datasets_map: plan.datasets_map,
            })),
            Ok(None) => Ok(TransformElaboration::UpToDate),
            // TODO: Trapping the error to preserve old behavior - we should consider
            // surfacing it and handling on upper layers
            Err(TransformElaborateError::InputSchemaNotDefined(e)) => {
                tracing::info!(
                    input = %e.dataset_handle,
                    "Not processing because one of the inputs was never pulled",
                );
                listener.begin();
                listener.success(&TransformResult::UpToDate);
                Ok(TransformElaboration::UpToDate)
            }
            Err(err @ TransformElaborateError::InvalidInputInterval(_))
                if options.reset_derivatives_on_diverged_input =>
            {
                tracing::warn!(
                    error = %err,
                    "Interval error detected - resetting on diverged input",
                );

                let compaction_plan = self
                    .compaction_planner
                    .plan_compaction(
                        target.clone(),
                        CompactionOptions {
                            keep_metadata_only: true,
                            ..Default::default()
                        },
                        None,
                    )
                    .await
                    .int_err()?;

                let compaction_result = self
                    .compaction_executor
                    .execute(target.clone(), compaction_plan, None)
                    .await
                    .int_err()?;

                if let CompactionResult::Success {
                    old_head, new_head, ..
                } = compaction_result
                {
                    // Set reference on the compacted dataset
                    target
                        .as_metadata_chain()
                        .set_ref(
                            &odf::BlockRef::Head,
                            &new_head,
                            odf::dataset::SetRefOpts {
                                validate_block_present: true,
                                check_ref_is: Some(Some(&old_head)),
                            },
                        )
                        .await
                        .int_err()?;

                    // Recursing to try again after compaction
                    self.elaborate_transform(
                        target.clone(),
                        TransformPreliminaryPlan {
                            preliminary_request: build_preliminary_request_ext(
                                target,
                                self.time_source.now(),
                            )
                            .await
                            .int_err()?,
                            datasets_map: plan.datasets_map,
                        },
                        TransformOptions {
                            reset_derivatives_on_diverged_input: false,
                        },
                        Some(listener),
                    )
                    .await
                } else {
                    Err(err)
                }
            }
            Err(e) => Err(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
