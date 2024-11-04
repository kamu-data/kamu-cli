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
use engine::{TransformRequestExt, TransformRequestInputExt, TransformResponseExt};
use internal_error::ResultIntoInternal;
use kamu_core::*;
use kamu_ingest_datafusion::DataWriterDataFusion;
use opendatafabric::{
    EnumWithVariants,
    ExecuteTransform,
    ExecuteTransformInput,
    SetDataSchema,
    Transform,
    TransformInput,
};

use super::get_transform_input_from_query_input;
use crate::build_preliminary_request_ext;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformExecutionServiceImpl {
    compaction_svc: Arc<dyn CompactionService>,
    engine_provisioner: Arc<dyn EngineProvisioner>,
}

#[component(pub)]
#[interface(dyn TransformExecutionService)]
impl TransformExecutionServiceImpl {
    pub fn new(
        compaction_svc: Arc<dyn CompactionService>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
    ) -> Self {
        Self {
            compaction_svc,
            engine_provisioner,
        }
    }

    async fn elaborate_preliminary_request(
        &self,
        preliminary_request: TransformPreliminaryRequestExt,
        datasets_map: &WorkingDatasetsMap,
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
        input_decl: TransformInput,
        input_state: Option<ExecuteTransformInput>,
        datasets_map: &WorkingDatasetsMap,
    ) -> Result<TransformRequestInputExt, TransformElaborateError> {
        let dataset_id = input_decl.dataset_ref.id().unwrap();
        if let Some(input_state) = &input_state {
            assert_eq!(*dataset_id, input_state.dataset_id);
        }

        let dataset = datasets_map.get_by_id(dataset_id);
        let input_chain = dataset.as_metadata_chain();

        // Determine last processed input block and offset
        let last_processed_block = input_state.as_ref().and_then(|i| i.last_block_hash());
        let last_processed_offset = input_state
            .as_ref()
            .and_then(ExecuteTransformInput::last_offset);

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

        get_transform_input_from_query_input(
            query_input,
            input_decl.alias.clone().unwrap(),
            None,
            datasets_map,
        )
        .await
        .map_err(Into::into)
    }

    // Note: Can be called from multiple threads
    #[tracing::instrument(level = "info", skip_all, fields(operation_id = %request.operation_id))]
    async fn do_transform<CommitFn, Fut>(
        engine_provisioner: Arc<dyn EngineProvisioner>,
        request: TransformRequestExt,
        datasets_map: &WorkingDatasetsMap,
        commit_fn: CommitFn,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, TransformExecuteError>
    where
        CommitFn: FnOnce(TransformRequestExt, TransformResponseExt) -> Fut,
        Fut: futures::Future<Output = Result<TransformResult, TransformExecuteError>>,
    {
        tracing::info!(?request, "Transform request");

        listener.begin();

        match Self::do_transform_inner(
            engine_provisioner,
            request,
            datasets_map,
            commit_fn,
            listener.clone(),
        )
        .await
        {
            Ok(res) => {
                tracing::info!("Transform successful");
                listener.success(&res);
                Ok(res)
            }
            Err(err) => {
                tracing::error!(error = ?err, error_msg = %err, "Transform failed");
                listener.execute_error(&err);
                Err(err)
            }
        }
    }

    // Note: Can be called from multiple threads
    async fn do_transform_inner<CommitFn, Fut>(
        engine_provisioner: Arc<dyn EngineProvisioner>,
        request: TransformRequestExt,
        datasets_map: &WorkingDatasetsMap,
        commit_fn: CommitFn,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, TransformExecuteError>
    where
        CommitFn: FnOnce(TransformRequestExt, TransformResponseExt) -> Fut,
        Fut: futures::Future<Output = Result<TransformResult, TransformExecuteError>>,
    {
        let engine = engine_provisioner
            .provision_engine(
                match request.transform {
                    Transform::Sql(ref sql) => &sql.engine,
                },
                listener.clone().get_engine_provisioning_listener(),
            )
            .await?;

        let response = engine
            .execute_transform(request.clone(), datasets_map)
            .await?;
        assert_eq!(
            response.new_offset_interval.is_some(),
            response.new_data.is_some()
        );

        commit_fn(request, response).await
    }

    async fn commit_execute_transform(
        dataset: Arc<dyn Dataset>,
        request: TransformRequestExt,
        response: TransformResponseExt,
    ) -> Result<TransformResult, TransformExecuteError> {
        let old_head = request.head.clone();
        let mut new_head = old_head.clone();

        if response.output_schema.is_none() {
            tracing::warn!("Engine did not produce a schema. In future this will become an error.");
        };

        if let Some(prev_schema) = request.schema {
            // Validate schema
            if let Some(new_schema) = response.output_schema {
                DataWriterDataFusion::validate_output_schema_equivalence(&prev_schema, &new_schema)
                    .int_err()?;
            }
        } else {
            // Set schema upon first transform
            if let Some(new_schema) = response.output_schema {
                // TODO: make schema commit atomic with data
                let commit_schema_result = dataset
                    .commit_event(
                        SetDataSchema::new(&new_schema).into(),
                        CommitOpts {
                            block_ref: &request.block_ref,
                            system_time: Some(request.system_time),
                            prev_block_hash: Some(Some(&new_head)),
                            check_object_refs: false,
                            update_block_ref: true,
                        },
                    )
                    .await?;

                new_head = commit_schema_result.new_head;
            }
        }

        let params = ExecuteTransformParams {
            query_inputs: request.inputs.iter().map(|i| i.clone().into()).collect(),
            prev_checkpoint: request.prev_checkpoint,
            prev_offset: request.prev_offset,
            new_offset_interval: response.new_offset_interval,
            new_watermark: response.new_watermark,
        };

        match dataset
            .commit_execute_transform(
                params,
                response.new_data,
                response.new_checkpoint.map(CheckpointRef::New),
                CommitOpts {
                    block_ref: &request.block_ref,
                    system_time: Some(request.system_time),
                    prev_block_hash: Some(Some(&new_head)),
                    check_object_refs: true,
                    update_block_ref: true,
                },
            )
            .await
        {
            Ok(res) => {
                new_head = res.new_head;
                Ok(())
            }
            Err(CommitError::MetadataAppendError(AppendError::InvalidBlock(
                AppendValidationError::NoOpEvent(_),
            ))) => Ok(()),
            Err(err) => Err(err),
        }?;

        assert_ne!(
            old_head, new_head,
            "Commit did not update neither schema nor data"
        );

        Ok(TransformResult::Updated { old_head, new_head })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TransformExecutionService for TransformExecutionServiceImpl {
    async fn elaborate_transform(
        &self,
        target: ResolvedDataset,
        plan: TransformPreliminaryPlan,
        options: &TransformOptions,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformElaboration, TransformElaborateError> {
        tracing::info!(target=%target.handle, options=?options, "Elaborating transform");
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

                let compaction_result = self
                    .compaction_svc
                    .compact_dataset(
                        target.clone(),
                        CompactionOptions {
                            keep_metadata_only: true,
                            ..Default::default()
                        },
                        None,
                    )
                    .await
                    .int_err()?;

                if let CompactionResult::Success { .. } = compaction_result {
                    // Recursing to try again after compaction
                    self.elaborate_transform(
                        target.clone(),
                        TransformPreliminaryPlan {
                            preliminary_request: build_preliminary_request_ext(
                                target,
                                plan.preliminary_request.system_time,
                            )
                            .await
                            .int_err()?,
                            datasets_map: plan.datasets_map,
                        },
                        &TransformOptions {
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

    async fn execute_transform(
        &self,
        target: ResolvedDataset,
        plan: TransformPlan,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> (
        ResolvedDataset,
        Result<TransformResult, TransformExecuteError>,
    ) {
        tracing::info!(target=%target.handle, "Executing transform");

        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullTransformListener));

        (
            target.clone(),
            Self::do_transform(
                self.engine_provisioner.clone(),
                plan.request,
                &plan.datasets_map,
                |request, response| async move {
                    Self::commit_execute_transform(target.dataset, request, response).await
                },
                listener,
            )
            .await,
        )
    }

    async fn execute_verify_transform(
        &self,
        target: ResolvedDataset,
        verification_operation: VerifyTransformOperation,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<(), VerifyTransformExecuteError> {
        let listener = maybe_listener.unwrap_or(Arc::new(NullVerificationListener {}));

        let num_steps = verification_operation.steps.len();
        listener.begin_phase(VerificationPhase::ReplayTransform);

        for (step_index, step) in verification_operation.steps.into_iter().enumerate() {
            let request = step.request;

            let block_hash = step.expected_hash;
            let expected_block = step.expected_block;
            let expected_event = expected_block
                .event
                .into_variant::<ExecuteTransform>()
                .unwrap();

            // Will be set during "commit" step
            let mut actual_event: Option<ExecuteTransform> = None;

            tracing::info!(
                %block_hash,
                "Replaying block"
            );

            listener.begin_block(
                &block_hash,
                step_index,
                num_steps,
                VerificationPhase::ReplayTransform,
            );

            let transform_listener = listener
                .clone()
                .get_transform_listener()
                .unwrap_or_else(|| Arc::new(NullTransformListener));

            let ds = target.dataset.clone();
            let out_event = &mut actual_event;

            let result = TransformResult::Updated {
                old_head: expected_block.prev_block_hash.clone().unwrap(),
                new_head: block_hash.clone(),
            };

            Self::do_transform(
                self.engine_provisioner.clone(),
                request,
                &verification_operation.datasets_map,
                |request, response| async move {
                    let params = ExecuteTransformParams {
                        query_inputs: request.inputs.iter().map(|i| i.clone().into()).collect(),
                        prev_checkpoint: request.prev_checkpoint,
                        prev_offset: request.prev_offset,
                        new_offset_interval: response.new_offset_interval,
                        new_watermark: response.new_watermark,
                    };

                    // We expect outputs to be cleaned up automatically on drop
                    let new_event = ds
                        .prepare_execute_transform(
                            params,
                            response.new_data.as_ref(),
                            response.new_checkpoint.map(CheckpointRef::New).as_ref(),
                        )
                        .await?;

                    *out_event = Some(new_event);

                    // This result is ignored
                    Ok(result)
                },
                transform_listener,
            )
            .await
            .map_err(|e| match e {
                TransformExecuteError::EngineProvisioningError(e) => {
                    VerifyTransformExecuteError::EngineProvisioningError(e)
                }
                TransformExecuteError::EngineError(e) => {
                    VerifyTransformExecuteError::EngineError(e)
                }
                TransformExecuteError::CommitError(_) => unreachable!(),
                TransformExecuteError::Internal(e) => VerifyTransformExecuteError::Internal(e),
            })?;

            let actual_event = actual_event.unwrap();

            tracing::debug!(%block_hash, ?expected_event, ?actual_event, "Comparing expected and replayed events");

            let mut cmp_actual_event = actual_event.clone();

            // Parquet format is non-reproducible, so we rely only on logical hash for
            // equivalence test and overwrite the physical hash and size with
            // the expected values for comparison
            if let Some(actual_slice) = &mut cmp_actual_event.new_data {
                if let Some(expected_slice) = &expected_event.new_data {
                    actual_slice.physical_hash = expected_slice.physical_hash.clone();
                    actual_slice.size = expected_slice.size;
                }
            }

            // Currently we're considering checkpoints non-reproducible and thus exclude
            // them from equivalence test
            cmp_actual_event
                .new_checkpoint
                .clone_from(&expected_event.new_checkpoint);

            if expected_event != cmp_actual_event {
                tracing::warn!(%block_hash, ?expected_event, ?actual_event, "Data is not reproducible");

                let err = VerifyTransformExecuteError::DataNotReproducible(DataNotReproducible {
                    block_hash,
                    expected_event: Box::new(expected_event.into()),
                    actual_event: Box::new(actual_event.into()),
                });
                listener.transform_error(&err);
                return Err(err);
            }

            tracing::info!(%block_hash, "Block is valid");
            listener.end_block(
                &block_hash,
                step_index,
                num_steps,
                VerificationPhase::ReplayTransform,
            );
        }

        listener.end_phase(VerificationPhase::ReplayTransform);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
