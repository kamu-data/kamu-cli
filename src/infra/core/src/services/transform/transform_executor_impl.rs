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
use engine::{TransformRequestExt, TransformResponseExt};
use internal_error::ResultIntoInternal;
use kamu_core::*;
use kamu_ingest_datafusion::DataWriterDataFusion;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformExecutorImpl {
    engine_provisioner: Arc<dyn EngineProvisioner>,
}

#[component(pub)]
#[interface(dyn TransformExecutor)]
impl TransformExecutorImpl {
    pub fn new(engine_provisioner: Arc<dyn EngineProvisioner>) -> Self {
        Self { engine_provisioner }
    }

    // Note: Can be called from multiple threads
    #[tracing::instrument(level = "info", skip_all, fields(operation_id = %request.operation_id))]
    async fn do_transform<CommitFn, Fut>(
        engine_provisioner: Arc<dyn EngineProvisioner>,
        request: TransformRequestExt,
        datasets_map: &ResolvedDatasetsMap,
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
        datasets_map: &ResolvedDatasetsMap,
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
                    odf::metadata::Transform::Sql(ref sql) => &sql.engine,
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
        resolved_dataset: ResolvedDataset,
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
                let commit_schema_result = resolved_dataset
                    .commit_event(
                        odf::metadata::SetDataSchema::new(&new_schema).into(),
                        odf::dataset::CommitOpts {
                            block_ref: &request.block_ref,
                            system_time: Some(request.system_time),
                            prev_block_hash: Some(Some(&new_head)),
                            check_object_refs: false,
                            update_block_ref: false,
                        },
                    )
                    .await?;

                new_head = commit_schema_result.new_head;
            }
        }

        let params = odf::dataset::ExecuteTransformParams {
            query_inputs: request.inputs.iter().map(|i| i.clone().into()).collect(),
            prev_checkpoint: request.prev_checkpoint,
            prev_offset: request.prev_offset,
            new_offset_interval: response.new_offset_interval,
            new_watermark: response.new_watermark,
        };

        match resolved_dataset
            .commit_execute_transform(
                params,
                response.new_data,
                response
                    .new_checkpoint
                    .map(odf::dataset::CheckpointRef::New),
                odf::dataset::CommitOpts {
                    block_ref: &request.block_ref,
                    system_time: Some(request.system_time),
                    prev_block_hash: Some(Some(&new_head)),
                    check_object_refs: true,
                    update_block_ref: false,
                },
            )
            .await
        {
            Ok(res) => {
                new_head = res.new_head;
                Ok(())
            }
            Err(odf::dataset::CommitError::MetadataAppendError(
                odf::dataset::AppendError::InvalidBlock(
                    odf::dataset::AppendValidationError::NoOpEvent(_),
                ),
            )) => Ok(()),
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
impl TransformExecutor for TransformExecutorImpl {
    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle()))]
    async fn execute_transform(
        &self,
        target: ResolvedDataset,
        plan: TransformPlan,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> (
        ResolvedDataset,
        Result<TransformResult, TransformExecuteError>,
    ) {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullTransformListener));

        (
            target.clone(),
            Self::do_transform(
                self.engine_provisioner.clone(),
                plan.request,
                &plan.datasets_map,
                |request, response| async move {
                    Self::commit_execute_transform(target, request, response).await
                },
                listener,
            )
            .await,
        )
    }

    #[tracing::instrument(level = "info", skip_all, fields(target=%target.get_handle()))]
    async fn execute_verify_transform(
        &self,
        target: ResolvedDataset,
        verification_operation: VerifyTransformOperation,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<(), VerifyTransformExecuteError> {
        let listener = maybe_listener.unwrap_or(Arc::new(NullVerificationListener {}));

        let num_steps = verification_operation.steps.len();
        listener.begin_phase(VerificationPhase::ReplayTransform);

        use odf::metadata::EnumWithVariants;
        for (step_index, step) in verification_operation.steps.into_iter().enumerate() {
            let request = step.request;

            let block_hash = step.expected_hash;
            let expected_block = step.expected_block;
            let expected_event = expected_block
                .event
                .into_variant::<odf::metadata::ExecuteTransform>()
                .unwrap();

            // Will be set during "commit" step
            let mut actual_event: Option<odf::metadata::ExecuteTransform> = None;

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

            let ds = (*target).clone();
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
                    let params = odf::dataset::ExecuteTransformParams {
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
                            response
                                .new_checkpoint
                                .map(odf::dataset::CheckpointRef::New)
                                .as_ref(),
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
