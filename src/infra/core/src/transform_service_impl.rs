// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::*;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use kamu_core::engine::*;
use kamu_core::*;
use opendatafabric::*;
use thiserror::Error;

pub struct TransformServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    engine_provisioner: Arc<dyn EngineProvisioner>,
}

#[component(pub)]
#[interface(dyn TransformService)]
impl TransformServiceImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_action_authorizer,
            engine_provisioner,
        }
    }

    // Note: Can be called from multiple threads
    #[tracing::instrument(level = "info", skip_all, fields(operation_id = %request.operation_id))]
    async fn do_transform<CommitFn, Fut>(
        engine_provisioner: Arc<dyn EngineProvisioner>,
        request: TransformRequest,
        commit_fn: CommitFn,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, TransformError>
    where
        CommitFn: FnOnce(ExecuteQueryParams, Option<OwnedFile>, Option<OwnedFile>) -> Fut,
        Fut: futures::Future<Output = Result<TransformResult, TransformError>>,
    {
        tracing::info!(?request, "Transform request");

        listener.begin();

        match Self::do_transform_inner(engine_provisioner, request, commit_fn, listener.clone())
            .await
        {
            Ok(res) => {
                tracing::info!("Transform successful");
                listener.success(&res);
                Ok(res)
            }
            Err(err) => {
                tracing::error!(error = ?err, "Transform failed");
                listener.error(&err);
                Err(err)
            }
        }
    }

    // Note: Can be called from multiple threads
    async fn do_transform_inner<CommitFn, Fut>(
        engine_provisioner: Arc<dyn EngineProvisioner>,
        request: TransformRequest,
        commit_fn: CommitFn,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, TransformError>
    where
        CommitFn: FnOnce(ExecuteQueryParams, Option<OwnedFile>, Option<OwnedFile>) -> Fut,
        Fut: futures::Future<Output = Result<TransformResult, TransformError>>,
    {
        let input_checkpoint = request.prev_checkpoint.clone();
        let input_slices = request.inputs.iter().map(|i| i.clone().into()).collect();

        let engine = engine_provisioner
            .provision_engine(
                match request.transform {
                    Transform::Sql(ref sql) => &sql.engine,
                },
                listener.clone().get_engine_provisioning_listener(),
            )
            .await?;

        let response = engine.transform(request).await?;
        assert_eq!(
            response.data_interval.is_some(),
            response.out_data.is_some()
        );

        commit_fn(
            ExecuteQueryParams {
                input_slices,
                input_checkpoint,
                output_data: response.data_interval,
                output_watermark: response.output_watermark,
            },
            response.out_data,
            response.out_checkpoint,
        )
        .await
    }

    // TODO: PERF: Avoid multiple passes over metadata chain
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn get_next_operation(
        &self,
        dataset_handle: &DatasetHandle,
        system_time: DateTime<Utc>,
    ) -> Result<Option<TransformRequest>, GetNextOperationError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await
            .int_err()?;
        let output_chain = dataset.as_metadata_chain();

        // TODO: limit traversal depth
        let mut sources: Vec<_> = output_chain
            .iter_blocks()
            .try_filter_map(|(_, b)| async move {
                match b.event {
                    MetadataEvent::SetTransform(st) => Ok(Some(st)),
                    MetadataEvent::SetPollingSource(_) => Err("Transform called on \
                                                               non-derivative dataset"
                        .int_err()
                        .into()),
                    _ => Ok(None),
                }
            })
            .try_collect()
            .await
            .int_err()?;

        // TODO: source could've changed several times
        if sources.len() > 1 {
            unimplemented!("Transform evolution is not yet supported");
        }

        let source = sources.pop().unwrap();
        tracing::debug!(?source, "Transforming using source");

        // Check if all inputs are non-empty
        if futures::stream::iter(&source.inputs)
            .map(|input| input.id.as_ref().unwrap().as_local_ref())
            .then(|input_ref| async move { self.is_never_pulled(&input_ref).await })
            .any_ok(|never_pulled| *never_pulled)
            .await?
        {
            tracing::info!("Not processing because one of the inputs was never pulled");
            return Ok(None);
        }

        // Prepare inputs
        let inputs: Vec<_> = futures::stream::iter(&source.inputs)
            .then(|input| self.get_transform_input(input, output_chain))
            .try_collect()
            .await
            .map_err(|e| match e {
                TransformInputError::Access(e) => GetNextOperationError::Access(e),
                TransformInputError::Internal(e) => GetNextOperationError::Internal(e),
            })?;

        // Nothing to do?
        if inputs
            .iter()
            .all(|i| i.data_slices.is_empty() && i.explicit_watermarks.is_empty())
        {
            return Ok(None);
        }

        let vocab = self.get_vocab(&dataset_handle.as_local_ref()).await?;

        let prev_checkpoint = output_chain
            .iter_blocks()
            .filter_map_ok(|(_, b)| b.event.into_variant::<ExecuteQuery>())
            .try_first()
            .await
            .int_err()?
            .and_then(|b| b.output_checkpoint)
            .map(|cp| cp.physical_hash);

        let last_offset = output_chain
            .iter_blocks()
            .filter_map_ok(|(_, b)| b.event.into_variant::<ExecuteQuery>())
            .filter_map_ok(|eq| eq.output_data)
            .map_ok(|s| s.interval.end)
            .try_first()
            .await
            .int_err()?;

        Ok(Some(TransformRequest {
            operation_id: self.next_operation_id(),
            dataset_handle: dataset_handle.clone(),
            transform: source.transform,
            system_time,
            next_offset: last_offset.map(|v| v + 1).unwrap_or(0),
            vocab,
            inputs,
            prev_checkpoint,
        }))
    }

    fn next_operation_id(&self) -> String {
        use rand::distributions::Alphanumeric;
        use rand::Rng;

        let mut name = String::with_capacity(16);
        name.extend(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from),
        );

        name
    }

    async fn is_never_pulled(&self, dataset_ref: &DatasetRef) -> Result<bool, InternalError> {
        let dataset = self.dataset_repo.get_dataset(dataset_ref).await.int_err()?;
        Ok(dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_data_stream_blocks()
            .filter_map_ok(|(_, b)| b.event.output_data)
            .try_first()
            .await
            .int_err()?
            .is_none())
    }

    // TODO: Avoid iterating through output chain multiple times
    async fn get_transform_input(
        &self,
        transform_input: &TransformInput,
        output_chain: &dyn MetadataChain,
    ) -> Result<TransformRequestInput, TransformInputError> {
        let dataset_id = transform_input.id.as_ref().unwrap();
        let dataset_handle = self
            .dataset_repo
            .resolve_dataset_ref(&dataset_id.as_local_ref())
            .await
            .int_err()?;
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await
            .int_err()?;
        let input_chain = dataset.as_metadata_chain();

        // Determine last processed input block
        let last_processed_block = output_chain
            .iter_blocks()
            .filter_map_ok(|(_, b)| b.event.into_variant::<ExecuteQuery>())
            .map_ok(|eq| eq.input_slices)
            .flatten_ok()
            .filter_ok(|slice| slice.dataset_id == *dataset_id)
            .filter_map_ok(|slice| slice.block_interval)
            .map_ok(|bi| bi.end)
            .try_first()
            .await
            .int_err()?;

        // Collect unprocessed input blocks
        let blocks_unprocessed: Vec<_> = input_chain
            .iter_blocks()
            .take_while_ok(|(block_hash, _)| Some(block_hash) != last_processed_block.as_ref())
            .try_collect()
            .await
            .int_err()?;

        // Sanity check: First (chronologically) unprocessed block should immediately
        // follow the last processed block
        if let Some((first_unprocessed_hash, first_unprocessed_block)) = blocks_unprocessed.last() {
            if first_unprocessed_block.prev_block_hash != last_processed_block {
                panic!(
                    "Input data for {} is inconsistent - first unprocessed block {} does not \
                     imediately follows last processed block {:?}",
                    dataset_handle, first_unprocessed_hash, last_processed_block
                );
            }
        }

        let block_interval = if blocks_unprocessed.is_empty() {
            None
        } else {
            Some(BlockInterval {
                start: blocks_unprocessed.last().map(|(h, _)| h.clone()).unwrap(),
                end: blocks_unprocessed.first().map(|(h, _)| h.clone()).unwrap(),
            })
        };

        // Determine unprocessed offset range. Can be (None, None) or [start, end]
        let offset_end = blocks_unprocessed
            .iter()
            .filter_map(|(_, b)| b.as_data_stream_block())
            .filter_map(|b| b.event.output_data)
            .map(|s| s.interval.end)
            .next();
        let offset_start = blocks_unprocessed
            .iter()
            .rev()
            .filter_map(|(_, b)| b.as_data_stream_block())
            .filter_map(|b| b.event.output_data)
            .map(|s| s.interval.start)
            .next();
        let data_interval = match (offset_start, offset_end) {
            (None, None) => None,
            (Some(start), Some(end)) if start <= end => Some(OffsetInterval { start, end }),
            _ => panic!(
                "Input data for {} is inconsistent at block interval {:?} - unprocessed offset \
                 range ended up as ({:?}, {:?})",
                dataset_handle, block_interval, offset_start, offset_end
            ),
        };

        self.get_transform_input_from_slice(
            InputSlice {
                dataset_id: dataset_id.clone(),
                block_interval: block_interval.clone(),
                data_interval: data_interval.clone(),
            },
            transform_input.name.to_string(),
            Some(blocks_unprocessed),
            None,
        )
        .await
    }

    async fn get_transform_input_from_slice(
        &self,
        input_slice: InputSlice,
        alias: String,
        blocks_hint: Option<Vec<(Multihash, MetadataBlock)>>,
        vocab_hint: Option<DatasetVocabulary>,
    ) -> Result<TransformRequestInput, TransformInputError> {
        let dataset_handle = self
            .dataset_repo
            .resolve_dataset_ref(&input_slice.dataset_id.as_local_ref())
            .await
            .int_err()?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, auth::DatasetAction::Read)
            .await?;

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await
            .int_err()?;
        let input_chain = dataset.as_metadata_chain();

        // Collect unprocessed input blocks
        let blocks_unprocessed = if let Some(block_interval) = &input_slice.block_interval {
            if let Some(blocks) = blocks_hint {
                assert_eq!(blocks.last().map(|(h, _)| h), Some(&block_interval.start));
                assert_eq!(blocks.first().map(|(h, _)| h), Some(&block_interval.end));
                blocks
            } else {
                input_chain
                    .iter_blocks_interval_inclusive(
                        &block_interval.end,
                        &block_interval.start,
                        false,
                    )
                    .try_collect()
                    .await
                    .int_err()?
            }
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
            if let Some(slice) = block.event.output_data {
                data_slices.push(slice.physical_hash.clone());
            }

            if let Some(wm) = block.event.output_watermark {
                explicit_watermarks.push(Watermark {
                    system_time: block.system_time.clone(),
                    event_time: wm.clone(),
                });
            }
        }

        // TODO: Migrate to providing schema directly
        // TODO: Will not work with schema evolution
        let schema_slice = if let Some(h) = data_slices.last() {
            h.clone()
        } else {
            // TODO: This will not work with schema evolution
            input_chain
                .iter_blocks()
                .filter_data_stream_blocks()
                .filter_map_ok(|(_, b)| b.event.output_data)
                .try_first()
                .await
                .int_err()?
                .unwrap() // Already checked that none of the inputs are empty
                .physical_hash
        };

        let vocab = match vocab_hint {
            Some(v) => v,
            None => self.get_vocab(&dataset_handle.as_local_ref()).await?,
        };

        let is_empty = data_slices.is_empty() && explicit_watermarks.is_empty();

        let input = TransformRequestInput {
            dataset_handle,
            alias,
            vocab,
            block_interval: input_slice.block_interval,
            data_interval: input_slice.data_interval,
            data_slices,
            schema_slice,
            explicit_watermarks,
        };

        tracing::info!(?input, is_empty, "Computed transform input");

        Ok(input)
    }

    // TODO: Avoid iterating through output chain multiple times
    async fn get_vocab(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetVocabulary, InternalError> {
        let dataset = self.dataset_repo.get_dataset(dataset_ref).await.int_err()?;
        Ok(dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_map_ok(|(_, b)| b.event.into_variant::<SetVocab>())
            .try_first()
            .await
            .int_err()?
            .map(|sv| sv.into())
            .unwrap_or_default())
    }

    // TODO: Improve error handling
    // Need an inconsistent medata error?
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn get_verification_plan(
        &self,
        dataset_handle: &DatasetHandle,
        block_range: (Option<Multihash>, Option<Multihash>),
    ) -> Result<Vec<VerificationStep>, VerificationError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;
        let metadata_chain = dataset.as_metadata_chain();

        let head = match block_range.1 {
            None => metadata_chain.get_ref(&BlockRef::Head).await?,
            Some(hash) => hash,
        };
        let tail = block_range.0;

        let mut source = None;
        let mut vocab = None;
        let mut blocks = Vec::new();
        let mut finished_range = false;

        {
            let mut block_stream = metadata_chain.iter_blocks_interval(&head, None, false);

            // TODO: This can be simplified
            while let Some((block_hash, block)) = block_stream.try_next().await? {
                match block.event {
                    MetadataEvent::SetTransform(st) => {
                        if source.is_none() {
                            source = Some(st);
                        } else {
                            // TODO: Support dataset evolution
                            unimplemented!(
                                "Verifying datasets with evolving queries is not yet supported"
                            );
                        }
                    }
                    MetadataEvent::SetVocab(sv) => {
                        if vocab.is_none() {
                            vocab = Some(sv.into())
                        }
                    }
                    MetadataEvent::ExecuteQuery(_) => {
                        if !finished_range {
                            blocks.push((block_hash.clone(), block));
                        }
                    }
                    MetadataEvent::AddData(_) | MetadataEvent::SetPollingSource(_) => {
                        unreachable!()
                    }
                    MetadataEvent::Seed(_)
                    | MetadataEvent::SetAttachments(_)
                    | MetadataEvent::SetInfo(_)
                    | MetadataEvent::SetLicense(_)
                    | MetadataEvent::SetWatermark(_) => (),
                }

                if !finished_range && Some(&block_hash) == tail.as_ref() {
                    finished_range = true;
                }
            }
        }

        // Ensure start_block was found if specified
        if tail.is_some() && !finished_range {
            return Err(InvalidIntervalError {
                head,
                tail: tail.unwrap(),
            }
            .into());
        }

        let source = source.ok_or(
            "Expected a derivative dataset but SetTransform block was not found".int_err(),
        )?;

        let dataset_vocabs: BTreeMap<_, _> = futures::stream::iter(&source.inputs)
            .map(|input| {
                (
                    input.id.clone().unwrap(),
                    input.id.as_ref().unwrap().as_local_ref(),
                )
            })
            .then(|(input_id, input_ref)| async move {
                self.get_vocab(&input_ref)
                    .map_ok(|vocab| (input_id, vocab))
                    .await
            })
            .try_collect()
            .await?;

        let input_names: BTreeMap<_, _> = source
            .inputs
            .iter()
            .map(|i| (i.id.clone().unwrap(), i.name.clone()))
            .collect();

        let mut plan = Vec::new();

        for (block_hash, block) in blocks.into_iter().rev() {
            let block_t = block.as_typed::<ExecuteQuery>().unwrap();

            let inputs = futures::stream::iter(&block_t.event.input_slices)
                .then(|slice| {
                    let name = input_names.get(&slice.dataset_id).unwrap();

                    let vocab = dataset_vocabs
                        .get(&slice.dataset_id)
                        .map(|v| v.clone())
                        .unwrap();

                    self.get_transform_input_from_slice(
                        slice.clone(),
                        name.to_string(),
                        None,
                        Some(vocab),
                    )
                })
                .try_collect()
                .await
                .map_err(|e| match e {
                    TransformInputError::Access(e) => VerificationError::Access(e),
                    TransformInputError::Internal(e) => VerificationError::Internal(e),
                })?;

            let step = VerificationStep {
                request: TransformRequest {
                    operation_id: self.next_operation_id(),
                    dataset_handle: dataset_handle.clone(),
                    transform: source.transform.clone(),
                    system_time: block.system_time,
                    next_offset: block_t
                        .event
                        .output_data
                        .as_ref()
                        .map(|s| s.interval.start)
                        .unwrap_or(0), /* TODO: Assuming offset does not matter if block is
                                        * not supposed to produce data */
                    inputs,
                    vocab: vocab.clone().unwrap_or_default(),
                    prev_checkpoint: block_t.event.input_checkpoint.clone(),
                },
                expected_block: block,
                expected_hash: block_hash,
            };

            plan.push(step);
        }

        Ok(plan)
    }

    #[tracing::instrument(level = "info", name = "transform", skip_all, fields(%dataset_ref))]
    async fn transform_impl(
        &self,
        dataset_ref: DatasetRef,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullTransformListener));
        let dataset_handle = self.dataset_repo.resolve_dataset_ref(&dataset_ref).await?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, auth::DatasetAction::Write)
            .await?;

        // TODO: There might be more operations to do
        // TODO: Inject time source
        let next_operation = self
            .get_next_operation(&dataset_handle, Utc::now())
            .await
            .map_err(|e| match e {
                GetNextOperationError::Access(e) => TransformError::Access(e),
                GetNextOperationError::Internal(e) => TransformError::Internal(e),
            })?;
        if let Some(operation) = next_operation {
            let dataset = self
                .dataset_repo
                .get_dataset(&dataset_handle.as_local_ref())
                .await?;
            let meta_chain = dataset.as_metadata_chain();

            let system_time = operation.system_time.clone();
            let head = meta_chain.get_ref(&BlockRef::Head).await.int_err()?;

            Self::do_transform(
                self.engine_provisioner.clone(),
                operation,
                move |execute_query, new_data, new_checkpoint| async move {
                    let commit_result = dataset
                        .commit_execute_query(
                            execute_query,
                            new_data,
                            new_checkpoint,
                            CommitOpts {
                                block_ref: &BlockRef::Head,
                                system_time: Some(system_time),
                                prev_block_hash: Some(Some(&head)),
                                check_object_refs: true,
                            },
                        )
                        .await?;

                    Ok(TransformResult::Updated {
                        old_head: commit_result.old_head.unwrap(),
                        new_head: commit_result.new_head,
                        num_blocks: 1,
                    })
                },
                listener,
            )
            .await
        } else {
            listener.begin();
            listener.success(&TransformResult::UpToDate);
            Ok(TransformResult::UpToDate)
        }
    }
}

#[async_trait::async_trait]
impl TransformService for TransformServiceImpl {
    async fn transform(
        &self,
        dataset_ref: &DatasetRef,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError> {
        tracing::info!(?dataset_ref, "Transforming a single dataset");

        self.transform_impl(dataset_ref.clone(), maybe_listener)
            .await
    }

    async fn transform_multi(
        &self,
        dataset_refs: Vec<DatasetRef>,
        maybe_multi_listener: Option<Arc<dyn TransformMultiListener>>,
    ) -> Vec<(DatasetRef, Result<TransformResult, TransformError>)> {
        let multi_listener =
            maybe_multi_listener.unwrap_or_else(|| Arc::new(NullTransformMultiListener));

        tracing::info!(?dataset_refs, "Transforming multiple datasets");

        let mut futures = Vec::new();

        for dataset_ref in &dataset_refs {
            let f = match self.dataset_repo.resolve_dataset_ref(dataset_ref).await {
                Ok(hdl) => {
                    let maybe_listener = multi_listener.begin_transform(&hdl);
                    self.transform_impl(hdl.into(), maybe_listener)
                }
                // Relying on this call to fail to avoid boxing the futures
                Err(_) => self.transform_impl(dataset_ref.clone(), None),
            };
            futures.push(f);
        }

        let results = futures::future::join_all(futures).await;
        dataset_refs.into_iter().zip(results).collect()
    }

    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref, ?block_range))]
    async fn verify_transform(
        &self,
        dataset_ref: &DatasetRef,
        block_range: (Option<Multihash>, Option<Multihash>),
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        let listener = maybe_listener.unwrap_or(Arc::new(NullVerificationListener {}));

        let dataset_handle = self.dataset_repo.resolve_dataset_ref(dataset_ref).await?;

        // Note: output dataset read permissions are already checked in
        // VerificationService. But permissions for input datasets have to be
        // checked here

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let verification_plan = self
            .get_verification_plan(&dataset_handle, block_range)
            .await?;
        let num_steps = verification_plan.len();
        listener.begin_phase(VerificationPhase::ReplayTransform);

        for (step_index, step) in verification_plan.into_iter().enumerate() {
            let request = step.request;
            let block_hash = step.expected_hash;
            let expected_block = step.expected_block;
            let expected_event = expected_block.event.into_variant::<ExecuteQuery>().unwrap();

            // Will be set during "commit" step
            let mut actual_event: Option<ExecuteQuery> = None;

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

            let ds = dataset.clone();
            let out_event = &mut actual_event;
            let result = TransformResult::Updated {
                old_head: expected_block.prev_block_hash.clone().unwrap(),
                new_head: block_hash.clone(),
                num_blocks: 1,
            };

            Self::do_transform(
                self.engine_provisioner.clone(),
                request,
                |execute_query, data, checkpoint| async move {
                    // We commit and expect outputs to be cleaned up automatically on drop
                    let new_event = ds
                        .prepare_execute_query(execute_query, data.as_ref(), checkpoint.as_ref())
                        .await?;

                    *out_event = Some(new_event);

                    // This result is ignored
                    Ok(result)
                },
                transform_listener,
            )
            .await?;

            let actual_event = actual_event.unwrap();

            tracing::debug!(%block_hash, ?expected_event, ?actual_event, "Comparing expected and replayed events");

            let mut cmp_actual_event = actual_event.clone();

            // Parquet format is non-reproducible, so we rely only on logical hash for
            // equivalence test and overwrite the physical hash and size with
            // the expected values for comparison
            if let Some(actual_slice) = &mut cmp_actual_event.output_data {
                if let Some(expected_slice) = &expected_event.output_data {
                    actual_slice.physical_hash = expected_slice.physical_hash.clone();
                    actual_slice.size = expected_slice.size;
                }
            }

            // Currently we're considering checkpoints non-reproducible and thus exclude
            // them from equivalence test
            cmp_actual_event.output_checkpoint = expected_event.output_checkpoint.clone();

            if expected_event != cmp_actual_event {
                tracing::warn!(%block_hash, ?expected_event, ?actual_event, "Data is not reproducible");

                let err = VerificationError::DataNotReproducible(DataNotReproducible {
                    block_hash,
                    expected_event: expected_event.into(),
                    actual_event: actual_event.into(),
                });
                listener.error(&err);
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
        Ok(VerificationResult::Valid)
    }

    async fn verify_transform_multi(
        &self,
        _datasets: Vec<VerificationRequest>,
        _listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        unimplemented!()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VerificationStep {
    pub request: TransformRequest,
    pub expected_block: MetadataBlock,
    pub expected_hash: Multihash,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetNextOperationError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
enum TransformInputError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<auth::DatasetActionUnauthorizedError> for TransformInputError {
    fn from(v: auth::DatasetActionUnauthorizedError) -> Self {
        match v {
            auth::DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            auth::DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
