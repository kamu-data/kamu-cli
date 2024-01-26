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
use itertools::Itertools;
use kamu_core::engine::*;
use kamu_core::*;
use kamu_ingest_datafusion::DataWriterDataFusion;
use opendatafabric::*;

pub struct TransformServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    engine_provisioner: Arc<dyn EngineProvisioner>,
    time_source: Arc<dyn SystemTimeSource>,
}

#[component(pub)]
#[interface(dyn TransformService)]
impl TransformServiceImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_action_authorizer,
            engine_provisioner,
            time_source,
        }
    }

    // Note: Can be called from multiple threads
    #[tracing::instrument(level = "info", skip_all, fields(operation_id = %request.operation_id))]
    async fn do_transform<CommitFn, Fut>(
        engine_provisioner: Arc<dyn EngineProvisioner>,
        request: TransformRequestExt,
        commit_fn: CommitFn,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, TransformError>
    where
        CommitFn: FnOnce(TransformRequestExt, TransformResponseExt) -> Fut,
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
        request: TransformRequestExt,
        commit_fn: CommitFn,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, TransformError>
    where
        CommitFn: FnOnce(TransformRequestExt, TransformResponseExt) -> Fut,
        Fut: futures::Future<Output = Result<TransformResult, TransformError>>,
    {
        let engine = engine_provisioner
            .provision_engine(
                match request.transform {
                    Transform::Sql(ref sql) => &sql.engine,
                },
                listener.clone().get_engine_provisioning_listener(),
            )
            .await?;

        let response = engine.execute_transform(request.clone()).await?;
        assert_eq!(
            response.new_offset_interval.is_some(),
            response.new_data.is_some()
        );

        commit_fn(request, response).await
    }

    async fn commit_execute_transform(
        dataset_repo: Arc<dyn DatasetRepository>,
        mut request: TransformRequestExt,
        response: TransformResponseExt,
    ) -> Result<TransformResult, TransformError> {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let old_head = request.head.clone();

        let dataset = dataset_repo
            .get_dataset(&request.dataset_handle.as_local_ref())
            .await
            .int_err()?;

        // Read new schema
        let new_schema = if let Some(out_data) = &response.new_data {
            let file = std::fs::File::open(out_data.as_path()).int_err()?;
            let schema = ParquetRecordBatchReaderBuilder::try_new(file)
                .int_err()?
                .schema()
                .clone();
            Some(schema)
        } else {
            None
        };

        if let Some(prev_schema) = request.schema {
            // Validate schema
            if let Some(new_schema) = new_schema {
                DataWriterDataFusion::validate_output_schema_equivalence(&prev_schema, &new_schema)
                    .int_err()?;
            }
        } else {
            // Set schema upon first transform
            if let Some(new_schema) = new_schema {
                // TODO: make schema commit atomic with data
                let commit_schema_result = dataset
                    .commit_event(
                        SetDataSchema::new(&new_schema).into(),
                        CommitOpts {
                            block_ref: &request.block_ref,
                            system_time: Some(request.system_time),
                            prev_block_hash: Some(Some(&request.head)),
                            check_object_refs: false,
                        },
                    )
                    .await?;

                // Advance head
                request.head = commit_schema_result.new_head;
            }
        }

        let params = ExecuteTransformParams {
            query_inputs: request.inputs.iter().map(|i| i.clone().into()).collect(),
            prev_checkpoint: request.prev_checkpoint,
            prev_offset: request.prev_offset,
            new_offset_interval: response.new_offset_interval,
            new_watermark: response.new_watermark,
        };

        let commit_result = dataset
            .commit_execute_transform(
                params,
                response.new_data,
                response.new_checkpoint,
                CommitOpts {
                    block_ref: &request.block_ref,
                    system_time: Some(request.system_time),
                    prev_block_hash: Some(Some(&request.head)),
                    check_object_refs: true,
                },
            )
            .await?;

        Ok(TransformResult::Updated {
            old_head,
            new_head: commit_result.new_head,
            num_blocks: 1,
        })
    }

    // TODO: PERF: Avoid multiple passes over metadata chain
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn get_next_operation(
        &self,
        dataset_handle: &DatasetHandle,
        system_time: DateTime<Utc>,
    ) -> Result<Option<TransformRequestExt>, TransformError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await
            .int_err()?;

        let output_chain = dataset.as_metadata_chain();

        // TODO: externalize
        let block_ref = BlockRef::Head;
        let head = output_chain.get_ref(&block_ref).await.int_err()?;

        let mut source = None;
        let mut schema = None;
        let mut set_vocab = None;
        let mut prev_query = None;

        // TODO: PERF: Search for source, vocab, and data schema result in full scan
        {
            let mut block_stream = output_chain.iter_blocks_interval(&head, None, false);
            while let Some((_, block)) = block_stream.try_next().await.int_err()? {
                match block.event {
                    MetadataEvent::SetVocab(e) => {
                        if set_vocab.is_none() {
                            set_vocab = Some(e);
                        }
                    }
                    MetadataEvent::SetTransform(e) => {
                        if source.is_none() {
                            source = Some(e);
                        } else {
                            unimplemented!("Transform evolution is not yet supported");
                        }
                    }
                    MetadataEvent::SetDataSchema(e) => {
                        if schema.is_none() {
                            schema = Some(e.schema_as_arrow().int_err()?);
                        }
                    }
                    MetadataEvent::ExecuteTransform(e) => {
                        if prev_query.is_none() {
                            prev_query = Some(e);
                        }
                    }
                    MetadataEvent::Seed(_)
                    | MetadataEvent::SetAttachments(_)
                    | MetadataEvent::SetInfo(_)
                    | MetadataEvent::SetLicense(_) => {}
                    MetadataEvent::AddData(_)
                    | MetadataEvent::SetPollingSource(_)
                    | MetadataEvent::AddPushSource(_)
                    | MetadataEvent::DisablePushSource(_)
                    | MetadataEvent::DisablePollingSource(_) => {
                        unreachable!()
                    }
                }

                if source.is_some()
                    && schema.is_some()
                    && set_vocab.is_some()
                    && prev_query.is_some()
                {
                    break;
                }
            }
        }

        let Some(source) = source else {
            return Err(TransformNotDefinedError {}.into());
        };
        tracing::debug!(?source, "Transforming using source");

        // Check if all inputs are non-empty
        if futures::stream::iter(&source.inputs)
            .map(|input| input.dataset_ref.id().unwrap().as_local_ref())
            .then(|input_ref| async move { self.is_never_pulled(&input_ref).await })
            .any_ok(|never_pulled| *never_pulled)
            .await?
        {
            tracing::info!("Not processing because one of the inputs was never pulled");
            return Ok(None);
        }

        // Prepare inputs
        let input_states: Vec<(&TransformInput, Option<&ExecuteTransformInput>)> =
            if let Some(query) = &prev_query {
                source
                    .inputs
                    .iter()
                    .zip_eq(query.query_inputs.iter().map(Some))
                    .collect()
            } else {
                source.inputs.iter().map(|i| (i, None)).collect()
            };

        let inputs: Vec<_> = futures::stream::iter(input_states)
            .then(|(input_decl, input_state)| self.get_transform_input(input_decl, input_state))
            .try_collect()
            .await?;

        // Nothing to do?
        if inputs
            .iter()
            .all(|i| i.data_slices.is_empty() && i.explicit_watermarks.is_empty())
        {
            return Ok(None);
        }

        Ok(Some(TransformRequestExt {
            operation_id: self.next_operation_id(),
            dataset_handle: dataset_handle.clone(),
            block_ref,
            head,
            transform: source.transform,
            system_time,
            schema,
            prev_offset: prev_query.as_ref().and_then(ExecuteTransform::last_offset),
            vocab: set_vocab.unwrap_or_default().into(),
            inputs,
            prev_checkpoint: prev_query.and_then(|q| q.new_checkpoint.map(|c| c.physical_hash)),
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

    // TODO: Allow derivative datasets to function with inputs containing no data
    // This will require passing the schema explicitly instead of relying on a file
    async fn is_never_pulled(&self, dataset_ref: &DatasetRef) -> Result<bool, InternalError> {
        let dataset = self.dataset_repo.get_dataset(dataset_ref).await.int_err()?;
        let last_data_block = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_data_stream_blocks()
            .try_next()
            .await
            .int_err()?;
        Ok(last_data_block
            .and_then(|(_, b)| b.event.last_offset())
            .is_none())
    }

    async fn get_transform_input(
        &self,
        input_decl: &TransformInput,
        input_state: Option<&ExecuteTransformInput>,
    ) -> Result<TransformRequestInputExt, TransformError> {
        let dataset_id = input_decl.dataset_ref.id().unwrap();
        if let Some(input_state) = input_state {
            assert_eq!(*dataset_id, input_state.dataset_id);
        }

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

        // Determine last processed input block and offset
        let last_processed_block = input_state.and_then(|i| i.last_block_hash());
        let last_processed_offset = input_state.and_then(ExecuteTransformInput::last_offset);

        // Determine unprocessed block and offset range
        let last_unprocessed_block = input_chain.get_ref(&BlockRef::Head).await.int_err()?;
        let last_unprocessed_offset = input_chain
            .iter_blocks_interval(&last_unprocessed_block, last_processed_block, false)
            .filter_map_ok(|(_, b)| b.into_data_stream_block())
            .try_next()
            .await
            .int_err()?
            .and_then(|b| b.event.last_offset())
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

        self.get_transform_input_from_query_input(
            query_input,
            input_decl.alias.clone().unwrap(),
            None,
        )
        .await
    }

    async fn get_transform_input_from_query_input(
        &self,
        query_input: ExecuteTransformInput,
        alias: String,
        vocab_hint: Option<DatasetVocabulary>,
    ) -> Result<TransformRequestInputExt, TransformError> {
        let dataset_handle = self
            .dataset_repo
            .resolve_dataset_ref(&query_input.dataset_id.as_local_ref())
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
        let blocks_unprocessed = if let Some(new_block_hash) = &query_input.new_block_hash {
            input_chain
                .iter_blocks_interval(new_block_hash, query_input.prev_block_hash.as_ref(), false)
                .try_collect()
                .await
                .int_err()?
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

        // TODO: Migrate to providing schema directly
        // TODO: Will not work with schema evolution
        let schema_slice = if let Some(h) = data_slices.last() {
            h.clone()
        } else {
            // TODO: This will not work with schema evolution
            input_chain
                .iter_blocks()
                .filter_data_stream_blocks()
                .filter_map_ok(|(_, b)| b.event.new_data)
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

        let input = TransformRequestInputExt {
            dataset_handle,
            alias,
            vocab,
            prev_block_hash: query_input.prev_block_hash,
            new_block_hash: query_input.new_block_hash,
            prev_offset: query_input.prev_offset,
            new_offset: query_input.new_offset,
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
            .unwrap_or_default()
            .into())
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
        let mut set_vocab = None;
        let mut schema = None;
        let mut blocks = Vec::new();
        let mut finished_range = false;

        {
            let mut block_stream = metadata_chain.iter_blocks_interval(&head, None, false);

            // TODO: PERF: Search for source, vocab, and data schema result in full scan
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
                        if set_vocab.is_none() {
                            set_vocab = Some(sv);
                        }
                    }
                    MetadataEvent::SetDataSchema(e) => {
                        if schema.is_none() {
                            schema = Some(e.schema_as_arrow().int_err()?);
                        }
                    }
                    MetadataEvent::ExecuteTransform(_) => {
                        if !finished_range {
                            blocks.push((block_hash.clone(), block));
                        }
                    }
                    MetadataEvent::AddData(_)
                    | MetadataEvent::SetPollingSource(_)
                    | MetadataEvent::DisablePollingSource(_)
                    | MetadataEvent::AddPushSource(_)
                    | MetadataEvent::DisablePushSource(_) => {
                        unreachable!()
                    }
                    MetadataEvent::Seed(_)
                    | MetadataEvent::SetAttachments(_)
                    | MetadataEvent::SetInfo(_)
                    | MetadataEvent::SetLicense(_) => (),
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

        // TODO: Replace maps with access by index, as ODF guarantees same order of
        // inputs in ExecuteTransform as in SetTransform
        let dataset_vocabs: BTreeMap<_, _> = futures::stream::iter(&source.inputs)
            .map(|input| {
                (
                    input.dataset_ref.id().cloned().unwrap(),
                    input.dataset_ref.id().unwrap().as_local_ref(),
                )
            })
            .then(|(input_id, input_ref)| async move {
                self.get_vocab(&input_ref)
                    .map_ok(|vocab| (input_id, vocab))
                    .await
            })
            .try_collect()
            .await?;

        let input_aliases: BTreeMap<_, _> = source
            .inputs
            .iter()
            .map(|i| {
                (
                    i.dataset_ref.id().cloned().unwrap(),
                    i.alias.clone().unwrap(),
                )
            })
            .collect();

        let mut plan = Vec::new();

        for (block_hash, block) in blocks.into_iter().rev() {
            let block_t = block.as_typed::<ExecuteTransform>().unwrap();

            let inputs = futures::stream::iter(&block_t.event.query_inputs)
                .then(|slice| {
                    let alias = input_aliases.get(&slice.dataset_id).unwrap();

                    let vocab = dataset_vocabs.get(&slice.dataset_id).cloned().unwrap();

                    self.get_transform_input_from_query_input(
                        slice.clone(),
                        alias.clone(),
                        Some(vocab),
                    )
                })
                .try_collect()
                .await
                .map_err(|e| match e {
                    TransformError::Access(e) => VerificationError::Access(e),
                    TransformError::Internal(e) => VerificationError::Internal(e),
                    _ => VerificationError::Internal(e.int_err()),
                })?;

            let step = VerificationStep {
                request: TransformRequestExt {
                    operation_id: self.next_operation_id(),
                    dataset_handle: dataset_handle.clone(),
                    block_ref: BlockRef::Head,
                    head: block_t.prev_block_hash.unwrap().clone(),
                    transform: source.transform.clone(),
                    system_time: block.system_time,
                    schema: schema.clone(),
                    prev_offset: block_t.event.prev_offset,
                    inputs,
                    vocab: set_vocab.clone().unwrap_or_default().into(),
                    prev_checkpoint: block_t.event.prev_checkpoint.clone(),
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
            .get_next_operation(&dataset_handle, self.time_source.now())
            .await?;

        if let Some(operation) = next_operation {
            let dataset_repo = self.dataset_repo.clone();
            Self::do_transform(
                self.engine_provisioner.clone(),
                operation,
                |request, response| async move {
                    Self::commit_execute_transform(dataset_repo, request, response).await
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
    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref))]
    async fn get_active_transform(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<(Multihash, MetadataBlockTyped<SetTransform>)>, GetDatasetError> {
        let dataset = self.dataset_repo.get_dataset(dataset_ref).await?;

        // TODO: Support transform evolution
        let source = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_map_ok(|(h, b)| b.into_typed::<SetTransform>().map(|b| (h, b)))
            .try_first()
            .await
            .int_err()?;

        Ok(source)
    }

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
                            response.new_checkpoint.as_ref(),
                        )
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
            if let Some(actual_slice) = &mut cmp_actual_event.new_data {
                if let Some(expected_slice) = &expected_event.new_data {
                    actual_slice.physical_hash = expected_slice.physical_hash.clone();
                    actual_slice.size = expected_slice.size;
                }
            }

            // Currently we're considering checkpoints non-reproducible and thus exclude
            // them from equivalence test
            cmp_actual_event.new_checkpoint = expected_event.new_checkpoint.clone();

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
    pub request: TransformRequestExt,
    pub expected_block: MetadataBlock,
    pub expected_hash: Multihash,
}

/////////////////////////////////////////////////////////////////////////////////////////
