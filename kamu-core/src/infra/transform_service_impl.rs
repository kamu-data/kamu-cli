// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use crate::infra::*;
use chrono::DateTime;
use chrono::Utc;
use opendatafabric::*;

use dill::*;
use opendatafabric::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::info_span;

pub struct TransformServiceImpl {
    metadata_repo: Arc<dyn MetadataRepository>,
    engine_provisioner: Arc<dyn EngineProvisioner>,
    volume_layout: VolumeLayout,
}

#[component(pub)]
impl TransformServiceImpl {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
        volume_layout: &VolumeLayout,
    ) -> Self {
        Self {
            metadata_repo,
            engine_provisioner,
            volume_layout: volume_layout.clone(),
        }
    }

    // Note: Can be called from multiple threads
    fn do_transform(
        engine_provisioner: Arc<dyn EngineProvisioner>,
        operation: TransformOperation,
        commit_fn: impl FnOnce(MetadataBlock, &Path, &Path) -> Result<TransformResult, TransformError>,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, TransformError> {
        let span = info_span!(
            "Performing transform",
            output_dataset = operation.request.dataset_id.as_str()
        );
        let _span_guard = span.enter();
        info!(operation = ?operation, "Transform request");

        listener.begin();

        match Self::do_transform_inner(engine_provisioner, operation, commit_fn, listener.clone()) {
            Ok(res) => {
                info!("Transform successful");
                listener.success(&res);
                Ok(res)
            }
            Err(err) => {
                error!(error = ?err, "Transform failed");
                listener.error(&err);
                Err(err)
            }
        }
    }

    // Note: Can be called from multiple threads
    fn do_transform_inner(
        engine_provisioner: Arc<dyn EngineProvisioner>,
        operation: TransformOperation,
        commit_fn: impl FnOnce(MetadataBlock, &Path, &Path) -> Result<TransformResult, TransformError>,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, TransformError> {
        let new_checkpoint_path = PathBuf::from(&operation.request.new_checkpoint_dir);
        let out_data_path = PathBuf::from(&operation.request.out_data_path);
        let offset = operation.request.offset;

        let engine = engine_provisioner.provision_engine(
            match operation.request.transform {
                Transform::Sql(ref sql) => &sql.engine,
            },
            listener.clone().get_engine_provisioning_listener(),
        )?;

        let metadata_block = engine.transform(operation.request)?.metadata_block;

        let mut metadata_block = if metadata_block.input_slices.is_some() {
            return Err(EngineError::contract_error(
                "Engine wrote input slices into metadata block",
                Vec::new(),
            )
            .into());
        } else {
            // TODO: This will go away once we move most block forming logic to coordinator
            MetadataBlock {
                input_slices: Some(operation.input_slices),
                ..metadata_block
            }
        };

        if let Some(slice) = &mut metadata_block.output_slice {
            // TODO: Move out this to validation
            if slice.data_interval.end < slice.data_interval.start
                || slice.data_interval.start != offset
            {
                return Err(EngineError::contract_error(
                    "Engine returned an output slice with invalid offset range",
                    Vec::new(),
                )
                .into());
            }
            if !out_data_path.exists() {
                return Err(EngineError::contract_error(
                    "Engine did not write a response data file",
                    Vec::new(),
                )
                .into());
            }

            // TODO: Make engine not return hashes to begin with
            // TODO: Move out into data commit procedure of sorts
            slice.data_logical_hash =
                crate::infra::utils::data_utils::get_parquet_logical_hash(&out_data_path)
                    .map_err(|e| TransformError::internal(e))?;
        } else if out_data_path.exists() {
            return Err(EngineError::contract_error(
                "Engine wrote data file while the ouput slice is empty",
                Vec::new(),
            )
            .into());
        }

        let result = commit_fn(metadata_block, &out_data_path, &new_checkpoint_path)?;

        // Commit should clean up
        assert!(!out_data_path.exists());
        assert!(!new_checkpoint_path.exists());

        Ok(result)
    }

    fn commit_transform(
        mut meta_chain: Box<dyn MetadataChain>,
        dataset_id: DatasetIDBuf,
        dataset_layout: DatasetLayout,
        prev_block_hash: Sha3_256,
        new_block: MetadataBlock,
        new_data_path: &Path,
        new_checkpoint_path: &Path,
    ) -> Result<TransformResult, TransformError> {
        let new_block = MetadataBlock {
            prev_block_hash: Some(prev_block_hash),
            ..new_block
        };

        let has_data = new_block.output_slice.is_some();
        let new_block_hash = meta_chain.append(new_block);

        // TODO: Data should be moved before writing block file
        if has_data {
            std::fs::rename(
                &new_data_path,
                dataset_layout.data_dir.join(new_block_hash.to_string()),
            )
            .map_err(|e| TransformError::internal(e))?;
        }

        // TODO: Checkpoint should be moved before writing block file
        std::fs::rename(
            &new_checkpoint_path,
            dataset_layout
                .checkpoints_dir
                .join(new_block_hash.to_string()),
        )
        .map_err(|e| TransformError::internal(e))?;

        info!(output_dataset = dataset_id.as_str(), new_head = ?new_block_hash, "Committed new block");

        Ok(TransformResult::Updated {
            old_head: prev_block_hash,
            new_head: new_block_hash,
            num_blocks: 1,
        })
    }

    // TODO: PERF: Avoid multiple passes over metadata chain
    pub fn get_next_operation(
        &self,
        dataset_id: &DatasetID,
        system_time: DateTime<Utc>,
    ) -> Result<Option<TransformOperation>, DomainError> {
        let span = info_span!(
            "Evaluating next transform operation",
            output_dataset = dataset_id.as_str()
        );
        let _span_guard = span.enter();

        let output_chain = self.metadata_repo.get_metadata_chain(dataset_id)?;

        // TODO: limit traversal depth
        let mut sources: Vec<_> = output_chain
            .iter_blocks()
            .filter_map(|b| match b.source {
                Some(DatasetSource::Derivative(t)) => Some(t),
                Some(DatasetSource::Root(_)) => {
                    panic!("Transform called on non-derivative dataset {}", dataset_id)
                }
                None => None,
            })
            .collect();

        // TODO: source could've changed several times
        if sources.len() > 1 {
            unimplemented!("Transform evolution is not yet supported");
        }

        let source = sources.pop().unwrap();

        if source
            .inputs
            .iter()
            .any(|id| self.is_never_pulled(id).unwrap())
        {
            info!("Not processing because one of the inputs was never pulled");
            return Ok(None);
        }

        // Prepare inputs
        let input_slices: Vec<_> = source
            .inputs
            .iter()
            .map(|input_id| self.get_input_slice(input_id, output_chain.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;

        let query_inputs: Vec<_> = input_slices
            .iter()
            .map(|i| self.to_query_input(i, None))
            .collect::<Result<Vec<_>, _>>()?;

        // Nothing to do?
        if query_inputs
            .iter()
            .all(|i| i.data_paths.is_empty() && i.explicit_watermarks.is_empty())
        {
            return Ok(None);
        }

        // TODO: Verify assumption that `input_slices` is a reliable indicator of a checkpoint presence
        let prev_checkpoint = output_chain
            .iter_blocks()
            .filter(|b| b.input_slices.is_some())
            .map(|b| b.block_hash)
            .next();

        let data_offset_end = output_chain
            .iter_blocks()
            .filter_map(|b| b.output_slice)
            .map(|s| s.data_interval.end)
            .next();

        let output_layout = DatasetLayout::new(&self.volume_layout, dataset_id);
        let out_data_path = output_layout.data_dir.join(".pending");
        let new_checkpoint_dir = output_layout.checkpoints_dir.join(".pending");

        // Clean up previous state leftovers
        if out_data_path.exists() {
            std::fs::remove_file(&out_data_path).map_err(|e| DomainError::InfraError(e.into()))?;
        }
        if new_checkpoint_dir.exists() {
            std::fs::remove_dir_all(&new_checkpoint_dir)
                .map_err(|e| DomainError::InfraError(e.into()))?;
        }
        std::fs::create_dir_all(&new_checkpoint_dir)
            .map_err(|e| DomainError::InfraError(e.into()))?;

        Ok(Some(TransformOperation {
            input_slices,
            request: ExecuteQueryRequest {
                dataset_id: dataset_id.to_owned(),
                system_time,
                offset: data_offset_end.map(|e| e + 1).unwrap_or(0),
                vocab: self.get_vocab(dataset_id)?,
                transform: source.transform,
                inputs: query_inputs,
                prev_checkpoint_dir: prev_checkpoint
                    .map(|hash| output_layout.checkpoints_dir.join(hash.to_string())),
                new_checkpoint_dir,
                out_data_path,
            },
        }))
    }

    fn is_never_pulled(&self, dataset_id: &DatasetID) -> Result<bool, DomainError> {
        let chain = self.metadata_repo.get_metadata_chain(dataset_id)?;
        Ok(chain
            .iter_blocks()
            .filter_map(|b| b.output_slice)
            .next()
            .is_none())
    }

    // TODO: Avoid iterating through output chain multiple times
    fn get_input_slice(
        &self,
        dataset_id: &DatasetID,
        output_chain: &dyn MetadataChain,
    ) -> Result<InputSlice, DomainError> {
        let input_chain = self.metadata_repo.get_metadata_chain(dataset_id)?;

        // Determine last processed input block
        let last_processed_block = output_chain
            .iter_blocks()
            .filter_map(|b| b.input_slices)
            .flatten()
            .filter(|slice| slice.dataset_id == *dataset_id)
            .filter_map(|slice| slice.block_interval)
            .map(|bi| bi.end)
            .next();

        // Collect unprocessed input blocks
        let blocks_unprocessed: Vec<_> = input_chain
            .iter_blocks()
            .take_while(|b| Some(b.block_hash) != last_processed_block)
            .collect();

        // Sanity check: First (chronologically) unprocessed block should immediately follow the last processed block
        if let Some(first_unprocessed) = blocks_unprocessed.last() {
            if first_unprocessed.prev_block_hash != last_processed_block {
                panic!(
                    "Input data for {} is inconsistent - first unprocessed block {} does not imediately follows last processed block {:?}",
                    dataset_id, first_unprocessed.block_hash, last_processed_block
                );
            }
        }

        let block_interval = if blocks_unprocessed.is_empty() {
            None
        } else {
            Some(BlockInterval {
                start: blocks_unprocessed.last().map(|b| b.block_hash).unwrap(),
                end: blocks_unprocessed.first().map(|b| b.block_hash).unwrap(),
            })
        };

        // Determine unprocessed offset range. Can be (None, None) or [start, end]
        let offset_end = blocks_unprocessed
            .iter()
            .filter_map(|b| b.output_slice.as_ref())
            .map(|s| s.data_interval.end)
            .next();
        let offset_start = blocks_unprocessed
            .iter()
            .rev()
            .filter_map(|b| b.output_slice.as_ref())
            .map(|s| s.data_interval.start)
            .next();
        let data_interval = match (offset_start, offset_end) {
            (None, None) => None,
            (Some(start), Some(end)) if start <= end => Some(OffsetInterval { start, end }),
            _ => panic!(
                "Input data for {} is inconsistent at block interval {:?} - unprocessed offset range ended up as ({:?}, {:?})",
                dataset_id, block_interval, offset_start, offset_end
            ),
        };

        Ok(InputSlice {
            dataset_id: dataset_id.to_owned(),
            block_interval,
            data_interval,
        })
    }

    // TODO: Avoid traversing same blocks again
    fn to_query_input(
        &self,
        slice: &InputSlice,
        vocab_hint: Option<DatasetVocabulary>,
    ) -> Result<QueryInput, DomainError> {
        let input_chain = self.metadata_repo.get_metadata_chain(&slice.dataset_id)?;
        let input_layout = DatasetLayout::new(&self.volume_layout, &slice.dataset_id);

        // List of part files and watermarks that will be used by the engine
        // Note: Engine will still filter the records by the offset interval
        let mut data_paths = Vec::new();
        let mut explicit_watermarks = Vec::new();

        if let Some(block_interval) = &slice.block_interval {
            let hash_to_stop_at = input_chain
                .get_block(&block_interval.start)
                .expect("Starting block of the interval not found")
                .prev_block_hash;

            for block in input_chain
                .iter_blocks_starting(&block_interval.end)
                .unwrap()
                .take_while(|b| Some(b.block_hash) != hash_to_stop_at)
            {
                if block.output_slice.is_some() {
                    data_paths.push(input_layout.data_dir.join(block.block_hash.to_string()));
                }

                if let Some(wm) = block.output_watermark {
                    explicit_watermarks.push(Watermark {
                        system_time: block.system_time,
                        event_time: wm,
                    });
                }
            }

            // Note: Order is important, so we reverse it to make chronological
            data_paths.reverse();
            explicit_watermarks.reverse();
        }

        // TODO: Migrate to providing schema directly
        // TODO: Will not work with schema evolution
        let schema_file = data_paths
            .last()
            .map(|p| p.clone())
            .unwrap_or_else(|| self.get_schema_file_fallback(&input_layout));

        let vocab = match vocab_hint {
            Some(v) => v,
            None => self.get_vocab(&slice.dataset_id)?,
        };

        let is_empty = data_paths.is_empty() && explicit_watermarks.is_empty();

        let input = QueryInput {
            dataset_id: slice.dataset_id.clone(),
            vocab,
            data_interval: slice.data_interval.clone(),
            data_paths,
            schema_file,
            explicit_watermarks,
        };

        info!(
            input_dataset = slice.dataset_id.as_str(),
            input = ?input,
            slice = ?slice,
            empty = is_empty,
            "Computed query input"
        );

        Ok(input)
    }

    // TODO: Avoid iterating through output chain multiple times
    fn get_vocab(&self, dataset_id: &DatasetID) -> Result<DatasetVocabulary, DomainError> {
        let chain = self.metadata_repo.get_metadata_chain(dataset_id)?;
        let vocab = chain.iter_blocks().filter_map(|b| b.vocab).next();
        Ok(vocab.unwrap_or_default())
    }

    // TODO: Migrate to providing schema directly
    fn get_schema_file_fallback(&self, dataset_layout: &DatasetLayout) -> PathBuf {
        std::fs::read_dir(&dataset_layout.data_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path()
    }

    // TODO: Improve error handling
    // Need an inconsistent medata error?
    pub fn get_verification_plan(
        &self,
        dataset_id: &DatasetID,
        block_range: (Option<Sha3_256>, Option<Sha3_256>),
    ) -> Result<Vec<VerificationStep>, VerificationError> {
        fn as_deriv_source(block: &MetadataBlock) -> Option<&DatasetSourceDerivative> {
            match &block.source {
                Some(DatasetSource::Derivative(s)) => Some(s),
                _ => None,
            }
        }

        let span = info_span!(
            "Preparing transformations replay plan",
            output_dataset = dataset_id.as_str()
        );
        let _span_guard = span.enter();

        let metadata_chain = self.metadata_repo.get_metadata_chain(dataset_id)?;

        let start_block = block_range.0;
        let end_block = block_range
            .1
            .unwrap_or_else(|| metadata_chain.read_ref(&BlockRef::Head).unwrap());

        let mut source = None;
        let mut prev_checkpoint = None;
        let mut vocab = None;
        let mut blocks = Vec::new();
        let mut finished_range = false;

        for block in metadata_chain
            .iter_blocks_starting(&end_block)
            .ok_or(VerificationError::NoSuchBlock(end_block))?
        {
            if let Some(src) = as_deriv_source(&block) {
                if block.prev_block_hash.is_some() {
                    // TODO: Support dataset evolution
                    unimplemented!("Verifying datasets with evolving queries is not yet supported")
                }

                source = Some(src.clone());
            }

            if let Some(vc) = &block.vocab {
                vocab = Some(vc.clone())
            }

            let block_hash = block.block_hash;

            // TODO: Assuming `input_slices` is a reliable indicator of a transform block and a checkpoint presence
            if block.input_slices.is_some() {
                if !finished_range {
                    blocks.push(block);
                } else if prev_checkpoint.is_none() {
                    // TODO: this might be incorrect - test with specific start_block
                    prev_checkpoint = Some(block_hash);
                }
            }

            if !finished_range && Some(block_hash) == start_block {
                finished_range = true;
            }
        }

        // TODO: missing validation of whether start_block was found

        let source = source.ok_or(VerificationError::NotDerivative)?;
        let dataset_layout = DatasetLayout::new(&self.volume_layout, dataset_id);

        let dataset_vocabs = source
            .inputs
            .iter()
            .map(|id| -> Result<_, DomainError> { Ok((id.to_owned(), self.get_vocab(id)?)) })
            .collect::<Result<BTreeMap<_, _>, _>>()?;

        let mut plan: Vec<_> = blocks
            .into_iter()
            .rev()
            .map(|block| -> Result<VerificationStep, DomainError> {
                let step = VerificationStep {
                    operation: TransformOperation {
                        input_slices: block.input_slices.as_ref().unwrap().clone(),
                        request: ExecuteQueryRequest {
                            dataset_id: dataset_id.to_owned(),
                            system_time: block.system_time,
                            offset: block
                                .output_slice
                                .as_ref()
                                .map(|s| s.data_interval.start)
                                .unwrap_or(0), // TODO: Assuming offset does not matter if block is not supposed to produce data
                            transform: source.transform.clone(),
                            vocab: vocab.clone().unwrap_or_default(),
                            inputs: block
                                .input_slices
                                .as_ref()
                                .unwrap()
                                .iter()
                                .map(|slice| {
                                    self.to_query_input(
                                        slice,
                                        Some(
                                            dataset_vocabs
                                                .get(&slice.dataset_id)
                                                .map(|v| v.clone())
                                                .unwrap(),
                                        ),
                                    )
                                })
                                .collect::<Result<_, _>>()?,
                            prev_checkpoint_dir: None, // Filled out below
                            new_checkpoint_dir: dataset_layout.checkpoints_dir.join(".pending"),
                            out_data_path: dataset_layout.data_dir.join(".pending"),
                        },
                    },
                    expected: block,
                };

                Ok(step)
            })
            .collect::<Result<_, _>>()?;

        // Populate prev checkpoints
        for i in 1..plan.len() {
            plan[i].operation.request.prev_checkpoint_dir = Some(
                dataset_layout
                    .checkpoints_dir
                    .join(plan[i - 1].expected.block_hash.to_string()),
            )
        }
        if !plan.is_empty() {
            plan[0].operation.request.prev_checkpoint_dir =
                prev_checkpoint.map(|h| dataset_layout.checkpoints_dir.join(h.to_string()));
        }

        Ok(plan)
    }
}

impl TransformService for TransformServiceImpl {
    fn transform(
        &self,
        dataset_id: &DatasetID,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError> {
        let listener = maybe_listener.unwrap_or(Arc::new(NullTransformListener {}));

        info!(
            dataset_id = dataset_id.as_str(),
            "Transforming a single dataset"
        );

        // TODO: There might be more operations to do
        // TODO: Inject time source
        if let Some(operation) = self
            .get_next_operation(dataset_id, Utc::now())
            .map_err(|e| TransformError::internal(e))?
        {
            let dataset_layout = DatasetLayout::new(&self.volume_layout, dataset_id);

            let meta_chain = self.metadata_repo.get_metadata_chain(&dataset_id).unwrap();
            let head = meta_chain.read_ref(&BlockRef::Head).unwrap();

            Self::do_transform(
                self.engine_provisioner.clone(),
                operation,
                move |new_block, new_data_path, new_checkpoint_path| {
                    Self::commit_transform(
                        meta_chain,
                        dataset_id.to_owned(),
                        dataset_layout,
                        head,
                        new_block,
                        new_data_path,
                        new_checkpoint_path,
                    )
                },
                listener,
            )
        } else {
            listener.begin();
            listener.success(&TransformResult::UpToDate);
            Ok(TransformResult::UpToDate)
        }
    }

    fn transform_multi(
        &self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        maybe_multi_listener: Option<Arc<dyn TransformMultiListener>>,
    ) -> Vec<(DatasetIDBuf, Result<TransformResult, TransformError>)> {
        let null_multi_listener = Arc::new(NullTransformMultiListener {});
        let multi_listener = maybe_multi_listener.unwrap_or(null_multi_listener);

        let dataset_ids_owned: Vec<_> = dataset_ids.map(|id| id.to_owned()).collect();
        info!(dataset_ids = ?dataset_ids_owned, "Transforming multiple datasets");

        // TODO: handle errors without crashing
        let requests: Vec<_> = dataset_ids_owned
            .into_iter()
            .map(|dataset_id| {
                let listener = multi_listener
                    .begin_transform(&dataset_id)
                    .unwrap_or(Arc::new(NullTransformListener {}));

                // TODO: Inject time source
                let next_op = self
                    .get_next_operation(&dataset_id, Utc::now())
                    .map_err(|e| TransformError::internal(e))
                    .unwrap();

                (dataset_id, next_op, listener)
            })
            .collect();

        let mut results: Vec<(DatasetIDBuf, Result<TransformResult, TransformError>)> =
            Vec::with_capacity(requests.len());

        let thread_handles: Vec<_> = requests
            .into_iter()
            .filter_map(
                |(dataset_id, maybe_request, listener)| match maybe_request {
                    None => {
                        listener.begin();
                        listener.success(&TransformResult::UpToDate);
                        results.push((dataset_id, Ok(TransformResult::UpToDate)));
                        None
                    }
                    Some(request) => {
                        let engine_provisioner = self.engine_provisioner.clone();

                        let commit_fn = {
                            let meta_chain =
                                self.metadata_repo.get_metadata_chain(&dataset_id).unwrap();
                            let head = meta_chain.read_ref(&BlockRef::Head).unwrap();
                            let dataset_id = dataset_id.clone();
                            let dataset_layout =
                                DatasetLayout::new(&self.volume_layout, &dataset_id);

                            move |new_block, new_data_path: &Path, new_checkpoint_path: &Path| {
                                Self::commit_transform(
                                    meta_chain,
                                    dataset_id,
                                    dataset_layout,
                                    head,
                                    new_block,
                                    new_data_path,
                                    new_checkpoint_path,
                                )
                            }
                        };

                        let thread_handle = std::thread::Builder::new()
                            .name("transform_multi".to_owned())
                            .spawn(move || {
                                let res = Self::do_transform(
                                    engine_provisioner,
                                    request,
                                    commit_fn,
                                    listener,
                                );
                                (dataset_id, res)
                            })
                            .unwrap();

                        Some(thread_handle)
                    }
                },
            )
            .collect();

        results.extend(thread_handles.into_iter().map(|h| h.join().unwrap()));

        results
    }

    fn verify_transform(
        &self,
        dataset_id: &DatasetID,
        block_range: (Option<Sha3_256>, Option<Sha3_256>),
        _options: VerificationOptions,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        let span = info_span!("Replaying dataset transformations", dataset_id = dataset_id.as_str(), block_range = ?block_range);
        let _span_guard = span.enter();

        let listener = maybe_listener.unwrap_or(Arc::new(NullVerificationListener {}));

        let verification_plan = self.get_verification_plan(dataset_id, block_range)?;
        let num_steps = verification_plan.len();
        listener.begin_phase(VerificationPhase::ReplayTransform, num_steps);

        for (step_index, step) in verification_plan.into_iter().enumerate() {
            let operation = step.operation;
            let expected_block = step.expected;
            let mut actual_block = None;

            info!(
                block_hash = ?expected_block.block_hash,
                "Replaying block"
            );

            listener.begin_block(
                &expected_block.block_hash,
                step_index,
                num_steps,
                VerificationPhase::ReplayTransform,
            );

            let transform_listener = listener
                .clone()
                .get_transform_listener()
                .unwrap_or_else(|| Arc::new(NullTransformListener));

            Self::do_transform(
                self.engine_provisioner.clone(),
                operation,
                |mut new_block: MetadataBlock, new_data_path, new_checkpoint_path| {
                    // Cleanup not needed outputs
                    if new_block.output_slice.is_some() {
                        std::fs::remove_file(new_data_path)
                            .map_err(|e| TransformError::internal(e))?;
                    }
                    std::fs::remove_dir_all(new_checkpoint_path)
                        .map_err(|e| TransformError::internal(e))?;

                    // Link new block
                    new_block.prev_block_hash = expected_block.prev_block_hash;
                    new_block.block_hash = FlatbuffersMetadataBlockSerializer
                        .set_metadata_block_hash(
                            &mut FlatbuffersMetadataBlockSerializer
                                .serialize_metadata_block(&new_block),
                        )
                        .unwrap();

                    // All we care about is the new block
                    actual_block = Some(new_block);

                    Ok(TransformResult::Updated {
                        old_head: expected_block.prev_block_hash.unwrap(),
                        new_head: expected_block.block_hash,
                        num_blocks: 1,
                    })
                },
                transform_listener,
            )?;

            let actual_block = actual_block.unwrap();
            debug!(expected = ?expected_block, actual = ?actual_block, "Comparing results");

            if expected_block != actual_block {
                info!(block_hash = ?expected_block.block_hash, expected = ?expected_block, actual = ?actual_block, "Block invalid");

                let err = VerificationError::DataNotReproducible(DataNotReproducible {
                    expected_block,
                    actual_block,
                });
                listener.error(&err);
                return Err(err);
            }

            info!(block_hash = ?expected_block.block_hash, "Block valid");
            listener.end_block(
                &expected_block.block_hash,
                step_index,
                num_steps,
                VerificationPhase::ReplayTransform,
            );
        }

        listener.end_phase(VerificationPhase::ReplayTransform, num_steps);
        Ok(VerificationResult::Valid {
            blocks_verified: num_steps,
        })
    }

    fn verify_transform_multi(
        &self,
        _datasets: &mut dyn Iterator<Item = VerificationRequest>,
        _options: VerificationOptions,
        _listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        unimplemented!()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq)]
pub struct TransformOperation {
    pub input_slices: Vec<InputSlice>,
    pub request: ExecuteQueryRequest,
}

#[derive(Debug)]
pub struct VerificationStep {
    pub operation: TransformOperation,
    pub expected: MetadataBlock,
}
