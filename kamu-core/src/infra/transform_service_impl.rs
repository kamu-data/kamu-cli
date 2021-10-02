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
use opendatafabric::*;

use dill::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::debug;
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
        request: ExecuteQueryRequest,
        commit_fn: impl FnOnce(MetadataBlock, &Path, &Path) -> Result<TransformResult, TransformError>,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, TransformError> {
        listener.begin();

        match Self::do_transform_inner(engine_provisioner, request, commit_fn, listener.clone()) {
            Ok(res) => {
                listener.success(&res);
                Ok(res)
            }
            Err(err) => {
                listener.error(&err);
                Err(err)
            }
        }
    }

    // Note: Can be called from multiple threads
    fn do_transform_inner(
        engine_provisioner: Arc<dyn EngineProvisioner>,
        request: ExecuteQueryRequest,
        commit_fn: impl FnOnce(MetadataBlock, &Path, &Path) -> Result<TransformResult, TransformError>,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, TransformError> {
        let span = info_span!(
            "Performing transform",
            output_dataset = request.dataset_id.as_str()
        );
        let _span_guard = span.enter();
        info!(request = ?request, "Tranform request");

        let new_checkpoint_path = PathBuf::from(&request.new_checkpoint_dir);
        let out_data_path = PathBuf::from(&request.out_data_path);

        let engine = engine_provisioner.provision_engine(
            match request.transform {
                Transform::Sql(ref sql) => &sql.engine,
            },
            listener.clone().get_engine_provisioning_listener(),
        )?;

        let result = engine.transform(request)?;

        if let Some(ref slice) = result.metadata_block.output_slice {
            if slice.num_records == 0 {
                return Err(EngineError::contract_error(
                    "Engine returned an output slice with zero records",
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
        } else if out_data_path.exists() {
            return Err(EngineError::contract_error(
                "Engine wrote data file while the ouput slice is empty",
                Vec::new(),
            )
            .into());
        }

        let result = commit_fn(result.metadata_block, &out_data_path, &new_checkpoint_path)?;

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
    ) -> Result<Option<ExecuteQueryRequest>, DomainError> {
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

        let inputs: Vec<_> = source
            .inputs
            .iter()
            .enumerate()
            .map(|(index, input_id)| {
                let input_layout = DatasetLayout::new(&self.volume_layout, input_id);

                self.get_input_slice(index, input_id, &input_layout, output_chain.as_ref())
            })
            .collect::<Result<Vec<_>, _>>()?;

        if inputs.iter().all(|s| s.is_empty()) {
            return Ok(None);
        }

        // TODO: Verify assumption that `input_slices` is a reliable indicator of a checkpoint presence
        let prev_checkpoint = output_chain
            .iter_blocks()
            .filter(|b| b.input_slices.is_some())
            .map(|b| b.block_hash)
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

        Ok(Some(ExecuteQueryRequest {
            dataset_id: dataset_id.to_owned(),
            vocab: self.get_vocab(dataset_id)?,
            transform: source.transform,
            inputs,
            prev_checkpoint_dir: prev_checkpoint
                .map(|hash| output_layout.checkpoints_dir.join(hash.to_string())),
            new_checkpoint_dir,
            out_data_path,
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
        index: usize,
        dataset_id: &DatasetID,
        dataset_layout: &DatasetLayout,
        output_chain: &dyn MetadataChain,
    ) -> Result<QueryInput, DomainError> {
        // Determine processed data range
        // Result is either: () or (inf, upper] or (lower, upper]
        let iv_processed = output_chain
            .iter_blocks()
            .filter_map(|b| b.input_slices)
            .map(|mut ss| ss.remove(index).interval)
            .find(|iv| !iv.is_empty())
            .unwrap_or(TimeInterval::empty());

        // Determine unprocessed data range
        // Result is either: (-inf, inf) or (lower, inf)
        let iv_unprocessed = iv_processed.right_complement();

        let input_chain = self.metadata_repo.get_metadata_chain(dataset_id)?;

        // Filter unprocessed input blocks
        let blocks_unprocessed: Vec<_> = input_chain
            .iter_blocks()
            .take_while(|b| iv_unprocessed.contains_point(&b.system_time))
            .collect();

        // Determine available data/watermark range
        // Result is either: () or (-inf, upper]
        let iv_available = blocks_unprocessed
            .first()
            .map(|b| TimeInterval::unbounded_closed_right(b.system_time.clone()))
            .unwrap_or(TimeInterval::empty());

        // Result is either: () or (lower, upper]
        let iv_to_process = iv_available.intersect(&iv_unprocessed);

        // List of part files that will be read by the engine
        // Note: Order is important
        // Note: Engine will still filter the rows by system time interval
        let data_paths: Vec<_> = blocks_unprocessed
            .iter()
            .rev()
            .filter(|b| b.output_slice.is_some())
            .map(|b| dataset_layout.data_dir.join(b.block_hash.to_string()))
            .collect();

        let explicit_watermarks: Vec<_> = blocks_unprocessed
            .iter()
            .rev()
            .filter(|b| b.output_watermark.is_some())
            .map(|b| Watermark {
                system_time: b.system_time.clone(),
                event_time: b.output_watermark.unwrap().clone(),
            })
            .collect();

        // TODO: Migrate to providing schema directly
        // TODO: Will not work with schema evolution
        let schema_file = data_paths
            .first()
            .map(|p| p.clone())
            .unwrap_or_else(|| self.get_schema_file_fallback(dataset_layout));

        let input = QueryInput {
            dataset_id: dataset_id.to_owned(),
            vocab: self.get_vocab(dataset_id)?,
            interval: iv_to_process,
            data_paths: data_paths,
            schema_file: schema_file,
            explicit_watermarks: explicit_watermarks,
        };

        info!(
            input_dataset = dataset_id.as_str(),
            input = ?input,
            empty = input.is_empty(),
            iv_unprocessed = ?iv_unprocessed,
            iv_available = ?iv_available,
            unprocessed_blocks = blocks_unprocessed.len(),
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
            "Preparing verification plan",
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
                    prev_checkpoint = Some(block_hash);
                }
            }

            if Some(block_hash) == start_block {
                finished_range = true;
            }
        }

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
            .map(|block| VerificationStep {
                request: ExecuteQueryRequest {
                    dataset_id: dataset_id.to_owned(),
                    transform: source.transform.clone(),
                    vocab: vocab.clone().unwrap_or_default(),
                    inputs: std::iter::zip(
                        source.inputs.iter(),
                        block.input_slices.as_ref().unwrap().iter(),
                    )
                    .map(|(id, slice)| {
                        QueryInput {
                            dataset_id: id.clone(),
                            vocab: dataset_vocabs.get(id).unwrap().clone(),
                            interval: slice.interval,
                            data_paths: Vec::new(),      // Filled out below
                            schema_file: PathBuf::new(), // Filled out below
                            explicit_watermarks: Vec::new(), // Filled out below
                        }
                    })
                    .collect(),
                    prev_checkpoint_dir: None, // Filled out below
                    new_checkpoint_dir: dataset_layout.checkpoints_dir.join(".pending"),
                    out_data_path: dataset_layout.data_dir.join(".pending"),
                },
                expected: block,
            })
            .collect();

        // Populate prev checkpoints
        for i in 1..plan.len() {
            plan[i].request.prev_checkpoint_dir = Some(
                dataset_layout
                    .checkpoints_dir
                    .join(plan[i - 1].expected.block_hash.to_string()),
            )
        }
        if !plan.is_empty() {
            plan[0].request.prev_checkpoint_dir =
                prev_checkpoint.map(|h| dataset_layout.checkpoints_dir.join(h.to_string()));
        }

        // Populate input slices
        // TODO: Assuming source never changes
        // This is the rough part - we walk backwards through input datasets' metadata chains and
        // include blocks into corresponding steps based on the time interval ranges.
        // Think of this as a interval-based merge join X_X
        for input_id in source.inputs.iter() {
            let input_chain = self.metadata_repo.get_metadata_chain(input_id)?;
            let input_layout = DatasetLayout::new(&self.volume_layout, &input_id);

            let mut step_iter = plan.iter_mut().rev();
            let mut input_blocks_iter = input_chain
                .iter_blocks()
                .filter(|b| b.output_slice.is_some() || b.output_watermark.is_some());

            let mut curr_step = step_iter.next();
            let mut curr_input_block = input_blocks_iter.next();

            while curr_step.is_some() && curr_input_block.is_some() {
                let step = curr_step.as_mut().unwrap();
                let input_block = curr_input_block.as_ref().unwrap();
                let slice = step
                    .request
                    .inputs
                    .iter_mut()
                    .filter(|i| i.dataset_id == *input_id)
                    .next()
                    .unwrap();

                debug!(
                    input_id = input_id.as_str(),
                    output_block = ?step.expected.block_hash,
                    input_block = ?input_block.block_hash,
                    "Considering blocks"
                );

                // Interval can only be (), or (lower, upper]
                if slice.interval.is_empty() {
                    curr_step = step_iter.next();
                    continue;
                }

                // Input block is younger than step block - continue to next input block
                if slice
                    .interval
                    .right_complement()
                    .contains_point(&input_block.system_time)
                {
                    curr_input_block = input_blocks_iter.next();
                    continue;
                }

                // Input block is older than step block - continue to next step
                if slice
                    .interval
                    .left_complement()
                    .contains_point(&input_block.system_time)
                {
                    curr_step = step_iter.next();
                    continue;
                }

                // Bingo! Include block inputs into the step
                assert!(slice.interval.contains_point(&input_block.system_time));

                debug!(
                    input_id = input_id.as_str(),
                    output_block = ?step.expected.block_hash,
                    input_block = ?input_block.block_hash,
                    "Associating input block"
                );

                if let Some(event_time) = input_block.output_watermark {
                    slice.explicit_watermarks.push(Watermark {
                        system_time: input_block.system_time,
                        event_time,
                    });
                }

                if input_block.output_slice.is_some() {
                    slice.data_paths.push(
                        input_layout
                            .data_dir
                            .join(input_block.block_hash.to_string()),
                    )
                }

                curr_input_block = input_blocks_iter.next();
            }
        }

        // Post-process input slices
        for step in plan.iter_mut() {
            for slice in step.request.inputs.iter_mut() {
                slice.data_paths.reverse();
                slice.explicit_watermarks.reverse();
                // TODO: Migrate to providing schema directly
                slice.schema_file =
                    slice
                        .data_paths
                        .first()
                        .map(|p| p.clone())
                        .unwrap_or_else(|| {
                            self.get_schema_file_fallback(&DatasetLayout::new(
                                &self.volume_layout,
                                &slice.dataset_id,
                            ))
                        });
            }

            if step.request.is_empty() {
                panic!(
                    "Produced empty transform for block {}",
                    step.expected.block_hash
                );
            }
        }

        Ok(plan)
    }

    fn check_blocks_equivalent(
        expected_block: &MetadataBlock,
        actual_block: &MetadataBlock,
    ) -> bool {
        // TODO: Moving from time intervals to offsets will avoid the need for this
        let expected_output_slice = expected_block.output_slice.clone().map(|s| DataSlice {
            interval: TimeInterval::empty(),
            ..s
        });
        let actual_output_slice = actual_block.output_slice.clone().map(|s| DataSlice {
            interval: TimeInterval::empty(),
            ..s
        });
        expected_block.input_slices == actual_block.input_slices
            && expected_output_slice == actual_output_slice
            && expected_block.output_watermark == actual_block.output_watermark
    }
}

impl TransformService for TransformServiceImpl {
    fn transform(
        &self,
        dataset_id: &DatasetID,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError> {
        let null_listener = Arc::new(NullTransformListener {});
        let listener = maybe_listener.unwrap_or(null_listener);

        info!(
            dataset_id = dataset_id.as_str(),
            "Transforming a single dataset"
        );

        // TODO: There might be more operations to do
        if let Some(request) = self
            .get_next_operation(dataset_id)
            .map_err(|e| TransformError::internal(e))?
        {
            let dataset_layout = DatasetLayout::new(&self.volume_layout, dataset_id);

            let meta_chain = self.metadata_repo.get_metadata_chain(&dataset_id).unwrap();
            let head = meta_chain.read_ref(&BlockRef::Head).unwrap();

            Self::do_transform(
                self.engine_provisioner.clone(),
                request,
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
                let next_op = self
                    .get_next_operation(&dataset_id)
                    .map_err(|e| TransformError::internal(e))
                    .unwrap();
                (dataset_id, next_op)
            })
            .collect();

        let mut results: Vec<(DatasetIDBuf, Result<TransformResult, TransformError>)> =
            Vec::with_capacity(requests.len());

        let thread_handles: Vec<_> = requests
            .into_iter()
            .filter_map(|(dataset_id, maybe_request)| match maybe_request {
                None => {
                    results.push((dataset_id, Ok(TransformResult::UpToDate)));
                    None
                }
                Some(request) => {
                    let null_listener = Arc::new(NullTransformListener {});

                    let listener = multi_listener
                        .begin_transform(&dataset_id)
                        .unwrap_or(null_listener);

                    let engine_provisioner = self.engine_provisioner.clone();

                    let commit_fn = {
                        let meta_chain =
                            self.metadata_repo.get_metadata_chain(&dataset_id).unwrap();
                        let head = meta_chain.read_ref(&BlockRef::Head).unwrap();
                        let dataset_id = dataset_id.clone();
                        let dataset_layout = DatasetLayout::new(&self.volume_layout, &dataset_id);

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
            })
            .collect();

        results.extend(thread_handles.into_iter().map(|h| h.join().unwrap()));

        results
    }

    fn verify(
        &self,
        dataset_id: &DatasetID,
        block_range: (Option<Sha3_256>, Option<Sha3_256>),
        _options: VerificationOptions,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        let span = info_span!("Verifying dataset", dataset_id = dataset_id.as_str(), block_range = ?block_range);
        let _span_guard = span.enter();

        let null_listener = Arc::new(NullVerificationListener {});
        let listener = maybe_listener.unwrap_or(null_listener);

        let verification_plan = self.get_verification_plan(dataset_id, block_range)?;
        let num_steps = verification_plan.len();
        listener.begin(num_steps);

        for (step_index, step) in verification_plan.into_iter().enumerate() {
            let request = step.request;
            let expected_block = step.expected;
            let mut actual_block = None;

            info!(
                block_hash = ?expected_block.block_hash,
                "Verifying block"
            );

            listener.on_phase(
                &expected_block.block_hash,
                step_index,
                num_steps,
                VerificationPhase::HashData,
            );
            /////////////////////
            ////////
            //////////
            // TODO: VERIFY DATA HASH!!!!!!!!!!!!!!!!
            ///////////
            /////////
            //////////

            listener.on_phase(
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
                request,
                |new_block: MetadataBlock, new_data_path, new_checkpoint_path| {
                    // Cleanup not needed outputs
                    if new_block.output_slice.is_some() {
                        std::fs::remove_file(new_data_path)
                            .map_err(|e| TransformError::internal(e))?;
                    }
                    std::fs::remove_dir_all(new_checkpoint_path)
                        .map_err(|e| TransformError::internal(e))?;

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

            if !Self::check_blocks_equivalent(&expected_block, &actual_block) {
                info!(block_hash = ?expected_block.block_hash, expected = ?expected_block, actual = ?actual_block, "Block invalid");

                let err = VerificationError::DataNotReproducible(DataNotReproducible {
                    expected_block,
                    actual_block,
                });
                listener.error(&err);
                return Err(err);
            }

            info!(block_hash = ?expected_block.block_hash, "Block valid");
            listener.on_phase(
                &expected_block.block_hash,
                step_index,
                num_steps,
                VerificationPhase::BlockValid,
            );
        }

        let res = VerificationResult::Valid {
            blocks_verified: num_steps,
        };
        listener.success(&res);
        Ok(res)
    }

    fn verify_multi(
        &self,
        _datasets: &mut dyn Iterator<Item = VerificationRequest>,
        _options: VerificationOptions,
        _listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        unimplemented!()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VerificationStep {
    pub request: ExecuteQueryRequest,
    pub expected: MetadataBlock,
}
