use crate::domain::*;
use crate::infra::*;
use opendatafabric::*;

use dill::*;
use slog::debug;
use slog::{info, o, Logger};
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub struct TransformServiceImpl {
    metadata_repo: Arc<dyn MetadataRepository>,
    engine_factory: Arc<dyn EngineFactory>,
    volume_layout: VolumeLayout,
    logger: Logger,
}

#[component(pub)]
impl TransformServiceImpl {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        engine_factory: Arc<dyn EngineFactory>,
        volume_layout: &VolumeLayout,
        logger: Logger,
    ) -> Self {
        Self {
            metadata_repo,
            engine_factory,
            volume_layout: volume_layout.clone(),
            logger: logger,
        }
    }

    // Note: Can be called from multiple threads
    fn do_transform(
        engine_factory: Arc<dyn EngineFactory>,
        request: ExecuteQueryRequest,
        commit_fn: impl FnOnce(MetadataBlock, &Path, &Path) -> Result<TransformResult, TransformError>,
        listener: Arc<Mutex<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError> {
        listener.lock().unwrap().begin();

        match Self::do_transform_inner(engine_factory, request, commit_fn, listener.clone()) {
            Ok(res) => {
                listener.lock().unwrap().success(&res);
                Ok(res)
            }
            Err(err) => {
                listener.lock().unwrap().error(&err);
                Err(err)
            }
        }
    }

    // Note: Can be called from multiple threads
    fn do_transform_inner(
        engine_factory: Arc<dyn EngineFactory>,
        request: ExecuteQueryRequest,
        commit_fn: impl FnOnce(MetadataBlock, &Path, &Path) -> Result<TransformResult, TransformError>,
        listener: Arc<Mutex<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError> {
        let new_checkpoint_path = request.new_checkpoint_dir.clone();
        let out_data_path = request.out_data_path.clone();

        let engine = engine_factory.get_engine(
            match request.source.transform {
                Transform::Sql(ref sql) => &sql.engine,
            },
            listener.lock().unwrap().get_pull_image_listener(),
        )?;

        let result = engine.lock().unwrap().transform(request)?;

        if let Some(ref slice) = result.block.output_slice {
            if slice.num_records == 0 {
                return Err(EngineError::ContractError(ContractError::new(
                    "Engine returned an output slice with zero records",
                    Vec::new(),
                ))
                .into());
            }
            if !out_data_path.exists() {
                return Err(EngineError::ContractError(ContractError::new(
                    "Engine did not write a response data file",
                    Vec::new(),
                ))
                .into());
            }
        }

        let result = commit_fn(result.block, &out_data_path, &new_checkpoint_path)?;

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
        logger: Logger,
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

        info!(logger, "Committed new block"; "output_dataset" => dataset_id.as_str(), "new_head" => new_block_hash.to_string());

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
        let logger = self
            .logger
            .new(o!("output_dataset" => dataset_id.as_str().to_owned()));

        info!(logger, "Evaluating next transform operation");

        let output_chain = self.metadata_repo.get_metadata_chain(dataset_id)?;

        // TODO: limit traversal depth
        let mut sources: Vec<_> = output_chain
            .iter_blocks()
            .filter_map(|b| b.source)
            .collect();

        // TODO: source could've changed several times
        if sources.len() > 1 {
            unimplemented!("Transform evolution is not yet supported");
        }

        let source = match sources.pop().unwrap() {
            DatasetSource::Derivative(src) => src,
            _ => panic!("Transform called on non-derivative dataset {}", dataset_id),
        };

        if source
            .inputs
            .iter()
            .any(|id| self.is_never_pulled(id).unwrap())
        {
            info!(
                logger,
                "Not processing because one of the inputs was never pulled"
            );
            return Ok(None);
        }

        let input_slices: BTreeMap<_, _> = source
            .inputs
            .iter()
            .enumerate()
            .map(|(index, input_id)| {
                let input_layout = DatasetLayout::new(&self.volume_layout, input_id);

                let slice = self.get_input_slice(
                    index,
                    input_id,
                    &input_layout,
                    output_chain.as_ref(),
                    logger.new(o!("input_dataset" => input_id.as_str().to_owned())),
                )?;

                Ok((input_id.clone(), slice))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;

        if input_slices.values().all(|s| s.is_empty()) {
            return Ok(None);
        }

        // TODO: Verify assumption that `input_slices` is a reliable indicator of a checkpoint presence
        let prev_checkpoint = output_chain
            .iter_blocks()
            .filter(|b| b.input_slices.is_some())
            .map(|b| b.block_hash)
            .next();

        let mut vocabs = source
            .inputs
            .iter()
            .map(|input_id| {
                self.get_vocab(input_id)
                    .map(|vocab| (input_id.clone(), vocab))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;

        vocabs.insert(dataset_id.to_owned(), self.get_vocab(dataset_id)?);

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
            prev_checkpoint_dir: prev_checkpoint
                .map(|hash| output_layout.checkpoints_dir.join(hash.to_string())),
            new_checkpoint_dir: new_checkpoint_dir.clone(),
            source: source,
            dataset_vocabs: vocabs,
            input_slices: input_slices,
            out_data_path: out_data_path,
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
        logger: Logger,
    ) -> Result<InputDataSlice, DomainError> {
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

        let slice = InputDataSlice {
            interval: iv_to_process,
            data_paths: data_paths,
            schema_file: schema_file,
            explicit_watermarks: explicit_watermarks,
        };

        info!(logger, "Computed input slice";
            "slice" => ?slice,
            "empty" => slice.is_empty(),
            "iv_unprocessed" => ?iv_unprocessed,
            "iv_available" => ?iv_available,
            "unprocessed_blocks" => blocks_unprocessed.len(),
        );

        Ok(slice)
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

        let logger = self
            .logger
            .new(o!("output_dataset" => dataset_id.as_str().to_owned()));
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

        let mut dataset_vocabs = source
            .inputs
            .iter()
            .map(|id| -> Result<_, DomainError> { Ok((id.to_owned(), self.get_vocab(id)?)) })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        dataset_vocabs.insert(dataset_id.to_owned(), vocab.unwrap_or_default());

        let mut plan: Vec<_> = blocks
            .into_iter()
            .rev()
            .map(|block| VerificationStep {
                request: ExecuteQueryRequest {
                    dataset_id: dataset_id.to_owned(),
                    source: source.clone(),
                    dataset_vocabs: dataset_vocabs.clone(),
                    input_slices: std::iter::zip(
                        source.inputs.iter(),
                        block.input_slices.as_ref().unwrap().iter(),
                    )
                    .map(|(id, slice)| {
                        (
                            id.to_owned(),
                            InputDataSlice {
                                interval: slice.interval,
                                data_paths: Vec::new(), // Filled out below
                                schema_file: PathBuf::new(), // Filled out below
                                explicit_watermarks: Vec::new(), // Filled out below
                            },
                        )
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
                let slice = step.request.input_slices.get_mut(input_id).unwrap();

                debug!(logger, "Considering blocks";
                    "input_id" => input_id.as_str(),
                    "output_block" => step.expected.block_hash.to_string(),
                    "input_block" => input_block.block_hash.to_string());

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

                debug!(logger, "Associating input block";
                    "input_id" => input_id.as_str(),
                    "output_block" => step.expected.block_hash.to_string(),
                    "input_block" => input_block.block_hash.to_string());

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
            for (input_id, slice) in step.request.input_slices.iter_mut() {
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
                                input_id,
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

        debug!(logger, "Final verification plan"; "plan" => ?plan);
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
        maybe_listener: Option<Arc<Mutex<dyn TransformListener>>>,
    ) -> Result<TransformResult, TransformError> {
        let null_listener = Arc::new(Mutex::new(NullTransformListener {}));
        let listener = maybe_listener.unwrap_or(null_listener);

        info!(self.logger, "Transforming single dataset"; "dataset" => dataset_id.as_str());

        // TODO: There might be more operations to do
        if let Some(request) = self
            .get_next_operation(dataset_id)
            .map_err(|e| TransformError::internal(e))?
        {
            let dataset_layout = DatasetLayout::new(&self.volume_layout, dataset_id);

            let meta_chain = self.metadata_repo.get_metadata_chain(&dataset_id).unwrap();
            let head = meta_chain.read_ref(&BlockRef::Head).unwrap();

            Self::do_transform(
                self.engine_factory.clone(),
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
                        self.logger.clone(),
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
        maybe_multi_listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<TransformResult, TransformError>)> {
        let null_multi_listener = Arc::new(Mutex::new(NullTransformMultiListener {}));
        let multi_listener = maybe_multi_listener.unwrap_or(null_multi_listener);

        let dataset_ids_owned: Vec<_> = dataset_ids.map(|id| id.to_owned()).collect();
        info!(self.logger, "Transforming multiple datasets"; "datasets" => ?dataset_ids_owned);

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
                    let null_listener = Arc::new(Mutex::new(NullTransformListener {}));

                    let listener = multi_listener
                        .lock()
                        .unwrap()
                        .begin_transform(&dataset_id)
                        .unwrap_or(null_listener);

                    let engine_factory = self.engine_factory.clone();

                    let commit_fn = {
                        let meta_chain =
                            self.metadata_repo.get_metadata_chain(&dataset_id).unwrap();
                        let head = meta_chain.read_ref(&BlockRef::Head).unwrap();
                        let dataset_id = dataset_id.clone();
                        let dataset_layout = DatasetLayout::new(&self.volume_layout, &dataset_id);
                        let logger = self.logger.clone();

                        move |new_block, new_data_path: &Path, new_checkpoint_path: &Path| {
                            Self::commit_transform(
                                meta_chain,
                                dataset_id,
                                dataset_layout,
                                head,
                                new_block,
                                new_data_path,
                                new_checkpoint_path,
                                logger,
                            )
                        }
                    };

                    let thread_handle = std::thread::Builder::new()
                        .name("transform_multi".to_owned())
                        .spawn(move || {
                            let res =
                                Self::do_transform(engine_factory, request, commit_fn, listener);
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
        info!(self.logger, "Verifying dataset"; "dataset_id" => dataset_id.as_str(), "block_range" => ?block_range);

        let null_listener = Arc::new(NullVerificationListener {});
        let listener = maybe_listener.unwrap_or(null_listener);

        let verification_plan = self.get_verification_plan(dataset_id, block_range)?;
        let num_steps = verification_plan.len();
        listener.begin(num_steps);

        for (step_index, step) in verification_plan.into_iter().enumerate() {
            let request = step.request;
            let expected_block = step.expected;
            let mut actual_block = None;

            info!(self.logger, "Verifying block"; "dataset_id" => dataset_id.as_str(), "block_hash" => expected_block.block_hash.to_string());

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

            Self::do_transform(
                self.engine_factory.clone(),
                request,
                |new_block, new_data_path, new_checkpoint_path| {
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
                Arc::new(Mutex::new(NullTransformListener)),
            )?;

            let actual_block = actual_block.unwrap();
            debug!(self.logger, "Comparing results"; "expected" => ?expected_block, "actual" => ?actual_block);

            if !Self::check_blocks_equivalent(&expected_block, &actual_block) {
                info!(self.logger, "Block invalid"; "dataset_id" => dataset_id.as_str(), "block_hash" => expected_block.block_hash.to_string());

                let err = VerificationError::DataNotReproducible(DataNotReproducible {
                    expected_block,
                    actual_block,
                });
                listener.error(&err);
                return Err(err);
            }

            info!(self.logger, "Block valid"; "dataset_id" => dataset_id.as_str(), "block_hash" => expected_block.block_hash.to_string());
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
