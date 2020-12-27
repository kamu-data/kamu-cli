use crate::domain::*;
use crate::infra::*;
use opendatafabric::*;

use slog::{info, o, Logger};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

pub struct TransformServiceImpl {
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
    engine_factory: Arc<Mutex<EngineFactory>>,
    volume_layout: VolumeLayout,
    logger: Logger,
}

impl TransformServiceImpl {
    pub fn new(
        metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
        engine_factory: Arc<Mutex<EngineFactory>>,
        volume_layout: &VolumeLayout,
        logger: Logger,
    ) -> Self {
        Self {
            metadata_repo: metadata_repo,
            engine_factory: engine_factory,
            volume_layout: volume_layout.clone(),
            logger: logger,
        }
    }

    // Note: Can be called from multiple threads
    fn do_transform(
        request: ExecuteQueryRequest,
        meta_chain: Box<dyn MetadataChain>,
        listener: Arc<Mutex<dyn TransformListener>>,
        engine_factory: Arc<Mutex<EngineFactory>>,
    ) -> Result<TransformResult, TransformError> {
        listener.lock().unwrap().begin();

        match Self::do_transform_inner(request, meta_chain, engine_factory, listener.clone()) {
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
        request: ExecuteQueryRequest,
        mut meta_chain: Box<dyn MetadataChain>,
        engine_factory: Arc<Mutex<EngineFactory>>,
        listener: Arc<Mutex<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError> {
        let prev_hash = meta_chain.read_ref(&BlockRef::Head);

        let engine = engine_factory.lock().unwrap().get_engine(
            match request.source.transform {
                Transform::Sql(ref sql) => &sql.engine,
            },
            listener.lock().unwrap().get_pull_image_listener(),
        )?;

        let result = engine.lock().unwrap().transform(request)?;

        let new_block = MetadataBlock {
            prev_block_hash: prev_hash,
            ..result.block
        };
        let block_hash = meta_chain.append(new_block);

        Ok(TransformResult::Updated {
            block_hash: block_hash,
        })
    }

    pub fn get_next_operation(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Option<ExecuteQueryRequest>, DomainError> {
        let logger = self
            .logger
            .new(o!("output_dataset" => dataset_id.as_str().to_owned()));

        info!(logger, "Evaluating next transform operation");

        let output_chain = self.metadata_repo.borrow().get_metadata_chain(dataset_id)?;

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

        let mut non_empty_slices = 0;
        let input_slices: BTreeMap<_, _> = source
            .inputs
            .iter()
            .enumerate()
            .map(|(index, input_id)| {
                let (slice, empty_slice) = self.get_input_slice(
                    index,
                    input_id,
                    output_chain.as_ref(),
                    logger.new(o!("input_dataset" => input_id.as_str().to_owned())),
                )?;

                if !empty_slice {
                    non_empty_slices += 1;
                }

                Ok((input_id.clone(), slice))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;

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

        let mut data_dirs: BTreeMap<_, _> = source
            .inputs
            .iter()
            .map(|input_id| {
                (
                    input_id.clone(),
                    DatasetLayout::new(&self.volume_layout, &input_id).data_dir,
                )
            })
            .collect();

        data_dirs.insert(dataset_id.to_owned(), output_layout.data_dir);

        if non_empty_slices > 0 {
            Ok(Some(ExecuteQueryRequest {
                dataset_id: dataset_id.to_owned(),
                checkpoints_dir: output_layout.checkpoints_dir, // TODO: move down a layer
                source: source,
                dataset_vocabs: vocabs,
                input_slices: input_slices,
                data_dirs: data_dirs, // TODO: move down a layer
            }))
        } else {
            Ok(None)
        }
    }

    fn is_never_pulled(&self, dataset_id: &DatasetID) -> Result<bool, DomainError> {
        let chain = self.metadata_repo.borrow().get_metadata_chain(dataset_id)?;
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
        output_chain: &dyn MetadataChain,
        logger: Logger,
    ) -> Result<(InputDataSlice, bool), DomainError> {
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

        let input_chain = self.metadata_repo.borrow().get_metadata_chain(dataset_id)?;

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

        let explicit_watermarks: Vec<_> = blocks_unprocessed
            .iter()
            .rev()
            .filter(|b| b.output_watermark.is_some())
            .map(|b| Watermark {
                system_time: b.system_time.clone(),
                event_time: b.output_watermark.unwrap().clone(),
            })
            .collect();

        let empty = !blocks_unprocessed.iter().any(|b| b.output_slice.is_some())
            && explicit_watermarks.is_empty();

        info!(logger, "Computed input slice";
            "iv_unprocessed" => ?iv_unprocessed,
            "iv_available" => ?iv_available,
            "iv_to_process" => ?iv_to_process,
            "unprocessed_blocks" => blocks_unprocessed.len(),
            "watermarks" => ?explicit_watermarks,
            "empty" => empty);

        Ok((
            InputDataSlice {
                interval: iv_to_process,
                explicit_watermarks: explicit_watermarks,
            },
            empty,
        ))
    }

    // TODO: Avoid iterating through output chain multiple times
    fn get_vocab(&self, dataset_id: &DatasetID) -> Result<DatasetVocabulary, DomainError> {
        let chain = self.metadata_repo.borrow().get_metadata_chain(dataset_id)?;
        let vocab = chain.iter_blocks().filter_map(|b| b.vocab).next();
        Ok(vocab.unwrap_or_default())
    }
}

impl TransformService for TransformServiceImpl {
    fn transform(
        &mut self,
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
            let meta_chain = self
                .metadata_repo
                .borrow()
                .get_metadata_chain(&dataset_id)
                .unwrap();

            Self::do_transform(request, meta_chain, listener, self.engine_factory.clone())
        } else {
            Ok(TransformResult::UpToDate)
        }
    }

    fn transform_multi(
        &mut self,
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
                    let meta_chain = self
                        .metadata_repo
                        .borrow()
                        .get_metadata_chain(&dataset_id)
                        .unwrap();
                    let engine_factory = self.engine_factory.clone();

                    let thread_handle = std::thread::Builder::new()
                        .name("transform_multi".to_owned())
                        .spawn(move || {
                            let res =
                                Self::do_transform(request, meta_chain, listener, engine_factory);
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
}
