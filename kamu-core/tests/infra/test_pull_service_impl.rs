use kamu::domain::*;
use kamu::infra::*;
use kamu_test::*;

use itertools::Itertools;
use std::cell::RefCell;
use std::convert::TryFrom;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

fn id(s: &str) -> DatasetIDBuf {
    DatasetIDBuf::try_from(s).unwrap()
}

fn create_graph(
    repo: &mut MetadataRepositoryImpl,
    associations: Vec<(DatasetIDBuf, Option<DatasetIDBuf>)>,
) {
    for (dataset_id, group) in &associations.into_iter().group_by(|(id, _)| id.clone()) {
        let deps: Vec<_> = group.filter_map(|(_, d)| d).collect();
        if deps.is_empty() {
            repo.add_dataset(MetadataFactory::dataset_snapshot().id(&dataset_id).build())
                .unwrap();
        } else {
            repo.add_dataset(
                MetadataFactory::dataset_snapshot()
                    .id(&dataset_id)
                    .source(MetadataFactory::dataset_source_deriv(deps.iter()).build())
                    .build(),
            )
            .unwrap();
        }
    }
}

#[test]
fn test_pull_batching() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = Rc::new(RefCell::new(MetadataRepositoryImpl::new(
        &WorkspaceLayout::create(tmp_dir.path()).unwrap(),
    )));
    let test_ingest_svc = Rc::new(RefCell::new(TestIngestService::new()));
    let test_transform_svc = Rc::new(RefCell::new(TestTransformService::new()));
    let mut pull_svc = PullServiceImpl::new(
        repo.clone(),
        test_ingest_svc.clone(),
        test_transform_svc.clone(),
        slog::Logger::root(slog::Discard, slog::o!()),
    );

    //    / C \
    // A <     > > E
    //    \ D / /
    //         /
    // B - - -/
    create_graph(
        &mut repo.borrow_mut(),
        vec![
            (id("a"), None),
            (id("b"), None),
            (id("c"), Some(id("a"))),
            (id("d"), Some(id("a"))),
            (id("e"), Some(id("c"))),
            (id("e"), Some(id("d"))),
            (id("e"), Some(id("b"))),
        ],
    );

    pull_svc.pull_multi(
        &mut [id("e")].iter().map(|id| id.as_ref()),
        false,
        false,
        None,
        None,
    );

    assert!(test_ingest_svc.borrow().calls.is_empty());

    assert_eq!(test_transform_svc.borrow().calls, vec![vec![id("e")]]);

    test_transform_svc.borrow_mut().calls.clear();

    pull_svc.pull_multi(
        &mut [id("e")].iter().map(|id| id.as_ref()),
        true,
        false,
        None,
        None,
    );

    assert_eq!(test_ingest_svc.borrow().calls, vec![vec![id("a"), id("b")]]);

    assert_eq!(
        test_transform_svc.borrow().calls,
        vec![vec![id("c"), id("d")], vec![id("e")]]
    );
}

pub struct TestIngestService {
    calls: Vec<Vec<DatasetIDBuf>>,
}

impl TestIngestService {
    pub fn new() -> Self {
        Self { calls: Vec::new() }
    }
}

impl IngestService for TestIngestService {
    fn ingest(
        &mut self,
        _dataset_id: &DatasetID,
        _maybe_listener: Option<Arc<Mutex<dyn IngestListener>>>,
    ) -> Result<IngestResult, IngestError> {
        unimplemented!();
    }

    fn ingest_multi(
        &mut self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        _maybe_multi_listener: Option<Arc<Mutex<dyn IngestMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<IngestResult, IngestError>)> {
        let ids: Vec<_> = dataset_ids.map(|id| id.to_owned()).collect();
        let results = ids
            .iter()
            .map(|id| (id.clone(), Ok(IngestResult::UpToDate)))
            .collect();
        self.calls.push(ids);
        results
    }
}

pub struct TestTransformService {
    calls: Vec<Vec<DatasetIDBuf>>,
}

impl TestTransformService {
    pub fn new() -> Self {
        Self { calls: Vec::new() }
    }
}

impl TransformService for TestTransformService {
    fn transform(
        &mut self,
        _dataset_id: &DatasetID,
        _maybe_listener: Option<Arc<Mutex<dyn TransformListener>>>,
    ) -> Result<TransformResult, TransformError> {
        unimplemented!();
    }

    fn transform_multi(
        &mut self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        _maybe_multi_listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<TransformResult, TransformError>)> {
        let ids: Vec<_> = dataset_ids.map(|id| id.to_owned()).collect();
        let results = ids
            .iter()
            .map(|id| (id.clone(), Ok(TransformResult::UpToDate)))
            .collect();
        self.calls.push(ids);
        results
    }
}
