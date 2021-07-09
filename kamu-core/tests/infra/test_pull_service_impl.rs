use kamu::domain::*;
use kamu::infra::*;
use kamu_test::*;
use opendatafabric::*;

use chrono::prelude::*;
use itertools::Itertools;
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

fn id(s: &str) -> DatasetIDBuf {
    DatasetIDBuf::try_from(s).unwrap()
}

fn create_graph(
    repo: &MetadataRepositoryImpl,
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
    let repo = Arc::new(MetadataRepositoryImpl::new(
        &WorkspaceLayout::create(tmp_dir.path()).unwrap(),
    ));
    let test_ingest_svc = Arc::new(TestIngestService::new());
    let test_transform_svc = Arc::new(TestTransformService::new());
    let pull_svc = PullServiceImpl::new(
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
        repo.as_ref(),
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
        PullOptions::default(),
        None,
        None,
    );

    assert!(test_ingest_svc.calls.lock().unwrap().is_empty());

    assert_eq!(
        *test_transform_svc.calls.lock().unwrap(),
        vec![vec![id("e")]]
    );

    test_transform_svc.calls.lock().unwrap().clear();

    pull_svc.pull_multi(
        &mut [id("e")].iter().map(|id| id.as_ref()),
        PullOptions {
            recursive: true,
            ..PullOptions::default()
        },
        None,
        None,
    );

    assert_eq!(
        *test_ingest_svc.calls.lock().unwrap(),
        vec![vec![id("a"), id("b")]]
    );

    assert_eq!(
        *test_transform_svc.calls.lock().unwrap(),
        vec![vec![id("c"), id("d")], vec![id("e")]]
    );
}

#[test]
fn test_set_watermark() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = Arc::new(MetadataRepositoryImpl::new(
        &WorkspaceLayout::create(tmp_dir.path()).unwrap(),
    ));
    let test_ingest_svc = Arc::new(TestIngestService::new());
    let test_transform_svc = Arc::new(TestTransformService::new());
    let pull_svc = PullServiceImpl::new(
        repo.clone(),
        test_ingest_svc.clone(),
        test_transform_svc.clone(),
        slog::Logger::root(slog::Discard, slog::o!()),
    );

    let dataset_id = DatasetIDBuf::try_from("foo").unwrap();

    repo.add_dataset(MetadataFactory::dataset_snapshot().id(&dataset_id).build())
        .unwrap();

    let num_blocks = || {
        repo.get_metadata_chain(&dataset_id)
            .unwrap()
            .iter_blocks()
            .count()
    };
    assert_eq!(num_blocks(), 1);

    assert!(matches!(
        pull_svc.set_watermark(&dataset_id, Utc.ymd(2000, 1, 2).and_hms(0, 0, 0)),
        Ok(PullResult::Updated { .. })
    ));
    assert_eq!(num_blocks(), 2);

    assert!(matches!(
        pull_svc.set_watermark(&dataset_id, Utc.ymd(2000, 1, 3).and_hms(0, 0, 0)),
        Ok(PullResult::Updated { .. })
    ));
    assert_eq!(num_blocks(), 3);

    assert!(matches!(
        pull_svc.set_watermark(&dataset_id, Utc.ymd(2000, 1, 3).and_hms(0, 0, 0)),
        Ok(PullResult::UpToDate)
    ));
    assert_eq!(num_blocks(), 3);

    assert!(matches!(
        pull_svc.set_watermark(&dataset_id, Utc.ymd(2000, 1, 2).and_hms(0, 0, 0)),
        Ok(PullResult::UpToDate)
    ));
    assert_eq!(num_blocks(), 3);
}

pub struct TestIngestService {
    calls: Mutex<Vec<Vec<DatasetIDBuf>>>,
}

impl TestIngestService {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl IngestService for TestIngestService {
    fn ingest(
        &self,
        _dataset_id: &DatasetID,
        _ingest_options: IngestOptions,
        _maybe_listener: Option<Arc<Mutex<dyn IngestListener>>>,
    ) -> Result<IngestResult, IngestError> {
        unimplemented!();
    }

    fn ingest_multi(
        &self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        _ingest_options: IngestOptions,
        _maybe_multi_listener: Option<Arc<Mutex<dyn IngestMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<IngestResult, IngestError>)> {
        let ids: Vec<_> = dataset_ids.map(|id| id.to_owned()).collect();
        let results = ids
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    Ok(IngestResult::UpToDate { uncacheable: false }),
                )
            })
            .collect();
        self.calls.lock().unwrap().push(ids);
        results
    }
}

pub struct TestTransformService {
    calls: Mutex<Vec<Vec<DatasetIDBuf>>>,
}

impl TestTransformService {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl TransformService for TestTransformService {
    fn transform(
        &self,
        _dataset_id: &DatasetID,
        _maybe_listener: Option<Arc<Mutex<dyn TransformListener>>>,
    ) -> Result<TransformResult, TransformError> {
        unimplemented!();
    }

    fn transform_multi(
        &self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        _maybe_multi_listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<TransformResult, TransformError>)> {
        let ids: Vec<_> = dataset_ids.map(|id| id.to_owned()).collect();
        let results = ids
            .iter()
            .map(|id| (id.clone(), Ok(TransformResult::UpToDate)))
            .collect();
        self.calls.lock().unwrap().push(ids);
        results
    }
}
