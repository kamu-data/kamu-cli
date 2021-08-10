use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;

use chrono::prelude::*;
use std::assert_matches::assert_matches;
use std::convert::TryFrom;
use std::path::Path;
use std::sync::{Arc, Mutex};

macro_rules! id {
    ($s:expr) => {
        DatasetIDBuf::new_unchecked($s)
    };
}

macro_rules! r {
    ($s:expr) => {
        DatasetRefBuf::new_unchecked($s)
    };
}

macro_rules! refs {
    [] => {
        vec![]
    };
    [$x:expr] => {
        vec![r!($x)]
    };
    [$x:expr, $($y:expr),+] => {
        vec![r!($x), $(r!($y)),+]
    };
}

fn create_graph(repo: &MetadataRepositoryImpl, datasets: Vec<(DatasetRefBuf, Vec<DatasetRefBuf>)>) {
    for (dataset_ref, deps) in &datasets {
        if deps.is_empty() {
            repo.add_dataset(MetadataFactory::dataset_snapshot().id(dataset_ref).build())
                .unwrap();
        } else {
            repo.add_dataset(
                MetadataFactory::dataset_snapshot()
                    .id(dataset_ref)
                    .source(MetadataFactory::dataset_source_deriv(deps.iter()).build())
                    .build(),
            )
            .unwrap();
        }
    }
}

fn create_graph_in_repository(
    repo_path: &Path,
    datasets: Vec<(DatasetRefBuf, Vec<DatasetRefBuf>)>,
) {
    let repo_factory = RepositoryFactoryFS::new(repo_path);

    for (dataset_ref, deps) in &datasets {
        let ds_builder = repo_factory
            .new_dataset()
            .id(dataset_ref.as_local().unwrap());

        if deps.is_empty() {
            ds_builder.append(
                MetadataFactory::metadata_block()
                    .source(MetadataFactory::dataset_source_root().build())
                    .build(),
            );
        } else {
            ds_builder.append(
                MetadataFactory::metadata_block()
                    .source(MetadataFactory::dataset_source_deriv(deps.iter()).build())
                    .build(),
            );
        }
    }
}

// Adding a remote dataset is a bit of a pain.
// We cannot add a local dataset and then add a pull alias without adding all of its dependencies too.
// So instead we're creating a repository based on temp dir and syncing it into the main workspace.
// TODO: Add simpler way to import remote dataset
fn create_graph_remote(
    ws: Arc<WorkspaceLayout>,
    repo: Arc<MetadataRepositoryImpl>,
    datasets: Vec<(DatasetRefBuf, Vec<DatasetRefBuf>)>,
    to_import: Vec<DatasetRefBuf>,
) {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    create_graph_in_repository(tmp_repo_dir.path(), datasets);

    let tmp_repo_id = RepositoryID::new_unchecked("tmp");

    repo.add_repository(
        tmp_repo_id,
        url::Url::from_file_path(tmp_repo_dir.path()).unwrap(),
    )
    .unwrap();

    let sync_service = SyncServiceImpl::new(
        ws,
        repo.clone(),
        Arc::new(RepositoryFactory::new(slog::Logger::root(
            slog::Discard,
            slog::o!(),
        ))),
        slog::Logger::root(slog::Discard, slog::o!()),
    );

    for r in &to_import {
        sync_service
            .sync_from(
                &DatasetRefBuf::new(Some(tmp_repo_id), None, r.as_local().unwrap()),
                r.as_local().unwrap(),
                SyncOptions::default(),
                None,
            )
            .unwrap();
    }
}

#[test]
fn test_pull_batching_chain() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path());

    // A - B - C
    create_graph(
        harness.metadata_repo.as_ref(),
        vec![
            (r!("a"), refs![]),
            (r!("b"), refs!["a"]),
            (r!("c"), refs!["b"]),
        ],
    );

    assert_eq!(
        harness.pull(refs!["c"], PullOptions::default()),
        vec![PullBatch::Transform(refs!["c"])]
    );

    assert_eq!(
        harness.pull(refs!["c", "a"], PullOptions::default()),
        vec![
            PullBatch::Ingest(refs!["a"]),
            PullBatch::Transform(refs!["c"])
        ],
    );

    assert_eq!(
        harness.pull(
            refs!["c"],
            PullOptions {
                recursive: true,
                ..PullOptions::default()
            }
        ),
        vec![
            PullBatch::Ingest(refs!["a"]),
            PullBatch::Transform(refs!["b"]),
            PullBatch::Transform(refs!["c"])
        ]
    );
}

#[test]
fn test_pull_batching_complex() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path());

    //    / C \
    // A <     > > E
    //    \ D / /
    //         /
    // B - - -/
    create_graph(
        harness.metadata_repo.as_ref(),
        vec![
            (r!("a"), refs![]),
            (r!("b"), refs![]),
            (r!("c"), refs!["a"]),
            (r!("d"), refs!["a"]),
            (r!("e"), refs!["c", "d", "b"]),
        ],
    );

    assert_eq!(
        harness.pull(refs!["e"], PullOptions::default()),
        vec![PullBatch::Transform(refs!["e"])]
    );

    assert_eq!(
        harness.pull(
            refs!["e"],
            PullOptions {
                recursive: true,
                ..PullOptions::default()
            }
        ),
        vec![
            PullBatch::Ingest(refs!["a", "b"]),
            PullBatch::Transform(refs!["c", "d"]),
            PullBatch::Transform(refs!["e"]),
        ]
    );
}

#[test]
fn test_pull_batching_complex_with_remote() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path());

    // (A) - (E) - F - G
    // (B) --/    /   /
    // C --------/   /
    // D -----------/
    create_graph_remote(
        harness.workspace_layout.clone(),
        harness.metadata_repo.clone(),
        vec![
            (r!("a"), refs![]),
            (r!("b"), refs![]),
            (r!("e"), refs!["a", "b"]),
        ],
        refs!("e"),
    );
    create_graph(
        harness.metadata_repo.as_ref(),
        vec![
            (r!("c"), refs![]),
            (r!("d"), refs![]),
            (r!("f"), refs!["e", "c"]),
            (r!("g"), refs!["f", "d"]),
        ],
    );

    // Add remote pull alias to E
    harness
        .metadata_repo
        .get_remote_aliases(&id!("e"))
        .unwrap()
        .add(r!("kamu.dev/anonymous/e"), RemoteAliasKind::Pull)
        .unwrap();

    // Pulling E results in a sync
    assert_eq!(
        harness.pull(
            refs!["e"],
            PullOptions {
                recursive: true,
                ..PullOptions::default()
            }
        ),
        vec![PullBatch::Sync(vec![(
            r!("kamu.dev/anonymous/e"),
            id!("e")
        )])],
    );

    // Explicit remote reference associates with E
    assert_eq!(
        harness.pull(
            refs!["kamu.dev/anonymous/e"],
            PullOptions {
                recursive: true,
                ..PullOptions::default()
            }
        ),
        vec![PullBatch::Sync(vec![(
            r!("kamu.dev/anonymous/e"),
            id!("e")
        )])],
    );

    // Remote is recursed onto
    assert_eq!(
        harness.pull(
            refs!["g"],
            PullOptions {
                recursive: true,
                ..PullOptions::default()
            }
        ),
        vec![
            PullBatch::Sync(vec![(r!("kamu.dev/anonymous/e"), id!("e"))]),
            PullBatch::Ingest(refs!("c", "d")),
            PullBatch::Transform(refs!("f")),
            PullBatch::Transform(refs!("g")),
        ],
    );

    // Remote is recursed onto while also specified explicitly (via local ID)
    assert_eq!(
        harness.pull(
            refs!["g", "e"],
            PullOptions {
                recursive: true,
                ..PullOptions::default()
            }
        ),
        vec![
            PullBatch::Sync(vec![(r!("kamu.dev/anonymous/e"), id!("e"))]),
            PullBatch::Ingest(refs!("c", "d")),
            PullBatch::Transform(refs!("f")),
            PullBatch::Transform(refs!("g")),
        ],
    );

    // Remote is recursed onto while also specified explicitly (via remote ref)
    assert_eq!(
        harness.pull(
            refs!["g", "kamu.dev/anonymous/e"],
            PullOptions {
                recursive: true,
                ..PullOptions::default()
            }
        ),
        vec![
            PullBatch::Sync(vec![(r!("kamu.dev/anonymous/e"), id!("e"))]),
            PullBatch::Ingest(refs!("c", "d")),
            PullBatch::Transform(refs!("f")),
            PullBatch::Transform(refs!("g")),
        ],
    );
}

#[test]
fn test_sync_from() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path());

    harness
        .metadata_repo
        .add_repository(
            RepositoryID::new_unchecked("myrepo"),
            url::Url::parse("file:///tmp/nowhere").unwrap(),
        )
        .unwrap();

    let res =
        harness
            .pull_svc
            .sync_from(&r!("myrepo/foo"), &id!("bar"), PullOptions::default(), None);

    assert_matches!(
        res,
        Ok(PullResult::Updated {
            old_head: None,
            new_head: _,
            num_blocks: 1,
        })
    );

    let aliases = harness
        .metadata_repo
        .get_remote_aliases(&id!("bar"))
        .unwrap();
    let pull_aliases: Vec<_> = aliases.get_by_kind(RemoteAliasKind::Pull).collect();

    assert_eq!(pull_aliases, vec![DatasetRef::new_unchecked("myrepo/foo")]);
}

#[test]
fn test_set_watermark() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path());

    let dataset_id = DatasetIDBuf::try_from("foo").unwrap();

    harness
        .metadata_repo
        .add_dataset(MetadataFactory::dataset_snapshot().id(&dataset_id).build())
        .unwrap();

    let num_blocks = || {
        harness
            .metadata_repo
            .get_metadata_chain(&dataset_id)
            .unwrap()
            .iter_blocks()
            .count()
    };
    assert_eq!(num_blocks(), 1);

    assert!(matches!(
        harness
            .pull_svc
            .set_watermark(&dataset_id, Utc.ymd(2000, 1, 2).and_hms(0, 0, 0)),
        Ok(PullResult::Updated { .. })
    ));
    assert_eq!(num_blocks(), 2);

    assert!(matches!(
        harness
            .pull_svc
            .set_watermark(&dataset_id, Utc.ymd(2000, 1, 3).and_hms(0, 0, 0)),
        Ok(PullResult::Updated { .. })
    ));
    assert_eq!(num_blocks(), 3);

    assert!(matches!(
        harness
            .pull_svc
            .set_watermark(&dataset_id, Utc.ymd(2000, 1, 3).and_hms(0, 0, 0)),
        Ok(PullResult::UpToDate)
    ));
    assert_eq!(num_blocks(), 3);

    assert!(matches!(
        harness
            .pull_svc
            .set_watermark(&dataset_id, Utc.ymd(2000, 1, 2).and_hms(0, 0, 0)),
        Ok(PullResult::UpToDate)
    ));
    assert_eq!(num_blocks(), 3);
}

/////////////////////////////////////////////////////////////////////////////////////////

struct PullTestHarness {
    calls: Arc<Mutex<Vec<PullBatch>>>,
    workspace_layout: Arc<WorkspaceLayout>,
    metadata_repo: Arc<MetadataRepositoryImpl>,
    pull_svc: PullServiceImpl,
}

impl PullTestHarness {
    fn new(tmp_path: &Path) -> Self {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let workspace_layout = Arc::new(WorkspaceLayout::create(tmp_path).unwrap());
        let metadata_repo = Arc::new(MetadataRepositoryImpl::new(workspace_layout.clone()));
        let ingest_svc = Arc::new(TestIngestService::new(calls.clone()));
        let transform_svc = Arc::new(TestTransformService::new(calls.clone()));
        let sync_svc = Arc::new(TestSyncService::new(calls.clone(), metadata_repo.clone()));
        let pull_svc = PullServiceImpl::new(
            metadata_repo.clone(),
            ingest_svc,
            transform_svc,
            sync_svc,
            slog::Logger::root(slog::Discard, slog::o!()),
        );

        Self {
            calls,
            workspace_layout,
            metadata_repo,
            pull_svc,
        }
    }

    fn collect_calls(&self) -> Vec<PullBatch> {
        let mut calls = Vec::new();
        std::mem::swap(self.calls.lock().unwrap().as_mut(), &mut calls);
        calls
    }

    fn pull(&self, refs: Vec<DatasetRefBuf>, options: PullOptions) -> Vec<PullBatch> {
        self.pull_svc.pull_multi(
            &mut refs.iter().map(|r| r.as_ref()),
            options,
            None,
            None,
            None,
        );
        self.collect_calls()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PullBatch {
    Ingest(Vec<DatasetRefBuf>),
    Transform(Vec<DatasetRefBuf>),
    Sync(Vec<(DatasetRefBuf, DatasetIDBuf)>),
}

/////////////////////////////////////////////////////////////////////////////////////////

struct TestIngestService {
    calls: Arc<Mutex<Vec<PullBatch>>>,
}

impl TestIngestService {
    fn new(calls: Arc<Mutex<Vec<PullBatch>>>) -> Self {
        Self { calls }
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

    fn ingest_from(
        &self,
        _dataset_id: &DatasetID,
        _fetch: FetchStep,
        _options: IngestOptions,
        _listener: Option<Arc<Mutex<dyn IngestListener>>>,
    ) -> Result<IngestResult, IngestError> {
        unimplemented!()
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
        self.calls.lock().unwrap().push(PullBatch::Ingest(
            ids.into_iter().map(|id| id.into()).collect(),
        ));
        results
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct TestTransformService {
    calls: Arc<Mutex<Vec<PullBatch>>>,
}

impl TestTransformService {
    fn new(calls: Arc<Mutex<Vec<PullBatch>>>) -> Self {
        Self { calls }
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
        self.calls.lock().unwrap().push(PullBatch::Transform(
            ids.into_iter().map(|id| id.into()).collect(),
        ));
        results
    }

    fn verify(
        &self,
        _dataset_id: &DatasetID,
        _blocks_range: (Option<Sha3_256>, Option<Sha3_256>),
        _options: VerificationOptions,
        _listener: Option<Arc<Mutex<dyn TransformListener>>>,
    ) -> Result<VerificationResult, VerificationError> {
        unimplemented!()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct TestSyncService {
    calls: Arc<Mutex<Vec<PullBatch>>>,
    metadata_repo: Arc<dyn MetadataRepository>,
}

impl TestSyncService {
    fn new(calls: Arc<Mutex<Vec<PullBatch>>>, metadata_repo: Arc<dyn MetadataRepository>) -> Self {
        Self {
            calls,
            metadata_repo,
        }
    }
}

impl SyncService for TestSyncService {
    fn sync_from(
        &self,
        _remote_dataset_ref: &DatasetRef,
        local_dataset_id: &DatasetID,
        _options: SyncOptions,
        _listener: Option<Arc<Mutex<dyn SyncListener>>>,
    ) -> Result<SyncResult, SyncError> {
        self.metadata_repo
            .add_dataset(
                MetadataFactory::dataset_snapshot()
                    .id(local_dataset_id)
                    .build(),
            )
            .unwrap();
        Ok(SyncResult::Updated {
            old_head: None,
            new_head: Sha3_256::zero(),
            num_blocks: 1,
        })
    }

    fn sync_from_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (&DatasetRef, &DatasetID)>,
        _options: SyncOptions,
        _listener: Option<Arc<Mutex<dyn SyncMultiListener>>>,
    ) -> Vec<((DatasetRefBuf, DatasetIDBuf), Result<SyncResult, SyncError>)> {
        let mut call = Vec::new();
        let mut results = Vec::new();
        for (rem, loc) in datasets {
            call.push((rem.to_owned(), loc.to_owned()));
            results.push(((rem.to_owned(), loc.to_owned()), Ok(SyncResult::UpToDate)));
        }
        self.calls.lock().unwrap().push(PullBatch::Sync(call));
        results
    }

    fn sync_to(
        &self,
        _local_dataset_id: &DatasetID,
        _remote_dataset_ref: &DatasetRef,
        _options: SyncOptions,
        _listener: Option<Arc<Mutex<dyn SyncListener>>>,
    ) -> Result<SyncResult, SyncError> {
        unimplemented!()
    }

    fn sync_to_multi(
        &self,
        _datasets: &mut dyn Iterator<Item = (&DatasetID, &DatasetRef)>,
        _options: SyncOptions,
        _listener: Option<Arc<Mutex<dyn SyncMultiListener>>>,
    ) -> Vec<((DatasetIDBuf, DatasetRefBuf), Result<SyncResult, SyncError>)> {
        unimplemented!()
    }
}
