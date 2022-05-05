// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;

use chrono::prelude::*;
use std::assert_matches::assert_matches;
use std::convert::TryFrom;
use std::path::Path;
use std::sync::{Arc, Mutex};

macro_rules! n {
    ($s:expr) => {
        DatasetName::try_from($s).unwrap()
    };
}

macro_rules! rl {
    ($s:expr) => {
        DatasetRefLocal::Name(DatasetName::try_from($s).unwrap())
    };
}

macro_rules! rr {
    ($s:expr) => {
        DatasetRefRemote::RemoteName(RemoteDatasetName::try_from($s).unwrap())
    };
}

macro_rules! ar {
    ($s:expr) => {
        match DatasetName::try_from($s) {
            Ok(n) => DatasetRefAny::Name(n),
            _ => DatasetRefAny::RemoteName(RemoteDatasetName::try_from($s).unwrap()),
        }
    };
}

macro_rules! names {
    [] => {
        vec![]
    };
    [$x:expr] => {
        vec![n!($x)]
    };
    [$x:expr, $($y:expr),+] => {
        vec![n!($x), $(n!($y)),+]
    };
}

macro_rules! refs {
    [] => {
        vec![]
    };
    [$x:expr] => {
        vec![ar!($x)]
    };
    [$x:expr, $($y:expr),+] => {
        vec![ar!($x), $(ar!($y)),+]
    };
}

fn create_graph(repo: &DatasetRegistryImpl, datasets: Vec<(DatasetName, Vec<DatasetName>)>) {
    for (dataset_name, deps) in &datasets {
        if deps.is_empty() {
            repo.add_dataset_from_blocks(
                dataset_name,
                &mut [
                    MetadataFactory::metadata_block(
                        MetadataFactory::seed(DatasetKind::Root)
                            .id_from(dataset_name.as_str())
                            .build(),
                    )
                    .build(),
                    MetadataFactory::metadata_block(MetadataFactory::set_polling_source().build())
                        .build(),
                ]
                .into_iter(),
            )
            .unwrap();
        } else {
            repo.add_dataset_from_blocks(
                dataset_name,
                &mut [
                    MetadataFactory::metadata_block(
                        MetadataFactory::seed(DatasetKind::Derivative)
                            .id_from(dataset_name.as_str())
                            .build(),
                    )
                    .build(),
                    MetadataFactory::metadata_block(
                        MetadataFactory::set_transform(deps)
                            .input_ids_from_names()
                            .build(),
                    )
                    .build(),
                ]
                .into_iter(),
            )
            .unwrap();
        }
    }
}

// TODO: Rewrite this abomination
async fn create_graph_in_repository(
    repo_path: &Path,
    datasets: Vec<(DatasetName, Vec<DatasetName>)>,
) {
    for (dataset_name, deps) in &datasets {
        let ds = DatasetRepoFactory::create_local_fs(repo_path.join(&dataset_name)).unwrap();
        let chain = ds.as_metadata_chain();

        if deps.is_empty() {
            let head = chain
                .append(
                    MetadataFactory::metadata_block(
                        MetadataFactory::seed(DatasetKind::Root)
                            .id_from(dataset_name.as_str())
                            .build(),
                    )
                    .build(),
                    AppendOpts::default(),
                )
                .await
                .unwrap();
            chain
                .append(
                    MetadataFactory::metadata_block(MetadataFactory::set_polling_source().build())
                        .prev(&head)
                        .build(),
                    AppendOpts::default(),
                )
                .await
                .unwrap();
        } else {
            let head = chain
                .append(
                    MetadataFactory::metadata_block(
                        MetadataFactory::seed(DatasetKind::Derivative)
                            .id_from(dataset_name.as_str())
                            .build(),
                    )
                    .build(),
                    AppendOpts::default(),
                )
                .await
                .unwrap();

            chain
                .append(
                    MetadataFactory::metadata_block(
                        MetadataFactory::set_transform(deps)
                            .input_ids_from_names()
                            .build(),
                    )
                    .prev(&head)
                    .build(),
                    AppendOpts::default(),
                )
                .await
                .unwrap();
        }
    }
}

// Adding a remote dataset is a bit of a pain.
// We cannot add a local dataset and then add a pull alias without adding all of its dependencies too.
// So instead we're creating a repository based on temp dir and syncing it into the main workspace.
// TODO: Add simpler way to import remote dataset
async fn create_graph_remote(
    ws: Arc<WorkspaceLayout>,
    reg: Arc<RemoteRepositoryRegistryImpl>,
    datasets: Vec<(DatasetName, Vec<DatasetName>)>,
    to_import: Vec<DatasetName>,
) {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    create_graph_in_repository(tmp_repo_dir.path(), datasets).await;

    let tmp_repo_name = RepositoryName::new_unchecked("tmp");

    reg.add_repository(
        &tmp_repo_name,
        url::Url::from_file_path(tmp_repo_dir.path()).unwrap(),
    )
    .unwrap();

    let sync_service = SyncServiceImpl::new(
        reg.clone(),
        Arc::new(LocalDatasetRepositoryImpl::new(ws.clone())),
    );

    for name in &to_import {
        sync_service
            .sync(
                &RemoteDatasetName::new(tmp_repo_name.clone(), None, name.clone()).into(),
                &name.into(),
                SyncOptions::default(),
                None,
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_pull_batching_chain() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path());

    // A - B - C
    create_graph(
        harness.dataset_reg.as_ref(),
        vec![
            (n!("a"), names![]),
            (n!("b"), names!["a"]),
            (n!("c"), names!["b"]),
        ],
    );

    assert_eq!(
        harness.pull(refs!["c"], PullOptions::default()).await,
        vec![PullBatch::Transform(refs!["c"])]
    );

    assert_eq!(
        harness.pull(refs!["c", "a"], PullOptions::default()).await,
        vec![
            PullBatch::Ingest(refs!["a"]),
            PullBatch::Transform(refs!["c"])
        ],
    );

    assert_eq!(
        harness
            .pull(
                refs!["c"],
                PullOptions {
                    recursive: true,
                    ..PullOptions::default()
                }
            )
            .await,
        vec![
            PullBatch::Ingest(refs!["a"]),
            PullBatch::Transform(refs!["b"]),
            PullBatch::Transform(refs!["c"])
        ]
    );
}

#[tokio::test]
async fn test_pull_batching_complex() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path());

    //    / C \
    // A <     > > E
    //    \ D / /
    //         /
    // B - - -/
    create_graph(
        harness.dataset_reg.as_ref(),
        vec![
            (n!("a"), names![]),
            (n!("b"), names![]),
            (n!("c"), names!["a"]),
            (n!("d"), names!["a"]),
            (n!("e"), names!["c", "d", "b"]),
        ],
    );

    assert_eq!(
        harness.pull(refs!["e"], PullOptions::default()).await,
        vec![PullBatch::Transform(refs!["e"])]
    );

    assert_eq!(
        harness
            .pull(
                refs!["e"],
                PullOptions {
                    recursive: true,
                    ..PullOptions::default()
                }
            )
            .await,
        vec![
            PullBatch::Ingest(refs!["a", "b"]),
            PullBatch::Transform(refs!["c", "d"]),
            PullBatch::Transform(refs!["e"]),
        ]
    );
}

#[tokio::test]
async fn test_pull_batching_complex_with_remote() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path());

    // (A) - (E) - F - G
    // (B) --/    /   /
    // C --------/   /
    // D -----------/
    create_graph_remote(
        harness.workspace_layout.clone(),
        harness.remote_repo_reg.clone(),
        vec![
            (n!("a"), names![]),
            (n!("b"), names![]),
            (n!("e"), names!["a", "b"]),
        ],
        names!("e"),
    )
    .await;
    create_graph(
        harness.dataset_reg.as_ref(),
        vec![
            (n!("c"), names![]),
            (n!("d"), names![]),
            (n!("f"), names!["e", "c"]),
            (n!("g"), names!["f", "d"]),
        ],
    );

    // Add remote pull alias to E
    harness
        .remote_alias_reg
        .get_remote_aliases(&rl!("e"))
        .unwrap()
        .add(
            &RemoteDatasetName::try_from("kamu.dev/anonymous/e").unwrap(),
            RemoteAliasKind::Pull,
        )
        .unwrap();

    // Pulling E results in a sync
    assert_eq!(
        harness
            .pull(
                refs!["e"],
                PullOptions {
                    recursive: true,
                    ..PullOptions::default()
                }
            )
            .await,
        vec![PullBatch::Sync(vec![(
            rr!("kamu.dev/anonymous/e").into(),
            n!("e").into()
        )])],
    );

    // Explicit remote reference associates with E
    assert_eq!(
        harness
            .pull(
                refs!["kamu.dev/anonymous/e"],
                PullOptions {
                    recursive: true,
                    ..PullOptions::default()
                }
            )
            .await,
        vec![PullBatch::Sync(vec![(
            rr!("kamu.dev/anonymous/e").into(),
            n!("e").into()
        )])],
    );

    // Remote is recursed onto
    assert_eq!(
        harness
            .pull(
                refs!["g"],
                PullOptions {
                    recursive: true,
                    ..PullOptions::default()
                }
            )
            .await,
        vec![
            PullBatch::Sync(vec![(rr!("kamu.dev/anonymous/e").into(), n!("e").into())]),
            PullBatch::Ingest(refs!("c", "d")),
            PullBatch::Transform(refs!("f")),
            PullBatch::Transform(refs!("g")),
        ],
    );

    // Remote is recursed onto while also specified explicitly (via local ID)
    assert_eq!(
        harness
            .pull(
                refs!["g", "e"],
                PullOptions {
                    recursive: true,
                    ..PullOptions::default()
                }
            )
            .await,
        vec![
            PullBatch::Sync(vec![(rr!("kamu.dev/anonymous/e").into(), n!("e").into())]),
            PullBatch::Ingest(refs!("c", "d")),
            PullBatch::Transform(refs!("f")),
            PullBatch::Transform(refs!("g")),
        ],
    );

    // Remote is recursed onto while also specified explicitly (via remote ref)
    assert_eq!(
        harness
            .pull(
                refs!["g", "kamu.dev/anonymous/e"],
                PullOptions {
                    recursive: true,
                    ..PullOptions::default()
                }
            )
            .await,
        vec![
            PullBatch::Sync(vec![(rr!("kamu.dev/anonymous/e").into(), n!("e").into())]),
            PullBatch::Ingest(refs!("c", "d")),
            PullBatch::Transform(refs!("f")),
            PullBatch::Transform(refs!("g")),
        ],
    );
}

#[tokio::test]
async fn test_sync_from() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path());

    harness
        .remote_repo_reg
        .add_repository(
            &RepositoryName::new_unchecked("myrepo"),
            url::Url::parse("file:///tmp/nowhere").unwrap(),
        )
        .unwrap();

    let res = harness
        .pull_svc
        .sync_from(&rr!("myrepo/foo"), &n!("bar"), PullOptions::default(), None)
        .await;

    assert_matches!(
        res,
        Ok(PullResult::Updated {
            old_head: None,
            new_head: _,
            num_blocks: 1,
        })
    );

    let aliases = harness
        .remote_alias_reg
        .get_remote_aliases(&rl!("bar"))
        .unwrap();
    let pull_aliases: Vec<_> = aliases
        .get_by_kind(RemoteAliasKind::Pull)
        .map(|i| i.clone())
        .collect();

    assert_eq!(
        pull_aliases,
        vec![RemoteDatasetName::try_from("myrepo/foo").unwrap()]
    );
}

#[tokio::test]
async fn test_set_watermark() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path());

    let dataset_name = n!("foo");

    harness
        .dataset_reg
        .add_dataset(
            MetadataFactory::dataset_snapshot()
                .name(&dataset_name)
                .build(),
        )
        .unwrap();

    let num_blocks = || {
        harness
            .dataset_reg
            .get_metadata_chain(&dataset_name.as_local_ref())
            .unwrap()
            .iter_blocks()
            .count()
    };
    assert_eq!(num_blocks(), 1);

    assert!(matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_name.as_local_ref(),
                Utc.ymd(2000, 1, 2).and_hms(0, 0, 0)
            )
            .await,
        Ok(PullResult::Updated { .. })
    ));
    assert_eq!(num_blocks(), 2);

    assert!(matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_name.as_local_ref(),
                Utc.ymd(2000, 1, 3).and_hms(0, 0, 0)
            )
            .await,
        Ok(PullResult::Updated { .. })
    ));
    assert_eq!(num_blocks(), 3);

    assert!(matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_name.as_local_ref(),
                Utc.ymd(2000, 1, 3).and_hms(0, 0, 0)
            )
            .await,
        Ok(PullResult::UpToDate)
    ));
    assert_eq!(num_blocks(), 3);

    assert!(matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_name.as_local_ref(),
                Utc.ymd(2000, 1, 2).and_hms(0, 0, 0)
            )
            .await,
        Ok(PullResult::UpToDate)
    ));
    assert_eq!(num_blocks(), 3);
}

/////////////////////////////////////////////////////////////////////////////////////////

struct PullTestHarness {
    calls: Arc<Mutex<Vec<PullBatch>>>,
    workspace_layout: Arc<WorkspaceLayout>,
    dataset_reg: Arc<DatasetRegistryImpl>,
    remote_repo_reg: Arc<RemoteRepositoryRegistryImpl>,
    remote_alias_reg: Arc<RemoteAliasesRegistryImpl>,
    pull_svc: PullServiceImpl,
}

impl PullTestHarness {
    fn new(tmp_path: &Path) -> Self {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let workspace_layout = Arc::new(WorkspaceLayout::create(tmp_path).unwrap());
        let dataset_reg = Arc::new(DatasetRegistryImpl::new(workspace_layout.clone()));
        let remote_repo_reg = Arc::new(RemoteRepositoryRegistryImpl::new(workspace_layout.clone()));
        let remote_alias_reg = Arc::new(RemoteAliasesRegistryImpl::new(
            dataset_reg.clone(),
            workspace_layout.clone(),
        ));
        let ingest_svc = Arc::new(TestIngestService::new(calls.clone()));
        let transform_svc = Arc::new(TestTransformService::new(calls.clone()));
        let sync_svc = Arc::new(TestSyncService::new(calls.clone(), dataset_reg.clone()));
        let pull_svc = PullServiceImpl::new(
            dataset_reg.clone(),
            remote_alias_reg.clone(),
            ingest_svc,
            transform_svc,
            sync_svc,
        );

        Self {
            calls,
            workspace_layout,
            dataset_reg,
            remote_repo_reg,
            remote_alias_reg,
            pull_svc,
        }
    }

    fn collect_calls(&self) -> Vec<PullBatch> {
        let mut calls = Vec::new();
        std::mem::swap(self.calls.lock().unwrap().as_mut(), &mut calls);
        calls
    }

    async fn pull(&self, refs: Vec<DatasetRefAny>, options: PullOptions) -> Vec<PullBatch> {
        self.pull_svc
            .pull_multi(&mut refs.into_iter(), options, None, None, None)
            .await;
        self.collect_calls()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullBatch {
    Ingest(Vec<DatasetRefAny>),
    Transform(Vec<DatasetRefAny>),
    Sync(Vec<(DatasetRefAny, DatasetRefAny)>),
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

#[async_trait::async_trait(?Send)]
impl IngestService for TestIngestService {
    async fn ingest(
        &self,
        _dataset_ref: &DatasetRefLocal,
        _ingest_options: IngestOptions,
        _maybe_listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        unimplemented!();
    }

    async fn ingest_from(
        &self,
        _dataset_ref: &DatasetRefLocal,
        _fetch: FetchStep,
        _options: IngestOptions,
        _listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        unimplemented!()
    }

    async fn ingest_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefLocal>,
        _ingest_options: IngestOptions,
        _maybe_multi_listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetRefLocal, Result<IngestResult, IngestError>)> {
        let dataset_refs: Vec<_> = dataset_refs.collect();
        let results = dataset_refs
            .iter()
            .map(|r| {
                (
                    r.clone(),
                    Ok(IngestResult::UpToDate {
                        uncacheable: false,
                        has_more: false,
                    }),
                )
            })
            .collect();
        self.calls.lock().unwrap().push(PullBatch::Ingest(
            dataset_refs.into_iter().map(|i| i.into()).collect(),
        ));
        results
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct TestTransformService {
    calls: Arc<Mutex<Vec<PullBatch>>>,
}

impl TestTransformService {
    pub fn new(calls: Arc<Mutex<Vec<PullBatch>>>) -> Self {
        Self { calls }
    }
}

#[async_trait::async_trait(?Send)]
impl TransformService for TestTransformService {
    async fn transform(
        &self,
        _dataset_ref: &DatasetRefLocal,
        _maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError> {
        unimplemented!();
    }

    async fn transform_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefLocal>,
        _maybe_multi_listener: Option<Arc<dyn TransformMultiListener>>,
    ) -> Vec<(DatasetRefLocal, Result<TransformResult, TransformError>)> {
        let dataset_refs: Vec<_> = dataset_refs.collect();
        let results = dataset_refs
            .iter()
            .map(|r| (r.clone(), Ok(TransformResult::UpToDate)))
            .collect();
        self.calls.lock().unwrap().push(PullBatch::Transform(
            dataset_refs.into_iter().map(|i| i.into()).collect(),
        ));
        results
    }

    async fn verify_transform(
        &self,
        _dataset_ref: &DatasetRefLocal,
        _block_range: (Option<Multihash>, Option<Multihash>),
        _options: VerificationOptions,
        _listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        unimplemented!()
    }

    async fn verify_transform_multi(
        &self,
        _datasets: &mut dyn Iterator<Item = VerificationRequest>,
        _options: VerificationOptions,
        _listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        unimplemented!()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct TestSyncService {
    calls: Arc<Mutex<Vec<PullBatch>>>,
    dataset_reg: Arc<dyn DatasetRegistry>,
}

impl TestSyncService {
    fn new(calls: Arc<Mutex<Vec<PullBatch>>>, dataset_reg: Arc<dyn DatasetRegistry>) -> Self {
        Self { calls, dataset_reg }
    }
}

#[async_trait::async_trait(?Send)]
impl SyncService for TestSyncService {
    async fn sync(
        &self,
        _src: &DatasetRefAny,
        dst: &DatasetRefAny,
        _options: SyncOptions,
        _listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let local_name = match dst {
            DatasetRefAny::Name(name) => name,
            DatasetRefAny::Handle(DatasetHandle { name, .. }) => name,
            _ => unreachable!(),
        };
        self.dataset_reg
            .add_dataset(MetadataFactory::dataset_snapshot().name(local_name).build())
            .unwrap();

        Ok(SyncResult::Updated {
            old_head: None,
            new_head: Multihash::from_digest_sha3_256(b"boop"),
            num_blocks: 1,
        })
    }

    async fn sync_multi(
        &self,
        src_dst: &mut dyn Iterator<Item = (DatasetRefAny, DatasetRefAny)>,
        _options: SyncOptions,
        _listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<SyncResultMulti> {
        let mut call = Vec::new();
        let mut results = Vec::new();
        for (src, dst) in src_dst {
            call.push((src.clone(), dst.clone()));
            results.push(SyncResultMulti {
                src,
                dst,
                result: Ok(SyncResult::UpToDate),
            });
        }
        self.calls.lock().unwrap().push(PullBatch::Sync(call));
        results
    }
}
