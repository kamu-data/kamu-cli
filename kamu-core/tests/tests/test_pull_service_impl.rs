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

use crate::utils::DummySmartTransferProtocolClient;

macro_rules! n {
    ($s:expr) => {
        DatasetAlias::new(None, DatasetName::try_from($s).unwrap())
    };
}

macro_rules! rl {
    ($s:expr) => {
        DatasetRef::Alias(DatasetAlias::new(None, DatasetName::try_from($s).unwrap()))
    };
}

macro_rules! rr {
    ($s:expr) => {
        DatasetRefRemote::try_from($s).unwrap()
    };
}

macro_rules! ar {
    ($s:expr) => {
        DatasetRefAny::try_from($s).unwrap()
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

async fn create_graph(
    repo: &DatasetRepositoryLocalFs,
    datasets: Vec<(DatasetAlias, Vec<DatasetAlias>)>,
) {
    for (dataset_alias, deps) in datasets {
        let dataset = repo
            .create_dataset(
                &dataset_alias,
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(if deps.is_empty() {
                        DatasetKind::Root
                    } else {
                        DatasetKind::Derivative
                    })
                    .id_from(dataset_alias.dataset_name.as_str())
                    .build(),
                )
                .build(),
            )
            .await
            .unwrap()
            .dataset;

        if deps.is_empty() {
            dataset
                .commit_event(
                    MetadataEvent::SetPollingSource(MetadataFactory::set_polling_source().build()),
                    CommitOpts::default(),
                )
                .await
                .unwrap();
        } else {
            dataset
                .commit_event(
                    MetadataEvent::SetTransform(
                        MetadataFactory::set_transform(deps.into_iter().map(|d| d.dataset_name))
                            .input_ids_from_names()
                            .build(),
                    ),
                    CommitOpts::default(),
                )
                .await
                .unwrap();
        }
    }
}

// TODO: Rewrite this abomination
async fn create_graph_in_repository(
    repo_path: &Path,
    datasets: Vec<(DatasetAlias, Vec<DatasetAlias>)>,
) {
    for (dataset_alias, deps) in datasets {
        let layout = DatasetLayout::create(repo_path.join(&dataset_alias.dataset_name)).unwrap();
        let ds = DatasetFactoryImpl::get_local_fs(layout);
        let chain = ds.as_metadata_chain();

        if deps.is_empty() {
            let seed_block = MetadataFactory::metadata_block(
                MetadataFactory::seed(DatasetKind::Root)
                    .id_from(dataset_alias.dataset_name.as_str())
                    .build(),
            )
            .build();
            let seed_block_sequence_number = seed_block.sequence_number;

            let head = chain
                .append(seed_block, AppendOpts::default())
                .await
                .unwrap();
            chain
                .append(
                    MetadataFactory::metadata_block(MetadataFactory::set_polling_source().build())
                        .prev(&head, seed_block_sequence_number)
                        .build(),
                    AppendOpts::default(),
                )
                .await
                .unwrap();
        } else {
            let seed_block = MetadataFactory::metadata_block(
                MetadataFactory::seed(DatasetKind::Derivative)
                    .id_from(dataset_alias.dataset_name.as_str())
                    .build(),
            )
            .build();
            let seed_block_sequence_number = seed_block.sequence_number;

            let head = chain
                .append(seed_block, AppendOpts::default())
                .await
                .unwrap();

            chain
                .append(
                    MetadataFactory::metadata_block(
                        MetadataFactory::set_transform(deps.into_iter().map(|d| d.dataset_name))
                            .input_ids_from_names()
                            .build(),
                    )
                    .prev(&head, seed_block_sequence_number)
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
    datasets: Vec<(DatasetAlias, Vec<DatasetAlias>)>,
    to_import: Vec<DatasetAlias>,
) {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    create_graph_in_repository(tmp_repo_dir.path(), datasets).await;

    let tmp_repo_name = RepoName::new_unchecked("tmp");

    reg.add_repository(
        &tmp_repo_name,
        url::Url::from_file_path(tmp_repo_dir.path()).unwrap(),
    )
    .unwrap();

    let sync_service = SyncServiceImpl::new(
        reg.clone(),
        Arc::new(DatasetRepositoryLocalFs::new(ws.clone())),
        Arc::new(DatasetFactoryImpl::new(IpfsGateway::default())),
        Arc::new(DummySmartTransferProtocolClient::new()),
        Arc::new(kamu::infra::utils::ipfs_wrapper::IpfsClient::default()),
    );

    for import_alias in to_import {
        sync_service
            .sync(
                &import_alias
                    .as_remote_alias(tmp_repo_name.clone())
                    .into_any_ref(),
                &import_alias.into_any_ref(),
                SyncOptions::default(),
                None,
            )
            .await
            .unwrap();
    }
}

#[test_log::test(tokio::test)]
async fn test_pull_batching_chain() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path());

    // A - B - C
    create_graph(
        harness.local_repo.as_ref(),
        vec![
            (n!("a"), names![]),
            (n!("b"), names!["a"]),
            (n!("c"), names!["b"]),
        ],
    )
    .await;

    assert_eq!(
        harness.pull(refs!["c"], PullOptions::default()).await,
        vec![PullBatch::Transform(refs!["c"])]
    );

    assert_eq!(
        harness.pull(refs!["c", "a"], PullOptions::default()).await,
        vec![
            PullBatch::Ingest(refs!["a"]),
            PullBatch::Transform(refs!["c"]),
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
            PullBatch::Transform(refs!["c"]),
        ]
    );
}

#[test_log::test(tokio::test)]
async fn test_pull_batching_complex() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path());

    //    / C \
    // A <     > > E
    //    \ D / /
    //         /
    // B - - -/
    create_graph(
        harness.local_repo.as_ref(),
        vec![
            (n!("a"), names![]),
            (n!("b"), names![]),
            (n!("c"), names!["a"]),
            (n!("d"), names!["a"]),
            (n!("e"), names!["c", "d", "b"]),
        ],
    )
    .await;

    assert_eq!(
        harness.pull(refs!["e"], PullOptions::default()).await,
        vec![PullBatch::Transform(refs!["e"])]
    );

    assert_matches!(
        harness
            .pull_svc
            .pull_multi(
                &mut vec![ar!("z")].into_iter(),
                PullOptions::default(),
                None,
                None,
                None
            )
            .await
            .unwrap()[0],
        PullResponse {
            result: Err(PullError::NotFound(_)),
            ..
        },
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

#[test_log::test(tokio::test)]
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
        harness.local_repo.as_ref(),
        vec![
            (n!("c"), names![]),
            (n!("d"), names![]),
            (n!("f"), names!["e", "c"]),
            (n!("g"), names!["f", "d"]),
        ],
    )
    .await;

    // Add remote pull alias to E
    harness
        .remote_alias_reg
        .get_remote_aliases(&rl!("e"))
        .await
        .unwrap()
        .add(
            &DatasetRefRemote::try_from("kamu.dev/anonymous/e").unwrap(),
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
            &RepoName::new_unchecked("myrepo"),
            url::Url::parse("file:///tmp/nowhere").unwrap(),
        )
        .unwrap();

    let res = harness
        .pull_svc
        .pull_multi_ext(
            &mut vec![PullRequest {
                local_ref: Some(n!("bar").into()),
                remote_ref: Some(rr!("myrepo/foo")),
                ingest_from: None,
            }]
            .into_iter(),
            PullOptions::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(res.len(), 1);
    assert_matches!(
        res[0],
        PullResponse {
            result: Ok(PullResult::Updated {
                old_head: None,
                new_head: _,
                num_blocks: 1,
            }),
            ..
        }
    );

    let aliases = harness
        .remote_alias_reg
        .get_remote_aliases(&rl!("bar"))
        .await
        .unwrap();
    let pull_aliases: Vec<_> = aliases
        .get_by_kind(RemoteAliasKind::Pull)
        .map(|i| i.clone())
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("myrepo/foo").unwrap()]
    );
}

#[tokio::test]
async fn test_sync_from_url_and_local_ref() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path());

    let res = harness
        .pull_svc
        .pull_multi_ext(
            &mut vec![PullRequest {
                local_ref: Some(n!("bar").into()),
                remote_ref: Some(rr!("http://example.com/odf/bar")),
                ingest_from: None,
            }]
            .into_iter(),
            PullOptions::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(res.len(), 1);
    assert_matches!(
        res[0],
        PullResponse {
            result: Ok(PullResult::Updated {
                old_head: None,
                new_head: _,
                num_blocks: 1,
            }),
            ..
        }
    );

    let aliases = harness
        .remote_alias_reg
        .get_remote_aliases(&rl!("bar"))
        .await
        .unwrap();
    let pull_aliases: Vec<_> = aliases
        .get_by_kind(RemoteAliasKind::Pull)
        .map(|i| i.clone())
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("http://example.com/odf/bar").unwrap()]
    );
}

#[tokio::test]
async fn test_sync_from_url_only() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path());

    let res = harness
        .pull_svc
        .pull_multi_ext(
            &mut vec![PullRequest {
                local_ref: None,
                remote_ref: Some(rr!("http://example.com/odf/bar")),
                ingest_from: None,
            }]
            .into_iter(),
            PullOptions::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(res.len(), 1);
    assert_matches!(
        res[0],
        PullResponse {
            result: Ok(PullResult::Updated {
                old_head: None,
                new_head: _,
                num_blocks: 1,
            }),
            ..
        }
    );

    let aliases = harness
        .remote_alias_reg
        .get_remote_aliases(&rl!("bar"))
        .await
        .unwrap();
    let pull_aliases: Vec<_> = aliases
        .get_by_kind(RemoteAliasKind::Pull)
        .map(|i| i.clone())
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("http://example.com/odf/bar").unwrap()]
    );
}

#[tokio::test]
async fn test_set_watermark() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path());

    let dataset_alias = n!("foo");

    harness
        .local_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name(&dataset_alias.dataset_name)
                .build(),
        )
        .await
        .unwrap();

    let num_blocks = || async {
        let ds = harness
            .local_repo
            .get_dataset(&dataset_alias.as_local_ref())
            .await
            .unwrap();

        use futures::StreamExt;
        ds.as_metadata_chain().iter_blocks().count().await
    };
    assert_eq!(num_blocks().await, 1);

    assert!(matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_alias.as_local_ref(),
                Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()
            )
            .await,
        Ok(PullResult::Updated { .. })
    ));
    assert_eq!(num_blocks().await, 2);

    assert!(matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_alias.as_local_ref(),
                Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap()
            )
            .await,
        Ok(PullResult::Updated { .. })
    ));
    assert_eq!(num_blocks().await, 3);

    assert!(matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_alias.as_local_ref(),
                Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap()
            )
            .await,
        Ok(PullResult::UpToDate)
    ));
    assert_eq!(num_blocks().await, 3);

    assert!(matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_alias.as_local_ref(),
                Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()
            )
            .await,
        Ok(PullResult::UpToDate)
    ));
    assert_eq!(num_blocks().await, 3);
}

/////////////////////////////////////////////////////////////////////////////////////////

struct PullTestHarness {
    calls: Arc<Mutex<Vec<PullBatch>>>,
    workspace_layout: Arc<WorkspaceLayout>,
    local_repo: Arc<DatasetRepositoryLocalFs>,
    remote_repo_reg: Arc<RemoteRepositoryRegistryImpl>,
    remote_alias_reg: Arc<RemoteAliasesRegistryImpl>,
    pull_svc: PullServiceImpl,
}

impl PullTestHarness {
    fn new(tmp_path: &Path) -> Self {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let workspace_layout = Arc::new(WorkspaceLayout::create(tmp_path).unwrap());
        let local_repo = Arc::new(DatasetRepositoryLocalFs::new(workspace_layout.clone()));
        let remote_repo_reg = Arc::new(RemoteRepositoryRegistryImpl::new(workspace_layout.clone()));
        let remote_alias_reg = Arc::new(RemoteAliasesRegistryImpl::new(
            local_repo.clone(),
            workspace_layout.clone(),
        ));
        let ingest_svc = Arc::new(TestIngestService::new(calls.clone()));
        let transform_svc = Arc::new(TestTransformService::new(calls.clone()));
        let sync_svc = Arc::new(TestSyncService::new(calls.clone(), local_repo.clone()));
        let pull_svc = PullServiceImpl::new(
            local_repo.clone(),
            remote_alias_reg.clone(),
            ingest_svc,
            transform_svc,
            sync_svc,
        );

        Self {
            calls,
            workspace_layout,
            local_repo,
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
        let results = self
            .pull_svc
            .pull_multi(&mut refs.into_iter(), options, None, None, None)
            .await
            .unwrap();

        for res in results {
            assert_matches!(res, PullResponse { result: Ok(_), .. });
        }

        self.collect_calls()
    }
}

#[derive(Debug, Clone, Eq)]
pub enum PullBatch {
    Ingest(Vec<DatasetRefAny>),
    Transform(Vec<DatasetRefAny>),
    Sync(Vec<(DatasetRefAny, DatasetRefAny)>),
}

impl PullBatch {
    fn cmp_ref(lhs: &DatasetRefAny, rhs: &DatasetRefAny) -> bool {
        fn tuplify(
            v: &DatasetRefAny,
        ) -> (
            Option<&DatasetID>,
            Option<&url::Url>,
            Option<&str>,
            Option<&str>,
            Option<&DatasetName>,
        ) {
            match v {
                DatasetRefAny::ID(_, id) => (Some(id), None, None, None, None),
                DatasetRefAny::Url(url) => (None, Some(url), None, None, None),
                DatasetRefAny::Alias(r, a, n) => (
                    None,
                    None,
                    r.as_ref().map(|v| v.as_ref()),
                    a.as_ref().map(|v| v.as_ref()),
                    Some(n),
                ),
                DatasetRefAny::LocalHandle(h) => (
                    None,
                    None,
                    None,
                    h.alias.account_name.as_ref().map(|v| v.as_str()),
                    Some(&h.alias.dataset_name),
                ),
                DatasetRefAny::RemoteHandle(h) => (
                    None,
                    None,
                    Some(h.alias.repo_name.as_str()),
                    h.alias.account_name.as_ref().map(|v| v.as_str()),
                    Some(&h.alias.dataset_name),
                ),
            }
        }
        tuplify(lhs) == tuplify(rhs)
    }
}

impl std::cmp::PartialEq for PullBatch {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Ingest(l), Self::Ingest(r)) => {
                let mut l = l.clone();
                l.sort();
                let mut r = r.clone();
                r.sort();
                l.len() == r.len() && std::iter::zip(&l, &r).all(|(li, ri)| Self::cmp_ref(li, ri))
            }
            (Self::Transform(l), Self::Transform(r)) => {
                let mut l = l.clone();
                l.sort();
                let mut r = r.clone();
                r.sort();
                l.len() == r.len() && std::iter::zip(&l, &r).all(|(li, ri)| Self::cmp_ref(li, ri))
            }
            (Self::Sync(l), Self::Sync(r)) => {
                let mut l = l.clone();
                l.sort();
                let mut r = r.clone();
                r.sort();
                l.len() == r.len()
                    && std::iter::zip(&l, &r)
                        .all(|((l1, l2), (r1, r2))| Self::cmp_ref(l1, r1) && Self::cmp_ref(l2, r2))
            }
            _ => false,
        }
    }
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
        _dataset_ref: &DatasetRef,
        _ingest_options: IngestOptions,
        _maybe_listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        unimplemented!();
    }

    async fn ingest_from(
        &self,
        _dataset_ref: &DatasetRef,
        _fetch: FetchStep,
        _options: IngestOptions,
        _listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError> {
        unimplemented!()
    }

    async fn ingest_multi(
        &self,
        _dataset_refs: &mut dyn Iterator<Item = DatasetRef>,
        _ingest_options: IngestOptions,
        _maybe_multi_listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetRef, Result<IngestResult, IngestError>)> {
        unimplemented!()
    }

    async fn ingest_multi_ext(
        &self,
        requests: &mut dyn Iterator<Item = IngestRequest>,
        _options: IngestOptions,
        _listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetRef, Result<IngestResult, IngestError>)> {
        let requests: Vec<_> = requests.collect();
        let results = requests
            .iter()
            .map(|r| {
                (
                    r.dataset_ref.clone(),
                    Ok(IngestResult::UpToDate {
                        no_polling_source: false,
                        uncacheable: false,
                    }),
                )
            })
            .collect();
        self.calls.lock().unwrap().push(PullBatch::Ingest(
            requests.into_iter().map(|i| i.dataset_ref.into()).collect(),
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
        _dataset_ref: &DatasetRef,
        _maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError> {
        unimplemented!();
    }

    async fn transform_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRef>,
        _maybe_multi_listener: Option<Arc<dyn TransformMultiListener>>,
    ) -> Vec<(DatasetRef, Result<TransformResult, TransformError>)> {
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
        _dataset_ref: &DatasetRef,
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
    local_repo: Arc<dyn DatasetRepository>,
}

impl TestSyncService {
    fn new(calls: Arc<Mutex<Vec<PullBatch>>>, local_repo: Arc<dyn DatasetRepository>) -> Self {
        Self { calls, local_repo }
    }
}

#[async_trait::async_trait(?Send)]
impl SyncService for TestSyncService {
    async fn sync(
        &self,
        _src: &DatasetRefAny,
        _dst: &DatasetRefAny,
        _options: SyncOptions,
        _listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        unimplemented!()
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

            let local_ref = dst.as_local_single_tenant_ref().unwrap();

            match self
                .local_repo
                .try_resolve_dataset_ref(&local_ref)
                .await
                .unwrap()
            {
                None => {
                    self.local_repo
                        .create_dataset_from_snapshot(
                            MetadataFactory::dataset_snapshot()
                                .name(local_ref.dataset_name().unwrap())
                                .build(),
                        )
                        .await
                        .unwrap();
                }
                Some(_) => (),
            }

            results.push(SyncResultMulti {
                src,
                dst,
                result: Ok(SyncResult::Updated {
                    old_head: None,
                    new_head: Multihash::from_digest_sha3_256(b"boop"),
                    num_blocks: 1,
                }),
            });
        }
        self.calls.lock().unwrap().push(PullBatch::Sync(call));
        results
    }

    async fn ipfs_add(&self, _src: &DatasetRef) -> Result<String, SyncError> {
        unimplemented!()
    }
}
