// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::convert::TryFrom;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dill::*;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use opendatafabric::*;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! n {
    ($s:expr) => {
        DatasetAlias::new(None, DatasetName::try_from($s).unwrap())
    };
}

macro_rules! mn {
    ($s:expr) => {
        DatasetAlias::try_from($s).unwrap()
    };
}

macro_rules! rl {
    ($s:expr) => {
        DatasetRef::Alias(DatasetAlias::new(None, DatasetName::try_from($s).unwrap()))
    };
}

/*
macro_rules! mrl {
    ($s:expr) => {
        DatasetRef::Alias(DatasetAlias::try_from($s).unwrap())
    };
}
    */

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

macro_rules! mnames {
    [] => {
        vec![]
    };
    [$x:expr] => {
        vec![mn!($x)]
    };
    [$x:expr, $($y:expr),+] => {
        vec![mn!($x), $(mn!($y)),+]
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

macro_rules! refs_local {
    [] => {
        vec![]
    };
    [$x:expr] => {
        vec![mn!($x).as_any_ref()]
    };
    [$x:expr, $($y:expr),+] => {
        vec![mn!($x).as_any_ref(), $(mn!($y).as_any_ref()),+]
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_graph(
    repo: &DatasetRepositoryLocalFs,
    datasets: Vec<(DatasetAlias, Vec<DatasetAlias>)>,
) {
    for (dataset_alias, deps) in datasets {
        repo.create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name(dataset_alias)
                .kind(if deps.is_empty() {
                    DatasetKind::Root
                } else {
                    DatasetKind::Derivative
                })
                .push_event::<MetadataEvent>(if deps.is_empty() {
                    MetadataFactory::set_polling_source().build().into()
                } else {
                    MetadataFactory::set_transform()
                        .inputs_from_refs(deps)
                        .build()
                        .into()
                })
                .build(),
        )
        .await
        .unwrap();
    }
}

// Adding a remote dataset is a bit of a pain.
// We cannot add a local dataset and then add a pull alias without adding all of
// its dependencies too. So instead we're creating a repository based on temp
// dir and syncing it into the main workspace. TODO: Add simpler way to import
// remote dataset
async fn create_graph_remote(
    remote_repo_name: &str,
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    reg: Arc<RemoteRepositoryRegistryImpl>,
    datasets: Vec<(DatasetAlias, Vec<DatasetAlias>)>,
    to_import: Vec<DatasetAlias>,
) -> tempfile::TempDir {
    let tmp_repo_dir = tempfile::tempdir().unwrap();

    let remote_dataset_repo = DatasetRepositoryLocalFs::new(
        tmp_repo_dir.path().to_owned(),
        Arc::new(CurrentAccountSubject::new_test()),
        false,
        Arc::new(SystemTimeSourceDefault),
    );

    create_graph(&remote_dataset_repo, datasets).await;

    let tmp_repo_name = RepoName::new_unchecked(remote_repo_name);

    reg.add_repository(
        &tmp_repo_name,
        url::Url::from_file_path(tmp_repo_dir.path()).unwrap(),
    )
    .unwrap();

    let dataset_factory = Arc::new(DatasetFactoryImpl::new(
        IpfsGateway::default(),
        Arc::new(auth::DummyOdfServerAccessTokenResolver::new()),
    ));

    let sync_service = SyncServiceImpl::new(
        reg.clone(),
        dataset_factory.clone(),
        Arc::new(DummySmartTransferProtocolClient::new()),
        Arc::new(kamu::utils::ipfs_wrapper::IpfsClient::default()),
    );

    let sync_request_builder = SyncRequestBuilder::new(
        Arc::new(DatasetRegistryRepoBridge::new(dataset_repo)),
        dataset_factory,
        dataset_repo_writer,
        reg.clone(),
    );

    for import_alias in to_import {
        sync_service
            .sync(
                sync_request_builder
                    .build_sync_request(
                        import_alias
                            .as_remote_alias(tmp_repo_name.clone())
                            .into_any_ref(),
                        import_alias.into_any_ref(),
                        true,
                    )
                    .await
                    .unwrap(),
                SyncOptions::default(),
                None,
            )
            .await
            .unwrap();
    }

    tmp_repo_dir
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_batching_chain() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path(), false);

    // A - B - C
    create_graph(
        harness.dataset_repo.as_ref(),
        vec![
            (n!("a"), names![]),
            (n!("b"), names!["a"]),
            (n!("c"), names!["b"]),
        ],
    )
    .await;

    assert_eq!(
        harness
            .pull(refs!["c"], PullOptions::default())
            .await
            .unwrap(),
        vec![PullBatch::Transform(refs!["c"])]
    );

    assert_eq!(
        harness
            .pull(refs!["c", "a"], PullOptions::default())
            .await
            .unwrap(),
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
            .await
            .unwrap(),
        vec![
            PullBatch::Ingest(refs!["a"]),
            PullBatch::Transform(refs!["b"]),
            PullBatch::Transform(refs!["c"]),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_batching_chain_multi_tenant() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path(), true);

    // XA - YB - ZC
    create_graph(
        harness.dataset_repo.as_ref(),
        vec![
            (mn!("x/a"), mnames![]),
            (mn!("y/b"), mnames!["x/a"]),
            (mn!("z/c"), mnames!["y/b"]),
        ],
    )
    .await;

    assert_eq!(
        harness
            .pull(refs!["z/c"], PullOptions::default())
            .await
            .unwrap(),
        vec![PullBatch::Transform(refs_local!["z/c"])]
    );

    assert_eq!(
        harness
            .pull(refs!["z/c", "x/a"], PullOptions::default())
            .await
            .unwrap(),
        vec![
            PullBatch::Ingest(refs_local!["x/a"]),
            PullBatch::Transform(refs_local!["z/c"]),
        ],
    );

    assert_eq!(
        harness
            .pull(
                refs!["z/c"],
                PullOptions {
                    recursive: true,
                    ..PullOptions::default()
                }
            )
            .await
            .unwrap(),
        vec![
            PullBatch::Ingest(refs_local!["x/a"]),
            PullBatch::Transform(refs_local!["y/b"]),
            PullBatch::Transform(refs_local!["z/c"]),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_batching_complex() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path(), false);

    //    / C \
    // A <     > > E
    //    \ D / /
    //         /
    // B - - -/
    create_graph(
        harness.dataset_repo.as_ref(),
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
        harness
            .pull(refs!["e"], PullOptions::default())
            .await
            .unwrap(),
        vec![PullBatch::Transform(refs!["e"])]
    );

    assert_matches!(
        harness
            .pull(vec![ar!("z")], PullOptions::default())
            .await
            .err()
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
            .await
            .unwrap(),
        vec![
            PullBatch::Ingest(refs!["a", "b"]),
            PullBatch::Transform(refs!["c", "d"]),
            PullBatch::Transform(refs!["e"]),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_batching_complex_with_remote() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_dir.path(), false);

    // (A) - (E) - F - G
    // (B) --/    /   /
    // C --------/   /
    // D -----------/
    let _remote_tmp_dir = create_graph_remote(
        "kamu.dev",
        harness.dataset_repo.clone(),
        harness.dataset_repo.clone(),
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
        harness.dataset_repo.as_ref(),
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
        .get_remote_aliases(&rl!("e"))
        .await
        .add(
            &DatasetRefRemote::try_from("kamu.dev/e").unwrap(),
            RemoteAliasKind::Pull,
        )
        .await
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
            .await
            .unwrap(),
        vec![PullBatch::Sync(vec![(
            rr!("kamu.dev/e").into(),
            n!("e").into()
        )])],
    );

    // Explicit remote reference associates with E
    assert_eq!(
        harness
            .pull(
                refs!["kamu.dev/e"],
                PullOptions {
                    recursive: true,
                    ..PullOptions::default()
                }
            )
            .await
            .unwrap(),
        vec![PullBatch::Sync(vec![(
            rr!("kamu.dev/e").into(),
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
            .await
            .unwrap(),
        vec![
            PullBatch::Sync(vec![(rr!("kamu.dev/e").into(), n!("e").into())]),
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
            .await
            .unwrap(),
        vec![
            PullBatch::Sync(vec![(rr!("kamu.dev/e").into(), n!("e").into())]),
            PullBatch::Ingest(refs!("c", "d")),
            PullBatch::Transform(refs!("f")),
            PullBatch::Transform(refs!("g")),
        ],
    );

    // Remote is recursed onto while also specified explicitly (via remote ref)
    assert_eq!(
        harness
            .pull(
                refs!["g", "kamu.dev/e"],
                PullOptions {
                    recursive: true,
                    ..PullOptions::default()
                }
            )
            .await
            .unwrap(),
        vec![
            PullBatch::Sync(vec![(rr!("kamu.dev/e").into(), n!("e").into())]),
            PullBatch::Ingest(refs!("c", "d")),
            PullBatch::Transform(refs!("f")),
            PullBatch::Transform(refs!("g")),
        ],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path(), false);

    let _remote_tmp_dir = create_graph_remote(
        "kamu.dev",
        harness.dataset_repo.clone(),
        harness.dataset_repo.clone(),
        harness.remote_repo_reg.clone(),
        vec![(n!("foo"), names![])],
        names!(),
    )
    .await;

    let res = harness
        .pull_with_requests(
            vec![PullRequest::Remote(PullRequestRemote {
                maybe_local_alias: Some(n!("bar")),
                remote_ref: rr!("kamu.dev/foo"),
            })],
            PullOptions::default(),
        )
        .await
        .unwrap();

    assert_eq!(
        res,
        vec![PullBatch::Sync(vec![(
            rr!("kamu.dev/foo").into(),
            n!("bar").into()
        )])]
    );

    // Note: we moved pull aliases into use case, so this awaits for refactoring
    /*
    let aliases = harness.get_remote_aliases(&rl!("bar")).await;
    let pull_aliases: Vec<_> = aliases
        .get_by_kind(RemoteAliasKind::Pull)
        .cloned()
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("kamu.dev/foo").unwrap()]
    );*/
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from_url_and_local_ref() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path(), false);

    let _remote_tmp_dir = create_graph_remote(
        "kamu.dev",
        harness.dataset_repo.clone(),
        harness.dataset_repo.clone(),
        harness.remote_repo_reg.clone(),
        vec![(n!("bar"), names![])],
        names!(),
    )
    .await;

    let res = harness
        .pull_with_requests(
            vec![PullRequest::Remote(PullRequestRemote {
                maybe_local_alias: Some(n!("bar")),
                remote_ref: rr!("kamu.dev/bar"),
            })],
            PullOptions::default(),
        )
        .await
        .unwrap();

    assert_eq!(
        res,
        vec![PullBatch::Sync(vec![(
            rr!("kamu.dev/bar").into(),
            n!("bar").into()
        )])]
    );

    // Note: we moved pull aliases into use case, so this awaits for refactoring
    /*let aliases = harness.get_remote_aliases(&rl!("bar")).await;
    let pull_aliases: Vec<_> = aliases
        .get_by_kind(RemoteAliasKind::Pull)
        .cloned()
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("kamu.dev/bar").unwrap()]
    );*/
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from_url_and_local_multi_tenant_ref() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path(), true);

    let _remote_tmp_dir = create_graph_remote(
        "kamu.dev",
        harness.dataset_repo.clone(),
        harness.dataset_repo.clone(),
        harness.remote_repo_reg.clone(),
        vec![(n!("bar"), names![])],
        names!(),
    )
    .await;

    let res = harness
        .pull_with_requests(
            vec![PullRequest::Remote(PullRequestRemote {
                maybe_local_alias: Some(mn!("x/bar")),
                remote_ref: rr!("kamu.dev/bar"),
            })],
            PullOptions::default(),
        )
        .await
        .unwrap();

    assert_eq!(
        res,
        vec![PullBatch::Sync(vec![(
            rr!("kamu.dev/bar").into(),
            mn!("x/bar").into()
        )])]
    );

    // Note: we moved pull aliases into use case, so this awaits for refactoring
    /*
    let aliases = harness.get_remote_aliases(&mrl!("x/bar")).await;
    let pull_aliases: Vec<_> = aliases
        .get_by_kind(RemoteAliasKind::Pull)
        .cloned()
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("http://example.com/odf/bar").unwrap()]
    );*/
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from_url_only() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path(), false);

    let _remote_tmp_dir = create_graph_remote(
        "kamu.dev",
        harness.dataset_repo.clone(),
        harness.dataset_repo.clone(),
        harness.remote_repo_reg.clone(),
        vec![(n!("bar"), names![])],
        names!(),
    )
    .await;

    let res = harness
        .pull_with_requests(
            vec![PullRequest::Remote(PullRequestRemote {
                maybe_local_alias: None,
                remote_ref: rr!("kamu.dev/bar"),
            })],
            PullOptions::default(),
        )
        .await
        .unwrap();

    assert_eq!(
        res,
        vec![PullBatch::Sync(vec![(
            rr!("kamu.dev/bar").into(),
            n!("bar").into()
        )])]
    );

    // Note: we moved pull aliases into use case, so this awaits for refactoring
    /*let aliases = harness.get_remote_aliases(&rl!("bar")).await;
    let pull_aliases: Vec<_> = aliases
        .get_by_kind(RemoteAliasKind::Pull)
        .cloned()
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("http://example.com/odf/bar").unwrap()]
    );*/
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from_url_only_multi_tenant_case() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path(), true);

    let _remote_tmp_dir = create_graph_remote(
        "kamu.dev",
        harness.dataset_repo.clone(),
        harness.dataset_repo.clone(),
        harness.remote_repo_reg.clone(),
        vec![(n!("bar"), names![])],
        names!(),
    )
    .await;

    let res = harness
        .pull_with_requests(
            vec![PullRequest::Remote(PullRequestRemote {
                maybe_local_alias: None,
                remote_ref: rr!("kamu.dev/bar"),
            })],
            PullOptions::default(),
        )
        .await
        .unwrap();

    assert_eq!(
        res,
        vec![PullBatch::Sync(vec![(
            rr!("kamu.dev/bar").into(),
            n!("bar").into()
        )])]
    );

    // Note: we moved pull aliases into use case, so this awaits for refactoring
    /*
    let aliases = harness
        .get_remote_aliases(&mrl!(format!("{}/{}", DEFAULT_ACCOUNT_NAME_STR, "bar")))
        .await;
    let pull_aliases: Vec<_> = aliases
        .get_by_kind(RemoteAliasKind::Pull)
        .cloned()
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("http://example.com/odf/bar").unwrap()]
    );*/
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PullTestHarness {
    calls: Arc<Mutex<Vec<PullBatch>>>,
    dataset_repo: Arc<DatasetRepositoryLocalFs>,
    remote_repo_reg: Arc<RemoteRepositoryRegistryImpl>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    pull_request_planner: Arc<dyn PullRequestPlanner>,
    in_multi_tenant_mode: bool,
}

impl PullTestHarness {
    fn new(tmp_path: &Path, multi_tenant: bool) -> Self {
        let calls = Arc::new(Mutex::new(Vec::new()));

        let datasets_dir_path = tmp_path.join("datasets");
        std::fs::create_dir(&datasets_dir_path).unwrap();

        let run_info_dir = tmp_path.join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add_value(RunInfoDir::new(run_info_dir))
            .add::<SystemTimeSourceDefault>()
            .add_value(CurrentAccountSubject::new_test())
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir_path)
                    .with_multi_tenant(multi_tenant),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<DatasetRegistryRepoBridge>()
            .add_value(RemoteRepositoryRegistryImpl::create(tmp_path.join("repos")).unwrap())
            .bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add::<PullRequestPlannerImpl>()
            .add::<TransformRequestPlannerImpl>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<SyncRequestBuilder>()
            .add::<DatasetFactoryImpl>()
            .add::<auth::DummyOdfServerAccessTokenResolver>()
            .add_value(IpfsGateway::default())
            .build();

        Self {
            calls,
            dataset_repo: catalog.get_one().unwrap(),
            remote_repo_reg: catalog.get_one().unwrap(),
            remote_alias_reg: catalog.get_one().unwrap(),
            pull_request_planner: catalog.get_one().unwrap(),
            in_multi_tenant_mode: multi_tenant,
        }
    }

    fn collect_calls(&self) -> Vec<PullBatch> {
        let mut calls = Vec::new();
        std::mem::swap(self.calls.lock().unwrap().as_mut(), &mut calls);
        calls
    }

    async fn pull(
        &self,
        refs: Vec<DatasetRefAny>,
        options: PullOptions,
    ) -> Result<Vec<PullBatch>, Vec<PullResponse>> {
        let requests: Vec<_> = refs
            .into_iter()
            .map(|r| PullRequest::from_any_ref(&r, |_| !self.in_multi_tenant_mode))
            .collect();
        self.pull_with_requests(requests, options).await
    }

    async fn pull_with_requests(
        &self,
        requests: Vec<PullRequest>,
        options: PullOptions,
    ) -> Result<Vec<PullBatch>, Vec<PullResponse>> {
        let (plan_iterations, errors) = self
            .pull_request_planner
            .build_pull_multi_plan(&requests, &options, self.in_multi_tenant_mode)
            .await;
        if !errors.is_empty() {
            return Err(errors);
        }

        for iteration in plan_iterations {
            match iteration.job {
                PullPlanIterationJob::Ingest(batch) => {
                    self.calls.lock().unwrap().push(PullBatch::Ingest(
                        batch
                            .into_iter()
                            .map(|pii| pii.target.handle.as_any_ref())
                            .collect(),
                    ));
                }
                PullPlanIterationJob::Transform(batch) => {
                    self.calls.lock().unwrap().push(PullBatch::Transform(
                        batch
                            .iter()
                            .map(|pti| pti.target.handle.as_any_ref())
                            .collect(),
                    ));
                }
                PullPlanIterationJob::Sync((_, sync_requests)) => {
                    let mut calls = Vec::new();
                    for SyncRequest { src, dst } in sync_requests {
                        calls.push((src.src_ref.clone(), dst.dst_ref.clone()));
                    }
                    self.calls.lock().unwrap().push(PullBatch::Sync(calls));
                }
            };
        }

        tokio::time::sleep(Duration::from_millis(1)).await;

        Ok(self.collect_calls())
    }

    async fn get_remote_aliases(&self, dataset_ref: &DatasetRef) -> Box<dyn RemoteAliases> {
        let dataset = self
            .dataset_repo
            .get_dataset_by_ref(dataset_ref)
            .await
            .unwrap();
        self.remote_alias_reg
            .get_remote_aliases(dataset)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq)]
enum PullBatch {
    Ingest(Vec<DatasetRefAny>),
    Transform(Vec<DatasetRefAny>),
    Sync(Vec<(DatasetRefAny, DatasetRefAny)>),
}

impl PullBatch {
    fn cmp_ref(lhs: &DatasetRefAny, rhs: &DatasetRefAny) -> bool {
        #[allow(clippy::type_complexity)]
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
                DatasetRefAny::LocalAlias(a, n) => {
                    (None, None, None, a.as_ref().map(AsRef::as_ref), Some(n))
                }
                DatasetRefAny::RemoteAlias(r, a, n) => (
                    None,
                    None,
                    Some(r.as_ref()),
                    a.as_ref().map(AsRef::as_ref),
                    Some(n),
                ),
                DatasetRefAny::AmbiguousAlias(ra, n) => {
                    (None, None, Some(ra.as_ref()), None, Some(n))
                }
                DatasetRefAny::LocalHandle(h) => (
                    None,
                    None,
                    None,
                    h.alias.account_name.as_ref().map(AccountName::as_str),
                    Some(&h.alias.dataset_name),
                ),
                DatasetRefAny::RemoteHandle(h) => (
                    None,
                    None,
                    Some(h.alias.repo_name.as_str()),
                    h.alias.account_name.as_ref().map(AccountName::as_str),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
