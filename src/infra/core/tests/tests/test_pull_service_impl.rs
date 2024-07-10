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

use chrono::prelude::*;
use dill::*;
use domain::auth::AlwaysHappyDatasetActionAuthorizer;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME_STR};
use opendatafabric::*;

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

macro_rules! mrl {
    ($s:expr) => {
        DatasetRef::Alias(DatasetAlias::try_from($s).unwrap())
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
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    reg: Arc<RemoteRepositoryRegistryImpl>,
    datasets: Vec<(DatasetAlias, Vec<DatasetAlias>)>,
    to_import: Vec<DatasetAlias>,
) {
    let tmp_repo_dir = tempfile::tempdir().unwrap();

    let remote_dataset_repo = DatasetRepositoryLocalFs::new(
        tmp_repo_dir.path().to_owned(),
        Arc::new(CurrentAccountSubject::new_test()),
        Arc::new(DependencyGraphServiceInMemory::new(None)),
        false,
        Arc::new(SystemTimeSourceDefault),
    );

    create_graph(&remote_dataset_repo, datasets).await;

    let tmp_repo_name = RepoName::new_unchecked("tmp");

    reg.add_repository(
        &tmp_repo_name,
        url::Url::from_file_path(tmp_repo_dir.path()).unwrap(),
    )
    .unwrap();

    let sync_service = SyncServiceImpl::new(
        reg.clone(),
        dataset_repo,
        dataset_repo_writer,
        Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
        Arc::new(DatasetFactoryImpl::new(
            IpfsGateway::default(),
            Arc::new(auth::DummyOdfServerAccessTokenResolver::new()),
        )),
        Arc::new(DummySmartTransferProtocolClient::new()),
        Arc::new(kamu::utils::ipfs_wrapper::IpfsClient::default()),
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
        harness.pull(refs!["c"], PullMultiOptions::default()).await,
        vec![PullBatch::Transform(refs!["c"])]
    );

    assert_eq!(
        harness
            .pull(refs!["c", "a"], PullMultiOptions::default())
            .await,
        vec![
            PullBatch::Ingest(refs!["a"]),
            PullBatch::Transform(refs!["c"]),
        ],
    );

    assert_eq!(
        harness
            .pull(
                refs!["c"],
                PullMultiOptions {
                    recursive: true,
                    ..PullMultiOptions::default()
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
            .pull(refs!["z/c"], PullMultiOptions::default())
            .await,
        vec![PullBatch::Transform(refs_local!["z/c"])]
    );

    assert_eq!(
        harness
            .pull(refs!["z/c", "x/a"], PullMultiOptions::default())
            .await,
        vec![
            PullBatch::Ingest(refs_local!["x/a"]),
            PullBatch::Transform(refs_local!["z/c"]),
        ],
    );

    assert_eq!(
        harness
            .pull(
                refs!["z/c"],
                PullMultiOptions {
                    recursive: true,
                    ..PullMultiOptions::default()
                }
            )
            .await,
        vec![
            PullBatch::Ingest(refs_local!["x/a"]),
            PullBatch::Transform(refs_local!["y/b"]),
            PullBatch::Transform(refs_local!["z/c"]),
        ]
    );
}

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
        harness.pull(refs!["e"], PullMultiOptions::default()).await,
        vec![PullBatch::Transform(refs!["e"])]
    );

    assert_matches!(
        harness
            .pull_svc
            .pull_multi(vec![ar!("z")], PullMultiOptions::default(), None)
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
                PullMultiOptions {
                    recursive: true,
                    ..PullMultiOptions::default()
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
    let harness = PullTestHarness::new(tmp_dir.path(), false);

    // (A) - (E) - F - G
    // (B) --/    /   /
    // C --------/   /
    // D -----------/
    create_graph_remote(
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
        .remote_alias_reg
        .get_remote_aliases(&rl!("e"))
        .await
        .unwrap()
        .add(
            &DatasetRefRemote::try_from("kamu.dev/anonymous/e").unwrap(),
            RemoteAliasKind::Pull,
        )
        .await
        .unwrap();

    // Pulling E results in a sync
    assert_eq!(
        harness
            .pull(
                refs!["e"],
                PullMultiOptions {
                    recursive: true,
                    ..PullMultiOptions::default()
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
                PullMultiOptions {
                    recursive: true,
                    ..PullMultiOptions::default()
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
                PullMultiOptions {
                    recursive: true,
                    ..PullMultiOptions::default()
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
                PullMultiOptions {
                    recursive: true,
                    ..PullMultiOptions::default()
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
                PullMultiOptions {
                    recursive: true,
                    ..PullMultiOptions::default()
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
    let harness = PullTestHarness::new(tmp_ws_dir.path(), false);

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
            vec![PullRequest {
                local_ref: Some(n!("bar").into()),
                remote_ref: Some(rr!("myrepo/foo")),
            }],
            PullMultiOptions::default(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(res.len(), 1);
    assert_matches!(
        res[0],
        PullResponse {
            result: Ok(PullResult::Updated { old_head: None, .. }),
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
        .cloned()
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("myrepo/foo").unwrap()]
    );
}

#[tokio::test]
async fn test_sync_from_url_and_local_ref() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path(), false);

    let res = harness
        .pull_svc
        .pull_multi_ext(
            vec![PullRequest {
                local_ref: Some(n!("bar").into()),
                remote_ref: Some(rr!("http://example.com/odf/bar")),
            }],
            PullMultiOptions::default(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(res.len(), 1);
    assert_matches!(
        res[0],
        PullResponse {
            result: Ok(PullResult::Updated { old_head: None, .. }),
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
        .cloned()
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("http://example.com/odf/bar").unwrap()]
    );
}

#[tokio::test]
async fn test_sync_from_url_and_local_multi_tenant_ref() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path(), true);

    let res = harness
        .pull_svc
        .pull_multi_ext(
            vec![PullRequest {
                local_ref: Some(mn!("x/bar").into()),
                remote_ref: Some(rr!("http://example.com/odf/bar")),
            }],
            PullMultiOptions::default(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(res.len(), 1);
    assert_matches!(
        res[0],
        PullResponse {
            result: Ok(PullResult::Updated { old_head: None, .. }),
            ..
        }
    );

    let aliases = harness
        .remote_alias_reg
        .get_remote_aliases(&mrl!("x/bar"))
        .await
        .unwrap();
    let pull_aliases: Vec<_> = aliases
        .get_by_kind(RemoteAliasKind::Pull)
        .cloned()
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("http://example.com/odf/bar").unwrap()]
    );
}

#[tokio::test]
async fn test_sync_from_url_only() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path(), false);

    let res = harness
        .pull_svc
        .pull_multi_ext(
            vec![PullRequest {
                local_ref: None,
                remote_ref: Some(rr!("http://example.com/odf/bar")),
            }],
            PullMultiOptions::default(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(res.len(), 1);
    assert_matches!(
        res[0],
        PullResponse {
            result: Ok(PullResult::Updated { old_head: None, .. }),
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
        .cloned()
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("http://example.com/odf/bar").unwrap()]
    );
}

#[tokio::test]
async fn test_sync_from_url_only_multi_tenant_case() {
    let tmp_ws_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new(tmp_ws_dir.path(), true);

    let res = harness
        .pull_svc
        .pull_multi_ext(
            vec![PullRequest {
                local_ref: None,
                remote_ref: Some(rr!("http://example.com/odf/bar")),
            }],
            PullMultiOptions::default(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(res.len(), 1);
    assert_matches!(
        res[0],
        PullResponse {
            result: Ok(PullResult::Updated { old_head: None, .. }),
            ..
        }
    );

    let aliases = harness
        .remote_alias_reg
        .get_remote_aliases(&mrl!(format!("{}/{}", DEFAULT_ACCOUNT_NAME_STR, "bar")))
        .await
        .unwrap();
    let pull_aliases: Vec<_> = aliases
        .get_by_kind(RemoteAliasKind::Pull)
        .cloned()
        .collect();

    assert_eq!(
        pull_aliases,
        vec![DatasetRefRemote::try_from("http://example.com/odf/bar").unwrap()]
    );
}

#[tokio::test]
async fn test_set_watermark() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new_with_authorizer(
        tmp_dir.path(),
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(
            DatasetAlias::new(None, DatasetName::new_unchecked("foo")),
            4,
        ),
        false,
    );

    let dataset_alias = n!("foo");
    harness.create_dataset(&dataset_alias).await;

    assert_eq!(harness.num_blocks(&dataset_alias).await, 1);

    assert_matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_alias.as_local_ref(),
                Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()
            )
            .await,
        Ok(PullResult::Updated { .. })
    );
    assert_eq!(harness.num_blocks(&dataset_alias).await, 2);

    assert_matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_alias.as_local_ref(),
                Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap()
            )
            .await,
        Ok(PullResult::Updated { .. })
    );
    assert_eq!(harness.num_blocks(&dataset_alias).await, 3);

    assert_matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_alias.as_local_ref(),
                Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap()
            )
            .await,
        Ok(PullResult::UpToDate)
    );
    assert_eq!(harness.num_blocks(&dataset_alias).await, 3);

    assert_matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_alias.as_local_ref(),
                Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()
            )
            .await,
        Ok(PullResult::UpToDate)
    );
    assert_eq!(harness.num_blocks(&dataset_alias).await, 3);
}

#[tokio::test]
async fn test_set_watermark_unauthorized() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new_with_authorizer(
        tmp_dir.path(),
        MockDatasetActionAuthorizer::denying(),
        true,
    );

    let dataset_alias = n!("foo");
    harness.create_dataset(&dataset_alias).await;

    assert_matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_alias.as_local_ref(),
                Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()
            )
            .await,
        Err(SetWatermarkError::Access(AccessError::Forbidden(_)))
    );

    assert_eq!(harness.num_blocks(&dataset_alias).await, 1);
}

#[tokio::test]
async fn test_set_watermark_rejects_on_derivative() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = PullTestHarness::new_with_authorizer(
        tmp_dir.path(),
        AlwaysHappyDatasetActionAuthorizer::new(),
        true,
    );

    let dataset_alias = n!("foo");

    harness
        .dataset_repo
        .create_dataset(
            &dataset_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Derivative).build())
                .build_typed(),
        )
        .await
        .unwrap();

    assert_matches!(
        harness
            .pull_svc
            .set_watermark(
                &dataset_alias.as_local_ref(),
                Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()
            )
            .await,
        Err(SetWatermarkError::IsDerivative)
    );

    assert_eq!(harness.num_blocks(&dataset_alias).await, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PullTestHarness {
    calls: Arc<Mutex<Vec<PullBatch>>>,
    dataset_repo: Arc<DatasetRepositoryLocalFs>,
    remote_repo_reg: Arc<RemoteRepositoryRegistryImpl>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    pull_svc: Arc<dyn PullService>,
}

impl PullTestHarness {
    fn new(tmp_path: &Path, multi_tenant: bool) -> Self {
        Self::new_with_authorizer(
            tmp_path,
            auth::AlwaysHappyDatasetActionAuthorizer::new(),
            multi_tenant,
        )
    }

    fn new_with_authorizer<TDatasetAuthorizer: auth::DatasetActionAuthorizer + 'static>(
        tmp_path: &Path,
        dataset_action_authorizer: TDatasetAuthorizer,
        multi_tenant: bool,
    ) -> Self {
        let calls = Arc::new(Mutex::new(Vec::new()));

        let datasets_dir_path = tmp_path.join("datasets");
        std::fs::create_dir(&datasets_dir_path).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<SystemTimeSourceDefault>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(dataset_action_authorizer)
            .bind::<dyn auth::DatasetActionAuthorizer, TDatasetAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir_path)
                    .with_multi_tenant(multi_tenant),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add_value(RemoteRepositoryRegistryImpl::create(tmp_path.join("repos")).unwrap())
            .bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add_value(TestIngestService::new(calls.clone()))
            .bind::<dyn PollingIngestService, TestIngestService>()
            .add_value(TestTransformService::new(calls.clone()))
            .bind::<dyn TransformService, TestTransformService>()
            .add_builder(TestSyncService::builder().with_calls(calls.clone()))
            .bind::<dyn SyncService, TestSyncService>()
            .add::<PullServiceImpl>()
            .build();

        let dataset_repo = catalog.get_one::<DatasetRepositoryLocalFs>().unwrap();
        let remote_repo_reg = catalog.get_one::<RemoteRepositoryRegistryImpl>().unwrap();
        let remote_alias_reg = catalog.get_one::<dyn RemoteAliasesRegistry>().unwrap();
        let pull_svc = catalog.get_one::<dyn PullService>().unwrap();

        Self {
            calls,
            dataset_repo,
            remote_repo_reg,
            remote_alias_reg,
            pull_svc,
        }
    }

    async fn create_dataset(&self, dataset_alias: &DatasetAlias) {
        self.dataset_repo
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(DatasetAlias::new(None, dataset_alias.dataset_name.clone()))
                    .build(),
            )
            .await
            .unwrap();
    }

    async fn num_blocks(&self, dataset_alias: &DatasetAlias) -> usize {
        let ds = self
            .dataset_repo
            .get_dataset(&dataset_alias.as_local_ref())
            .await
            .unwrap();

        use futures::StreamExt;
        ds.as_metadata_chain().iter_blocks().count().await
    }

    fn collect_calls(&self) -> Vec<PullBatch> {
        let mut calls = Vec::new();
        std::mem::swap(self.calls.lock().unwrap().as_mut(), &mut calls);
        calls
    }

    async fn pull(&self, refs: Vec<DatasetRefAny>, options: PullMultiOptions) -> Vec<PullBatch> {
        let results = self.pull_svc.pull_multi(refs, options, None).await.unwrap();

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

struct TestIngestService {
    calls: Arc<Mutex<Vec<PullBatch>>>,
}

impl TestIngestService {
    fn new(calls: Arc<Mutex<Vec<PullBatch>>>) -> Self {
        Self { calls }
    }
}

// TODO: Replace with a mock
#[async_trait::async_trait]
impl PollingIngestService for TestIngestService {
    async fn get_active_polling_source(
        &self,
        _dataset_ref: &DatasetRef,
    ) -> Result<Option<(Multihash, MetadataBlockTyped<SetPollingSource>)>, GetDatasetError> {
        unimplemented!()
    }

    async fn ingest(
        &self,
        _dataset_ref: &DatasetRef,
        _ingest_options: PollingIngestOptions,
        _maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResult, PollingIngestError> {
        unimplemented!();
    }

    async fn ingest_multi(
        &self,
        dataset_refs: Vec<DatasetRef>,
        _options: PollingIngestOptions,
        _listener: Option<Arc<dyn PollingIngestMultiListener>>,
    ) -> Vec<PollingIngestResponse> {
        let results = dataset_refs
            .iter()
            .map(|r| PollingIngestResponse {
                dataset_ref: r.clone(),
                result: Ok(PollingIngestResult::UpToDate {
                    no_source_defined: false,
                    uncacheable: false,
                }),
            })
            .collect();
        self.calls.lock().unwrap().push(PullBatch::Ingest(
            dataset_refs.into_iter().map(Into::into).collect(),
        ));
        results
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestTransformService {
    calls: Arc<Mutex<Vec<PullBatch>>>,
}

impl TestTransformService {
    pub fn new(calls: Arc<Mutex<Vec<PullBatch>>>) -> Self {
        Self { calls }
    }
}

#[async_trait::async_trait]
impl TransformService for TestTransformService {
    async fn get_active_transform(
        &self,
        _dataset_ref: &DatasetRef,
    ) -> Result<Option<(Multihash, MetadataBlockTyped<SetTransform>)>, GetDatasetError> {
        unimplemented!()
    }

    async fn transform(
        &self,
        _dataset_ref: &DatasetRef,
        _options: TransformOptions,
        _maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError> {
        unimplemented!();
    }

    async fn transform_multi(
        &self,
        dataset_refs: Vec<DatasetRef>,
        _options: TransformOptions,
        _maybe_multi_listener: Option<Arc<dyn TransformMultiListener>>,
    ) -> Vec<(DatasetRef, Result<TransformResult, TransformError>)> {
        let results = dataset_refs
            .iter()
            .map(|r| (r.clone(), Ok(TransformResult::UpToDate)))
            .collect();
        self.calls.lock().unwrap().push(PullBatch::Transform(
            dataset_refs.into_iter().map(Into::into).collect(),
        ));
        results
    }

    async fn verify_transform(
        &self,
        _dataset_ref: &DatasetRef,
        _block_range: (Option<Multihash>, Option<Multihash>),
        _listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<(), VerificationError> {
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestSyncService {
    calls: Arc<Mutex<Vec<PullBatch>>>,
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
}

#[dill::component(pub)]
impl TestSyncService {
    fn new(
        calls: Arc<Mutex<Vec<PullBatch>>>,
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    ) -> Self {
        Self {
            calls,
            dataset_repo,
            dataset_repo_writer,
        }
    }
}

#[async_trait::async_trait]
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
        requests: Vec<SyncRequest>,
        _options: SyncOptions,
        _listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<SyncResultMulti> {
        let mut call = Vec::new();
        let mut results = Vec::new();
        for SyncRequest { src, dst } in requests {
            call.push((src.clone(), dst.clone()));

            let local_ref = dst.as_local_single_tenant_ref().unwrap();

            match self
                .dataset_repo
                .try_resolve_dataset_ref(&local_ref)
                .await
                .unwrap()
            {
                None => {
                    self.dataset_repo_writer
                        .create_dataset_from_snapshot(
                            MetadataFactory::dataset_snapshot()
                                .name(local_ref.alias().unwrap().clone())
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
