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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use dill::Component;
use futures::TryStreamExt;
use kamu::domain::*;
use kamu::testing::{BaseRepoHarness, *};
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocol;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::{DatasetReferenceMessage, MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE};
use kamu_datasets_inmem::{
    InMemoryDatasetDependencyRepository,
    InMemoryDatasetReferenceRepository,
};
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::{
    AppendDatasetMetadataBatchUseCaseImpl,
    CreateDatasetUseCaseImpl,
    DatasetEntryWriter,
    DatasetReferenceServiceImpl,
    DependencyGraphIndexer,
    DependencyGraphServiceImpl,
    MockDatasetEntryWriter,
};
use messaging_outbox::{ConsumerFilter, Outbox, OutboxImmediateImpl, register_message_dispatcher};
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! n {
    ($s:expr) => {
        odf::DatasetAlias::new(None, odf::DatasetName::try_from($s).unwrap())
    };
}

macro_rules! mn {
    ($s:expr) => {
        odf::DatasetAlias::try_from($s).unwrap()
    };
}

macro_rules! rl {
    ($s:expr) => {
        odf::DatasetRef::Alias(odf::DatasetAlias::new(
            None,
            odf::DatasetName::try_from($s).unwrap(),
        ))
    };
}

macro_rules! rr {
    ($s:expr) => {
        odf::DatasetRefRemote::try_from($s).unwrap()
    };
}

macro_rules! ar {
    ($s:expr) => {
        odf::DatasetRefAny::try_from($s).unwrap()
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_graph(
    dataset_registry: &dyn DatasetRegistry,
    dataset_storage_unit: &dyn odf::DatasetStorageUnitWriter,
    datasets: Vec<(odf::DatasetAlias, Vec<odf::DatasetAlias>)>,
) {
    for (dataset_alias, deps) in datasets {
        create_test_dataset_from_snapshot(
            dataset_registry,
            dataset_storage_unit,
            MetadataFactory::dataset_snapshot()
                .name(dataset_alias)
                .kind(if deps.is_empty() {
                    odf::DatasetKind::Root
                } else {
                    odf::DatasetKind::Derivative
                })
                .push_event::<odf::MetadataEvent>(if deps.is_empty() {
                    MetadataFactory::set_polling_source().build().into()
                } else {
                    MetadataFactory::set_transform()
                        .inputs_from_refs(deps)
                        .build()
                        .into()
                })
                .build(),
            odf::DatasetID::new_generated_ed25519().1,
            Utc::now(),
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
    harness: &PullTestHarness,
    datasets: Vec<(odf::DatasetAlias, Vec<odf::DatasetAlias>)>,
    to_import: Vec<odf::DatasetAlias>,
) -> tempfile::TempDir {
    // First, create a temporary managed repository, and fill it with datasets.
    // It will have a unified id-based structure
    let tmp_registry_dir = tempfile::tempdir().unwrap();

    let (tmp_dataset_registry, tmp_storage_unit) = {
        let catalog = dill::CatalogBuilder::new()
            .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                tmp_registry_dir.path().to_owned(),
            ))
            .add::<odf::dataset::DatasetLfsBuilderDefault>()
            .add::<DatasetRegistrySoloUnitBridge>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(TenancyConfig::SingleTenant)
            .build();

        (
            catalog.get_one::<dyn DatasetRegistry>().unwrap(),
            catalog
                .get_one::<dyn odf::DatasetStorageUnitWriter>()
                .unwrap(),
        )
    };

    create_graph(
        tmp_dataset_registry.as_ref(),
        tmp_storage_unit.as_ref(),
        datasets,
    )
    .await;

    // Now, let's organize a simple local FS remote repo by copying those datasets.
    // Unlike unified structure, this one is single-tenant and name-based.
    let tmp_repo_dir = tempfile::tempdir().unwrap();

    {
        let mut hdl_stream = tmp_dataset_registry.all_dataset_handles();
        while let Some(hdl) = hdl_stream.try_next().await.unwrap() {
            let src_path = tmp_registry_dir
                .path()
                .join(hdl.id.as_multibase().to_stack_string());
            let src_dataset_layout = odf::dataset::DatasetLayout::new(src_path);

            let dst_path = tmp_repo_dir.path().join(hdl.alias.dataset_name);
            let dst_dataset_layout = odf::dataset::DatasetLayout::new(dst_path);

            copy_dataset_files(&src_dataset_layout, &dst_dataset_layout).unwrap();
        }
    }

    // Register a remote repository pointing to this formed directory
    let tmp_repo_name = odf::RepoName::new_unchecked(remote_repo_name);
    harness
        .remote_repo_reg
        .add_repository(
            &tmp_repo_name,
            url::Url::from_file_path(tmp_repo_dir.path()).unwrap(),
        )
        .unwrap();

    // Start syncing with main repository
    for import_alias in to_import {
        let remote_alias = import_alias.as_remote_alias(tmp_repo_name.clone());
        harness
            .sync_service
            .sync(
                harness
                    .sync_request_builder
                    .build_sync_request(remote_alias.as_any_ref(), import_alias.as_any_ref(), true)
                    .await
                    .unwrap(),
                SyncOptions::default(),
                None,
            )
            .await
            .unwrap();

        // Add remote pull alias to E
        harness
            .get_remote_aliases(&import_alias.as_local_ref())
            .await
            .add(&remote_alias.as_remote_ref(), RemoteAliasKind::Pull)
            .await
            .unwrap();
    }

    tmp_repo_dir
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn copy_folder_recursively(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    if src.exists() {
        std::fs::create_dir_all(dst)?;
        let copy_options = fs_extra::dir::CopyOptions::new().content_only(true);
        fs_extra::dir::copy(src, dst, &copy_options).unwrap();
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn copy_dataset_files(
    src_layout: &odf::dataset::DatasetLayout,
    dst_layout: &odf::dataset::DatasetLayout,
) -> std::io::Result<()> {
    // Don't copy `info`
    copy_folder_recursively(&src_layout.blocks_dir, &dst_layout.blocks_dir)?;
    copy_folder_recursively(&src_layout.checkpoints_dir, &dst_layout.checkpoints_dir)?;
    copy_folder_recursively(&src_layout.data_dir, &dst_layout.data_dir)?;
    copy_folder_recursively(&src_layout.refs_dir, &dst_layout.refs_dir)?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_batching_chain() {
    let harness = PullTestHarness::new(TenancyConfig::SingleTenant, MockDatasetEntryWriter::new());

    // A - B - C
    create_graph(
        harness.dataset_registry(),
        harness.dataset_storage_unit_writer(),
        vec![
            (n!("a"), names![]),
            (n!("b"), names!["a"]),
            (n!("c"), names!["b"]),
        ],
    )
    .await;

    harness.index_dataset_dependencies().await;

    assert_eq!(
        harness
            .pull(refs!["c"], PullOptions::default())
            .await
            .unwrap(),
        vec![vec![PullJob::Transform(ar!["c"])]]
    );

    assert_eq!(
        harness
            .pull(refs!["c", "a"], PullOptions::default())
            .await
            .unwrap(),
        vec![
            vec![PullJob::Ingest(ar!["a"])],
            vec![PullJob::Transform(ar!["c"])]
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
            vec![PullJob::Ingest(ar!["a"])],
            vec![PullJob::Transform(ar!["b"])],
            vec![PullJob::Transform(ar!["c"])],
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_batching_chain_multi_tenant() {
    let harness = PullTestHarness::new(TenancyConfig::MultiTenant, MockDatasetEntryWriter::new());

    // XA - YB - ZC
    create_graph(
        harness.dataset_registry(),
        harness.dataset_storage_unit_writer(),
        vec![
            (mn!("x/a"), mnames![]),
            (mn!("y/b"), mnames!["x/a"]),
            (mn!("z/c"), mnames!["y/b"]),
        ],
    )
    .await;

    harness.index_dataset_dependencies().await;

    assert_eq!(
        harness
            .pull(refs!["z/c"], PullOptions::default())
            .await
            .unwrap(),
        vec![vec![PullJob::Transform(mn!["z/c"].as_any_ref())]]
    );

    assert_eq!(
        harness
            .pull(refs!["z/c", "x/a"], PullOptions::default())
            .await
            .unwrap(),
        vec![
            vec![PullJob::Ingest(mn!["x/a"].as_any_ref())],
            vec![PullJob::Transform(mn!["z/c"].as_any_ref())],
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
            vec![PullJob::Ingest(mn!["x/a"].as_any_ref())],
            vec![PullJob::Transform(mn!["y/b"].as_any_ref())],
            vec![PullJob::Transform(mn!["z/c"].as_any_ref())],
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_batching_complex() {
    let harness = PullTestHarness::new(TenancyConfig::SingleTenant, MockDatasetEntryWriter::new());

    //    / C \
    // A <     > > E
    //    \ D / /
    //         /
    // B - - -/
    create_graph(
        harness.dataset_registry(),
        harness.dataset_storage_unit_writer(),
        vec![
            (n!("a"), names![]),
            (n!("b"), names![]),
            (n!("c"), names!["a"]),
            (n!("d"), names!["a"]),
            (n!("e"), names!["c", "d", "b"]),
        ],
    )
    .await;

    harness.index_dataset_dependencies().await;

    assert_eq!(
        harness
            .pull(refs!["e"], PullOptions::default())
            .await
            .unwrap(),
        vec![vec![PullJob::Transform(ar!["e"])]]
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
            vec![PullJob::Ingest(ar!["a"]), PullJob::Ingest(ar!["b"])],
            vec![PullJob::Transform(ar!["c"]), PullJob::Transform(ar!["d"])],
            vec![PullJob::Transform(ar!["e"])],
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_batching_complex_with_remote() {
    let mut mock_dataset_entry_writer = MockDatasetEntryWriter::new();
    mock_dataset_entry_writer
        .expect_create_entry()
        .times(1)
        .returning(|_, _, _, _, _| Ok(()));

    let harness = PullTestHarness::new(TenancyConfig::SingleTenant, mock_dataset_entry_writer);

    // (A) - (E) - F - G
    // (B) --/    /   /
    // C --------/   /
    // D -----------/
    let _remote_tmp_dir = create_graph_remote(
        "kamu.dev",
        &harness,
        vec![
            (n!("a"), names![]),
            (n!("b"), names![]),
            (n!("e"), names!["a", "b"]),
        ],
        names!("e"),
    )
    .await;

    create_graph(
        harness.dataset_registry(),
        harness.dataset_storage_unit_writer(),
        vec![
            (n!("c"), names![]),
            (n!("d"), names![]),
            (n!("f"), names!["e", "c"]),
            (n!("g"), names!["f", "d"]),
        ],
    )
    .await;

    harness.index_dataset_dependencies().await;

    // Add remote pull alias to E
    harness
        .get_remote_aliases(&rl!("e"))
        .await
        .add(
            &odf::DatasetRefRemote::try_from("kamu.dev/e").unwrap(),
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
        vec![vec![PullJob::Sync((
            rr!("kamu.dev/e").into(),
            n!("e").into()
        ))]],
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
        vec![vec![PullJob::Sync((
            rr!("kamu.dev/e").into(),
            n!("e").into()
        ))]],
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
            vec![
                PullJob::Sync((rr!("kamu.dev/e").into(), n!("e").into())),
                PullJob::Ingest(ar!("c")),
                PullJob::Ingest(ar!("d")),
            ],
            vec![PullJob::Transform(ar!("f"))],
            vec![PullJob::Transform(ar!("g"))],
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
            vec![
                PullJob::Sync((rr!("kamu.dev/e").into(), n!("e").into())),
                PullJob::Ingest(ar!("c")),
                PullJob::Ingest(ar!("d"))
            ],
            vec![PullJob::Transform(ar!("f"))],
            vec![PullJob::Transform(ar!("g"))],
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
            vec![
                PullJob::Sync((rr!("kamu.dev/e").into(), n!("e").into())),
                PullJob::Ingest(ar!("c")),
                PullJob::Ingest(ar!("d"))
            ],
            vec![PullJob::Transform(ar!("f"))],
            vec![PullJob::Transform(ar!("g"))],
        ],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from() {
    let harness = PullTestHarness::new(TenancyConfig::SingleTenant, MockDatasetEntryWriter::new());

    let _remote_tmp_dir =
        create_graph_remote("kamu.dev", &harness, vec![(n!("foo"), names![])], names!()).await;

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
        vec![vec![PullJob::Sync((
            rr!("kamu.dev/foo").into(),
            n!("bar").into()
        ))]]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from_url_and_local_ref() {
    let harness = PullTestHarness::new(TenancyConfig::SingleTenant, MockDatasetEntryWriter::new());

    let _remote_tmp_dir =
        create_graph_remote("kamu.dev", &harness, vec![(n!("bar"), names![])], names!()).await;

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
        vec![vec![PullJob::Sync((
            rr!("kamu.dev/bar").into(),
            n!("bar").into()
        ))]]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from_url_and_local_multi_tenant_ref() {
    let harness = PullTestHarness::new(TenancyConfig::MultiTenant, MockDatasetEntryWriter::new());

    let _remote_tmp_dir =
        create_graph_remote("kamu.dev", &harness, vec![(n!("bar"), names![])], names!()).await;

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
        vec![vec![PullJob::Sync((
            rr!("kamu.dev/bar").into(),
            mn!("x/bar").into()
        ))]]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from_url_only() {
    let harness = PullTestHarness::new(TenancyConfig::SingleTenant, MockDatasetEntryWriter::new());

    let _remote_tmp_dir =
        create_graph_remote("kamu.dev", &harness, vec![(n!("bar"), names![])], names!()).await;

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
        vec![vec![PullJob::Sync((
            rr!("kamu.dev/bar").into(),
            n!("bar").into()
        ))]]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from_url_only_multi_tenant_case() {
    let harness = PullTestHarness::new(TenancyConfig::MultiTenant, MockDatasetEntryWriter::new());

    let _remote_tmp_dir =
        create_graph_remote("kamu.dev", &harness, vec![(n!("bar"), names![])], names!()).await;

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
        vec![vec![PullJob::Sync((
            rr!("kamu.dev/bar").into(),
            n!("bar").into()
        ))]]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from_url_same_id_different_aliases() {
    let mut mock_dataset_entry_writer = MockDatasetEntryWriter::new();
    mock_dataset_entry_writer
        .expect_create_entry()
        .times(1)
        .returning(|_, _, _, _, _| Ok(()));

    let harness = PullTestHarness::new(TenancyConfig::SingleTenant, mock_dataset_entry_writer);

    let _remote_tmp_dir = create_graph_remote(
        "kamu.dev",
        &harness,
        vec![(n!("bar"), names![])],
        names!("bar"),
    )
    .await;

    let res = harness
        .pull_with_requests(
            vec![PullRequest::Remote(PullRequestRemote {
                maybe_local_alias: Some(n!("foo")),
                remote_ref: rr!("kamu.dev/bar"),
            })],
            PullOptions::default(),
        )
        .await;
    assert!(res.is_err());
    let responses = res.err().unwrap();
    assert_eq!(1, responses.len());
    assert_matches!(
        responses.first().unwrap().result,
        Err(PullError::SaveUnderDifferentAlias(ref diff_alias))
            if diff_alias.as_str() == "bar"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from_same_url_different_accounts() {
    let mut mock_dataset_entry_writer = MockDatasetEntryWriter::new();
    mock_dataset_entry_writer
        .expect_create_entry()
        .times(1)
        .returning(|_, _, _, _, _| Ok(()));

    let harness = PullTestHarness::new(TenancyConfig::MultiTenant, mock_dataset_entry_writer);

    let _remote_tmp_dir = create_graph_remote(
        "kamu.dev",
        &harness,
        vec![(n!("bar"), names![])],
        names!("bar"),
    )
    .await;

    let res = harness
        .pull_with_requests(
            vec![PullRequest::Remote(PullRequestRemote {
                maybe_local_alias: Some(n!("foo")),
                remote_ref: rr!("kamu.dev/bar"),
            })],
            PullOptions::default(),
        )
        .await;
    assert!(res.is_err());
    let responses = res.err().unwrap();
    assert_eq!(1, responses.len());
    assert_matches!(
        responses.first().unwrap().result,
        Err(PullError::SaveUnderDifferentAlias(ref diff_alias))
            if diff_alias.as_str() == "kamu/bar"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct PullTestHarness {
    base_repo_harness: BaseRepoHarness,
    calls: Arc<Mutex<Vec<Vec<PullJob>>>>,
    sync_service: Arc<dyn SyncService>,
    sync_request_builder: Arc<SyncRequestBuilder>,
    remote_repo_reg: Arc<RemoteRepositoryRegistryImpl>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    pull_request_planner: Arc<dyn PullRequestPlanner>,
    dependency_graph_indexer: Arc<DependencyGraphIndexer>,
    tenancy_config: TenancyConfig,
}

impl PullTestHarness {
    fn new(
        tenancy_config: TenancyConfig,
        mock_dataset_entry_writer: MockDatasetEntryWriter,
    ) -> Self {
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(tenancy_config)
            .build();

        let calls = Arc::new(Mutex::new(Vec::new()));

        let repos_dir = base_repo_harness.temp_dir_path().join("repos");
        std::fs::create_dir(&repos_dir).unwrap();

        let mut b = dill::CatalogBuilder::new_chained(base_repo_harness.catalog());
        b.add::<RemoteRepositoryRegistryImpl>()
            .add_value(RemoteReposDir::new(repos_dir))
            .add::<RemoteAliasesRegistryImpl>()
            .add::<PullRequestPlannerImpl>()
            .add::<TransformRequestPlannerImpl>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<SyncServiceImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add::<SyncRequestBuilder>()
            .add::<odf::dataset::DatasetFactoryImpl>()
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add::<DummySmartTransferProtocolClient>()
            .add::<SimpleTransferProtocol>()
            .add::<CreateDatasetUseCaseImpl>()
            .add::<CreateDatasetUseCaseHelper>()
            .add::<AppendDatasetMetadataBatchUseCaseImpl>()
            .add_builder(
                OutboxImmediateImpl::builder().with_consumer_filter(ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add_value(IpfsClient::default())
            .add_value(odf::dataset::IpfsGateway::default())
            .add_value(mock_dataset_entry_writer)
            .bind::<dyn DatasetEntryWriter, MockDatasetEntryWriter>()
            .add::<DatasetReferenceServiceImpl>()
            .add::<InMemoryDatasetReferenceRepository>()
            .add::<DependencyGraphServiceImpl>()
            .add::<InMemoryDatasetDependencyRepository>()
            .add::<DependencyGraphIndexer>();

        register_message_dispatcher::<DatasetReferenceMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        );

        let catalog = b.build();

        Self {
            base_repo_harness,
            calls,
            sync_service: catalog.get_one().unwrap(),
            sync_request_builder: catalog.get_one().unwrap(),
            remote_repo_reg: catalog.get_one().unwrap(),
            remote_alias_reg: catalog.get_one().unwrap(),
            pull_request_planner: catalog.get_one().unwrap(),
            dependency_graph_indexer: catalog.get_one().unwrap(),
            tenancy_config,
        }
    }

    fn collect_calls(&self) -> Vec<Vec<PullJob>> {
        let mut calls = Vec::new();
        std::mem::swap(self.calls.lock().unwrap().as_mut(), &mut calls);
        calls
    }

    async fn pull(
        &self,
        refs: Vec<odf::DatasetRefAny>,
        options: PullOptions,
    ) -> Result<Vec<Vec<PullJob>>, Vec<PullResponse>> {
        let requests: Vec<_> = refs
            .into_iter()
            .map(|r| {
                PullRequest::from_any_ref(&r, |_| {
                    self.tenancy_config == TenancyConfig::SingleTenant
                })
            })
            .collect();
        self.pull_with_requests(requests, options).await
    }

    async fn pull_with_requests(
        &self,
        requests: Vec<PullRequest>,
        options: PullOptions,
    ) -> Result<Vec<Vec<PullJob>>, Vec<PullResponse>> {
        let (plan_iterations, errors) = self
            .pull_request_planner
            .build_pull_multi_plan(&requests, &options, self.tenancy_config)
            .await;
        if !errors.is_empty() {
            return Err(errors);
        }

        for iteration in plan_iterations {
            let mut jobs = Vec::new();
            for job in iteration.jobs {
                match job {
                    PullPlanIterationJob::Ingest(pii) => {
                        jobs.push(PullJob::Ingest(pii.target.get_handle().as_any_ref()));
                    }
                    PullPlanIterationJob::Transform(pti) => {
                        jobs.push(PullJob::Transform(pti.target.get_handle().as_any_ref()));
                    }
                    PullPlanIterationJob::Sync(psi) => {
                        jobs.push(PullJob::Sync((
                            psi.sync_request.src.as_user_friendly_any_ref(),
                            psi.sync_request.dst.as_user_friendly_any_ref(),
                        )));
                    }
                }
            }

            self.calls.lock().unwrap().push(jobs);
        }

        tokio::time::sleep(Duration::from_millis(1)).await;

        Ok(self.collect_calls())
    }

    async fn get_remote_aliases(&self, dataset_ref: &odf::DatasetRef) -> Box<dyn RemoteAliases> {
        let hdl = self
            .dataset_registry()
            .resolve_dataset_handle_by_ref(dataset_ref)
            .await
            .unwrap();
        self.remote_alias_reg
            .get_remote_aliases(&hdl)
            .await
            .unwrap()
    }

    async fn index_dataset_dependencies(&self) {
        use init_on_startup::InitOnStartup;
        self.dependency_graph_indexer
            .run_initialization()
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq)]
enum PullJob {
    Ingest(odf::DatasetRefAny),
    Transform(odf::DatasetRefAny),
    Sync((odf::DatasetRefAny, odf::DatasetRefAny)),
}

impl PullJob {
    fn cmp_ref(lhs: &odf::DatasetRefAny, rhs: &odf::DatasetRefAny) -> bool {
        #[allow(clippy::type_complexity)]
        fn tuplify(
            v: &odf::DatasetRefAny,
        ) -> (
            Option<&odf::DatasetID>,
            Option<&url::Url>,
            Option<&str>,
            Option<&str>,
            Option<&odf::DatasetName>,
        ) {
            match v {
                odf::DatasetRefAny::ID(_, id) => (Some(id), None, None, None, None),
                odf::DatasetRefAny::Url(url) => (None, Some(url), None, None, None),
                odf::DatasetRefAny::LocalAlias(a, n) => {
                    (None, None, None, a.as_ref().map(AsRef::as_ref), Some(n))
                }
                odf::DatasetRefAny::RemoteAlias(r, a, n) => (
                    None,
                    None,
                    Some(r.as_ref()),
                    a.as_ref().map(AsRef::as_ref),
                    Some(n),
                ),
                odf::DatasetRefAny::AmbiguousAlias(ra, n) => {
                    (None, None, Some(ra.as_ref()), None, Some(n))
                }
                odf::DatasetRefAny::LocalHandle(h) => (
                    None,
                    None,
                    None,
                    h.alias.account_name.as_ref().map(odf::AccountName::as_str),
                    Some(&h.alias.dataset_name),
                ),
                odf::DatasetRefAny::RemoteHandle(h) => (
                    None,
                    None,
                    Some(h.alias.repo_name.as_str()),
                    h.alias.account_name.as_ref().map(odf::AccountName::as_str),
                    Some(&h.alias.dataset_name),
                ),
            }
        }
        tuplify(lhs) == tuplify(rhs)
    }
}

impl std::cmp::PartialEq for PullJob {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Transform(l), Self::Transform(r)) | (Self::Ingest(l), Self::Ingest(r)) => {
                Self::cmp_ref(l, r)
            }
            (Self::Sync(l), Self::Sync(r)) => {
                Self::cmp_ref(&l.0, &r.0) && Self::cmp_ref(&l.1, &r.1)
            }
            _ => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
