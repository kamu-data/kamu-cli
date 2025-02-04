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
use kamu::domain::*;
use kamu::testing::{BaseRepoHarness, *};
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocol;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets_services::{
    CreateDatasetUseCaseImpl,
    DatasetEntryWriter,
    DependencyGraphWriter,
    MockDatasetEntryWriter,
    MockDependencyGraphWriter,
};
use messaging_outbox::DummyOutboxImpl;
use odf::dataset::testing::create_test_dataset_fron_snapshot;
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
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
        create_test_dataset_fron_snapshot(
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
    let tmp_repo_dir = tempfile::tempdir().unwrap();

    let remote_dataset_repo = Arc::new(DatasetStorageUnitLocalFs::new(
        tmp_repo_dir.path().to_owned(),
        Arc::new(CurrentAccountSubject::new_test()),
        Arc::new(TenancyConfig::SingleTenant),
    ));

    let dataset_registry = DatasetRegistrySoloUnitBridge::new(remote_dataset_repo.clone());

    create_graph(&dataset_registry, remote_dataset_repo.as_ref(), datasets).await;

    let tmp_repo_name = odf::RepoName::new_unchecked(remote_repo_name);

    harness
        .remote_repo_reg
        .add_repository(
            &tmp_repo_name,
            url::Url::from_file_path(tmp_repo_dir.path()).unwrap(),
        )
        .unwrap();

    for import_alias in to_import {
        harness
            .sync_service
            .sync(
                harness
                    .sync_request_builder
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
    let harness = PullTestHarness::new(
        TenancyConfig::SingleTenant,
        MockDatasetEntryWriter::new(),
        MockDependencyGraphWriter::new(),
    );

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
    let harness = PullTestHarness::new(
        TenancyConfig::MultiTenant,
        MockDatasetEntryWriter::new(),
        MockDependencyGraphWriter::new(),
    );

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
    let harness = PullTestHarness::new(
        TenancyConfig::SingleTenant,
        MockDatasetEntryWriter::new(),
        MockDependencyGraphWriter::new(),
    );

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
        .returning(|_, _, _| Ok(()));

    let mut mock_dependency_graph_writer = MockDependencyGraphWriter::new();
    mock_dependency_graph_writer
        .expect_create_dataset_node()
        .times(1)
        .returning(|_| Ok(()));

    let harness = PullTestHarness::new(
        TenancyConfig::SingleTenant,
        mock_dataset_entry_writer,
        mock_dependency_graph_writer,
    );

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
    let harness = PullTestHarness::new(
        TenancyConfig::SingleTenant,
        MockDatasetEntryWriter::new(),
        MockDependencyGraphWriter::new(),
    );

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
    let harness = PullTestHarness::new(
        TenancyConfig::SingleTenant,
        MockDatasetEntryWriter::new(),
        MockDependencyGraphWriter::new(),
    );

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
    let harness = PullTestHarness::new(
        TenancyConfig::MultiTenant,
        MockDatasetEntryWriter::new(),
        MockDependencyGraphWriter::new(),
    );

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
    let harness = PullTestHarness::new(
        TenancyConfig::SingleTenant,
        MockDatasetEntryWriter::new(),
        MockDependencyGraphWriter::new(),
    );

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
    let harness = PullTestHarness::new(
        TenancyConfig::MultiTenant,
        MockDatasetEntryWriter::new(),
        MockDependencyGraphWriter::new(),
    );

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

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct PullTestHarness {
    base_repo_harness: BaseRepoHarness,
    calls: Arc<Mutex<Vec<Vec<PullJob>>>>,
    sync_service: Arc<dyn SyncService>,
    sync_request_builder: Arc<SyncRequestBuilder>,
    remote_repo_reg: Arc<RemoteRepositoryRegistryImpl>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    pull_request_planner: Arc<dyn PullRequestPlanner>,
    tenancy_config: TenancyConfig,
}

impl PullTestHarness {
    fn new(
        tenancy_config: TenancyConfig,
        mock_dataset_entry_writer: MockDatasetEntryWriter,
        mock_dependency_graph_writer: MockDependencyGraphWriter,
    ) -> Self {
        let base_repo_harness = BaseRepoHarness::new(tenancy_config, None);

        let calls = Arc::new(Mutex::new(Vec::new()));

        let repos_dir = base_repo_harness.temp_dir_path().join("repos");
        std::fs::create_dir(&repos_dir).unwrap();

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add_value(RemoteRepositoryRegistryImpl::create(repos_dir).unwrap())
            .bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add::<PullRequestPlannerImpl>()
            .add::<TransformRequestPlannerImpl>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<SyncServiceImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add::<SyncRequestBuilder>()
            .add::<DatasetFactoryImpl>()
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add::<DummySmartTransferProtocolClient>()
            .add::<SimpleTransferProtocol>()
            .add::<CreateDatasetUseCaseImpl>()
            .add::<DummyOutboxImpl>()
            .add_value(IpfsClient::default())
            .add_value(IpfsGateway::default())
            .add_value(mock_dataset_entry_writer)
            .bind::<dyn DatasetEntryWriter, MockDatasetEntryWriter>()
            .add_value(mock_dependency_graph_writer)
            .bind::<dyn DependencyGraphWriter, MockDependencyGraphWriter>()
            .build();

        Self {
            base_repo_harness,
            calls,
            sync_service: catalog.get_one().unwrap(),
            sync_request_builder: catalog.get_one().unwrap(),
            remote_repo_reg: catalog.get_one().unwrap(),
            remote_alias_reg: catalog.get_one().unwrap(),
            pull_request_planner: catalog.get_one().unwrap(),
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
                };
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
