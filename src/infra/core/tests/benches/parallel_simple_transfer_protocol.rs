// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use kamu::domain::*;
use kamu::testing::{
    DatasetTestHelper,
    DummySmartTransferProtocolClient,
    HttpFileServer,
    MetadataFactory,
};
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::{
    DatasetFactoryImpl,
    DatasetRepositoryLocalFs,
    IpfsGateway,
    RemoteRepositoryRegistryImpl,
    SyncServiceImpl,
};
use opendatafabric::*;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

async fn setup_dataset(
    tmp_workspace_dir: &Path,
    dataset_alias: Arc<DatasetAlias>,
    ipfs: Option<(IpfsGateway, IpfsClient)>,
) -> (Arc<SyncServiceImpl>, Arc<DatasetRepositoryLocalFs>) {
    let (ipfs_gateway, ipfs_client) = ipfs.unwrap_or_default();

    let dataset_authorizer = Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new());

    let dataset_repo = Arc::new(
        DatasetRepositoryLocalFs::create(
            tmp_workspace_dir.join("datasets"),
            Arc::new(CurrentAccountSubject::new_test()),
            dataset_authorizer.clone() as Arc<dyn auth::DatasetActionAuthorizer>,
            false,
        )
        .unwrap(),
    );

    let remote_repo_reg =
        Arc::new(RemoteRepositoryRegistryImpl::create(tmp_workspace_dir.join("repos")).unwrap());
    let dataset_factory = Arc::new(DatasetFactoryImpl::new(
        ipfs_gateway,
        Arc::new(auth::DummyOdfServerAccessTokenResolver::new()),
    ));

    let sync_svc = SyncServiceImpl::new(
        remote_repo_reg.clone(),
        dataset_repo.clone(),
        dataset_authorizer.clone(),
        dataset_factory,
        Arc::new(DummySmartTransferProtocolClient::new()),
        Arc::new(ipfs_client),
    );

    // Add dataset
    let snapshot = MetadataFactory::dataset_snapshot()
        .name(&dataset_alias.dataset_name)
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_data_schema().build())
        .build();

    let _ = dataset_repo
        .create_dataset_from_snapshot(None, snapshot)
        .await
        .unwrap()
        .head;

    // Generate initial 70 blocks in dataset
    for _ in 1..70 {
        let _ = DatasetTestHelper::append_random_data(dataset_repo.as_ref(), &*dataset_alias).await;
    }

    (Arc::new(sync_svc), dataset_repo)
}

async fn do_test_sync(
    sync_svc: Arc<SyncServiceImpl>,
    dataset_alias: Arc<DatasetAlias>,
    pull_repo_url: &DatasetRefRemote,
    push_repo_url: &DatasetRefRemote,
    dataset_repo: Arc<DatasetRepositoryLocalFs>,
) {
    let _push_res = sync_svc
        .sync(
            &dataset_alias.as_any_ref(),
            &push_repo_url.as_any_ref(),
            SyncOptions::default(),
            None,
        )
        .await;

    let _pull_res = sync_svc
        .sync(
            &pull_repo_url.as_any_ref(),
            &dataset_alias.as_any_ref(),
            SyncOptions::default(),
            None,
        )
        .await;

    // Generate additional 70 blocks in dataset to make sure next iteration will be
    // the same as previos
    for _ in 1..70 {
        let _ = DatasetTestHelper::append_random_data(dataset_repo.as_ref(), &*dataset_alias).await;
    }
}

fn build_temp_dirs(rt: Arc<tokio::runtime::Runtime>) -> (Arc<DatasetAlias>, Url, Url) {
    let tmp_repo_dir = tempfile::tempdir().unwrap();

    // to perfrom multithreded operation (initialization server) rt.enter menthod
    // need to be called
    let _guard = rt.enter();
    let server = HttpFileServer::new(tmp_repo_dir.path());
    let pull_repo_url = Url::from_str(&format!("http://{}/", server.local_addr())).unwrap();
    let push_repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    let dataset_alias = Arc::new(DatasetAlias::new(None, DatasetName::new_unchecked("foo")));
    rt.spawn(server.run());

    (dataset_alias, pull_repo_url, push_repo_url)
}

/////////////////////////////////////////////////////////////////////////////////////////

fn bench_with_1_parallel(c: &mut Criterion) {
    std::env::set_var("SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS", "1");
    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let tmp_workspace_dir = tempfile::tempdir().unwrap();

    let (dataset_alias, pull_repo_url, push_repo_url) = build_temp_dirs(rt.clone());

    let (sync_service_impl, dataset_repo) = rt.block_on(setup_dataset(
        &tmp_workspace_dir.path(),
        dataset_alias.clone(),
        None,
    ));

    let mut group = c.benchmark_group("parallel_1_simple_transfer_protocol");
    // Min size of iterations is 10
    group.sample_size(10);
    group.bench_function("transfer", |b| {
        b.iter(|| {
            rt.block_on(do_test_sync(
                sync_service_impl.clone(),
                dataset_alias.clone(),
                &DatasetRefRemote::from(&pull_repo_url),
                &DatasetRefRemote::from(&push_repo_url),
                dataset_repo.clone(),
            ))
        });
    });
}

fn bench_with_10_parallels(c: &mut Criterion) {
    std::env::set_var("SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS", "10");
    let rt: Arc<tokio::runtime::Runtime> = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let tmp_workspace_dir = tempfile::tempdir().unwrap();

    let (dataset_alias, pull_repo_url, push_repo_url) = build_temp_dirs(rt.clone());

    let (sync_service_impl, dataset_repo) = rt.block_on(setup_dataset(
        &tmp_workspace_dir.path(),
        dataset_alias.clone(),
        None,
    ));

    let mut group = c.benchmark_group("parallel_10_simple_transfer_protocol");
    // Min size of iterations is 10
    group.sample_size(10);
    group.bench_function("transfer", |b| {
        b.iter(|| {
            rt.block_on(do_test_sync(
                sync_service_impl.clone(),
                dataset_alias.clone(),
                &DatasetRefRemote::from(&pull_repo_url),
                &DatasetRefRemote::from(&push_repo_url),
                dataset_repo.clone(),
            ))
        });
    });
}

/////////////////////////////////////////////////////////////////////////////////////////

criterion_group!(benches, bench_with_1_parallel, bench_with_10_parallels);
criterion_main!(benches);
