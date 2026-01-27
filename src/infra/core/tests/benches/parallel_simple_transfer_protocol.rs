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

use chrono::Utc;
use criterion::{Criterion, criterion_group, criterion_main};
use kamu::domain::*;
use kamu::testing::{DatasetTestHelper, DummySmartTransferProtocolClient};
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::utils::simple_transfer_protocol::{
    ENV_VAR_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS,
    SimpleTransferProtocol,
};
use kamu::{
    DatasetRegistrySoloUnitBridge,
    RemoteReposDir,
    RemoteRepositoryRegistryImpl,
    SyncRequestBuilder,
    SyncServiceImpl,
};
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::{AlwaysHappyDatasetActionAuthorizer, DatasetRegistry};
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::DependencyGraphServiceImpl;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
use odf::metadata::testing::MetadataFactory;
use test_utils::HttpFileServer;
use time_source::SystemTimeSourceDefault;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FILE_DATA_ARRAY_SIZE: usize = 1_572_864;
const AMOUNT_OF_BLOCKS_TO_APPEND: usize = 70;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn setup_dataset(
    tmp_workspace_dir: &Path,
    dataset_alias: &odf::DatasetAlias,
    ipfs: Option<(IpfsGateway, IpfsClient)>,
) -> (
    Arc<dyn SyncService>,
    Arc<SyncRequestBuilder>,
    Arc<dyn DatasetRegistry>,
) {
    let (ipfs_gateway, ipfs_client) = ipfs.unwrap_or_default();

    let datasets_dir = tmp_workspace_dir.join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    let repos_dir = tmp_workspace_dir.join("repos");
    std::fs::create_dir(&repos_dir).unwrap();

    let catalog = dill::CatalogBuilder::new()
        .add::<DependencyGraphServiceImpl>()
        .add::<InMemoryDatasetDependencyRepository>()
        .add_value(ipfs_gateway)
        .add_value(ipfs_client)
        .add_value(CurrentAccountSubject::new_test())
        .add_value(TenancyConfig::SingleTenant)
        .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
            datasets_dir,
        ))
        .add::<DatasetRegistrySoloUnitBridge>()
        .add_value(RemoteReposDir::new(repos_dir))
        .add::<RemoteRepositoryRegistryImpl>()
        .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
        .add::<DatasetFactoryImpl>()
        .add::<SyncServiceImpl>()
        .add::<SyncRequestBuilder>()
        .add::<SystemTimeSourceDefault>()
        .add::<DummySmartTransferProtocolClient>()
        .add::<AlwaysHappyDatasetActionAuthorizer>()
        .add::<SimpleTransferProtocol>()
        .build();

    init_on_startup::run_startup_jobs(&catalog).await.unwrap();

    let sync_svc = catalog.get_one::<dyn SyncService>().unwrap();
    let sync_request_builder = catalog.get_one::<SyncRequestBuilder>().unwrap();
    let dataset_registry = catalog.get_one::<dyn DatasetRegistry>().unwrap();
    let dataset_storage_unit_writer = catalog
        .get_one::<dyn odf::DatasetStorageUnitWriter>()
        .unwrap();

    // Add dataset
    let snapshot = MetadataFactory::dataset_snapshot()
        .name(dataset_alias.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_data_schema().build())
        .build();

    let _ = create_test_dataset_from_snapshot(
        dataset_registry.as_ref(),
        dataset_storage_unit_writer.as_ref(),
        snapshot,
        odf::DatasetID::new_generated_ed25519().1,
        Utc::now(),
    )
    .await
    .unwrap();

    append_data_to_dataset(
        AMOUNT_OF_BLOCKS_TO_APPEND,
        dataset_registry.as_ref(),
        dataset_alias,
    )
    .await;

    (sync_svc, sync_request_builder, dataset_registry)
}

async fn append_data_to_dataset(
    block_amount: usize,
    dataset_registry: &dyn DatasetRegistry,
    dataset_ref: &odf::DatasetAlias,
) {
    for _ in 1..block_amount {
        let _ = DatasetTestHelper::append_random_data(
            dataset_registry,
            dataset_ref,
            FILE_DATA_ARRAY_SIZE,
        )
        .await;
    }
}

async fn do_test_sync(
    sync_svc: Arc<dyn SyncService>,
    sync_request_builder: Arc<SyncRequestBuilder>,
    dataset_alias: &odf::DatasetAlias,
    pull_repo_url: &odf::DatasetRefRemote,
    push_repo_url: &odf::DatasetRefRemote,
    dataset_registry: Arc<dyn DatasetRegistry>,
) {
    let _push_res = sync_svc
        .sync(
            sync_request_builder
                .build_sync_request(dataset_alias.as_any_ref(), push_repo_url.as_any_ref(), true)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await;

    let _pull_res = sync_svc
        .sync(
            sync_request_builder
                .build_sync_request(pull_repo_url.as_any_ref(), dataset_alias.as_any_ref(), true)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await;

    // Generate additional 70 blocks in dataset to make sure next iteration will be
    // the same as previous
    append_data_to_dataset(
        AMOUNT_OF_BLOCKS_TO_APPEND,
        dataset_registry.as_ref(),
        dataset_alias,
    )
    .await;
}

async fn build_temp_dirs(rt: &tokio::runtime::Runtime) -> (odf::DatasetAlias, Url, Url) {
    let tmp_repo_dir = tempfile::tempdir().unwrap();

    // to perform multithreading operation (initialization server) rt.enter method
    // need to be called
    let _guard = rt.enter();
    let server = HttpFileServer::new(tmp_repo_dir.path()).await;
    let pull_repo_url = Url::from_str(&format!("http://{}/", server.local_addr())).unwrap();
    let push_repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    let dataset_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    rt.spawn(server.run());

    (dataset_alias, pull_repo_url, push_repo_url)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn bench_with_1_parallel(c: &mut Criterion) {
    unsafe {
        std::env::set_var(ENV_VAR_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS, "1");
    }

    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let tmp_workspace_dir = tempfile::tempdir().unwrap();

    let (dataset_alias, pull_repo_url, push_repo_url) = rt.block_on(build_temp_dirs(&rt));

    let (sync_service_impl, sync_request_builder, dataset_registry) = rt.block_on(setup_dataset(
        tmp_workspace_dir.path(),
        &dataset_alias,
        None,
    ));

    let mut group = c.benchmark_group("parallel_1_simple_transfer_protocol");
    // Min size of iterations is 10
    group.sample_size(10);
    group.bench_function("transfer", |b| {
        b.iter(|| {
            rt.block_on(do_test_sync(
                sync_service_impl.clone(),
                sync_request_builder.clone(),
                &dataset_alias,
                &odf::DatasetRefRemote::from(&pull_repo_url),
                &odf::DatasetRefRemote::from(&push_repo_url),
                dataset_registry.clone(),
            ));
        });
    });
}

fn bench_with_10_parallels(c: &mut Criterion) {
    unsafe {
        std::env::set_var(ENV_VAR_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS, "10");
    }

    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let tmp_workspace_dir = tempfile::tempdir().unwrap();

    let (dataset_alias, pull_repo_url, push_repo_url) = rt.block_on(build_temp_dirs(&rt));

    let (sync_service_impl, sync_request_builder, storage_unit) = rt.block_on(setup_dataset(
        tmp_workspace_dir.path(),
        &dataset_alias,
        None,
    ));

    let mut group = c.benchmark_group("parallel_10_simple_transfer_protocol");
    // Min size of iterations is 10
    group.sample_size(10);
    group.bench_function("transfer", |b| {
        b.iter(|| {
            rt.block_on(do_test_sync(
                sync_service_impl.clone(),
                sync_request_builder.clone(),
                &dataset_alias,
                &odf::DatasetRefRemote::from(&pull_repo_url),
                &odf::DatasetRefRemote::from(&push_repo_url),
                storage_unit.clone(),
            ));
        });
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

criterion_group!(benches, bench_with_1_parallel, bench_with_10_parallels);
criterion_main!(benches);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
