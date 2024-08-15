// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::path::Path;
use std::sync::Arc;

use container_runtime::ContainerRuntime;
use dill::Component;
use event_bus::EventBus;
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets_services::DatasetKeyValueServiceSysEnv;
use opendatafabric::*;

async fn test_engine_io_common(
    object_stores: Vec<Arc<dyn ObjectStoreBuilder>>,
    dataset_repo: Arc<dyn DatasetRepository>,
    run_info_dir: &Path,
    cache_dir: &Path,
    transform: Transform,
) {
    let run_info_dir = Arc::new(RunInfoDir::new(run_info_dir.to_path_buf()));
    let cache_dir = Arc::new(CacheDir::new(cache_dir.to_path_buf()));

    let engine_provisioner = Arc::new(EngineProvisionerLocal::new(
        EngineProvisionerLocalConfig::default(),
        Arc::new(ContainerRuntime::default()),
        dataset_repo.clone(),
        run_info_dir.clone(),
    ));

    let dataset_action_authorizer = Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new());
    let object_store_registry = Arc::new(ObjectStoreRegistryImpl::new(object_stores));
    let time_source = Arc::new(SystemTimeSourceDefault);
    let dataset_env_var_sys_env = Arc::new(DatasetKeyValueServiceSysEnv::new());

    let ingest_svc = PollingIngestServiceImpl::new(
        dataset_repo.clone(),
        dataset_action_authorizer.clone(),
        Arc::new(FetchService::new(
            Arc::new(ContainerRuntime::default()),
            None,
            None,
            None,
            dataset_env_var_sys_env,
            run_info_dir.clone(),
        )),
        engine_provisioner.clone(),
        object_store_registry.clone(),
        Arc::new(DataFormatRegistryImpl::new()),
        run_info_dir.clone(),
        cache_dir,
        time_source.clone(),
    );

    let transform_svc = TransformServiceImpl::new(
        dataset_repo.clone(),
        dataset_action_authorizer.clone(),
        engine_provisioner.clone(),
        Arc::new(SystemTimeSourceDefault),
        Arc::new(CompactionServiceImpl::new(
            dataset_action_authorizer.clone(),
            dataset_repo.clone(),
            object_store_registry.clone(),
            time_source.clone(),
            run_info_dir.clone(),
        )),
    );

    ///////////////////////////////////////////////////////////////////////////
    // Root setup
    ///////////////////////////////////////////////////////////////////////////

    let src_path = run_info_dir.join("data.csv");
    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            A,1000
            B,2000
            C,3000
            "
        ),
    )
    .unwrap();

    let root_snapshot = MetadataFactory::dataset_snapshot()
        .name("root")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch_file(&src_path)
                .read(ReadStepCsv {
                    header: Some(true),
                    schema: Some(vec![
                        "city STRING".to_string(),
                        "population INT".to_string(),
                    ]),
                    ..ReadStepCsv::default()
                })
                .merge(MergeStrategySnapshot {
                    primary_key: vec!["city".to_string()],
                    compare_columns: None,
                })
                .build(),
        )
        .build();

    let root_alias = root_snapshot.name.clone();

    dataset_repo
        .create_dataset_from_snapshot(root_snapshot)
        .await
        .unwrap();

    ingest_svc
        .ingest(
            &root_alias.as_local_ref(),
            PollingIngestOptions::default(),
            None,
        )
        .await
        .unwrap();

    ///////////////////////////////////////////////////////////////////////////
    // Derivative setup
    ///////////////////////////////////////////////////////////////////////////

    let deriv_snapshot = MetadataFactory::dataset_snapshot()
        .name("deriv")
        .kind(DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs([&root_alias.dataset_name])
                .transform(transform)
                .build(),
        )
        .build();

    let deriv_alias = deriv_snapshot.name.clone();

    let dataset_deriv = dataset_repo
        .create_dataset_from_snapshot(deriv_snapshot)
        .await
        .unwrap()
        .dataset;

    let block_hash = match transform_svc
        .transform(
            &deriv_alias.as_local_ref(),
            TransformOptions::default(),
            None,
        )
        .await
        .unwrap()
    {
        TransformResult::Updated { new_head, .. } => new_head,
        v => panic!("Unexpected result: {v:?}"),
    };

    let block = dataset_deriv
        .as_metadata_chain()
        .get_block(&block_hash)
        .await
        .unwrap()
        .into_data_stream_block()
        .unwrap();

    assert_eq!(
        block.event.new_data.unwrap().offset_interval,
        OffsetInterval { start: 0, end: 2 }
    );

    ///////////////////////////////////////////////////////////////////////////
    // Round 2
    ///////////////////////////////////////////////////////////////////////////

    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            A,1000
            B,2000
            C,3000
            D,4000
            E,5000
            "
        ),
    )
    .unwrap();

    ingest_svc
        .ingest(
            &root_alias.as_local_ref(),
            PollingIngestOptions::default(),
            None,
        )
        .await
        .unwrap();

    let block_hash = match transform_svc
        .transform(
            &deriv_alias.as_local_ref(),
            TransformOptions::default(),
            None,
        )
        .await
        .unwrap()
    {
        TransformResult::Updated { new_head, .. } => new_head,
        v => panic!("Unexpected result: {v:?}"),
    };

    let block = dataset_deriv
        .as_metadata_chain()
        .get_block(&block_hash)
        .await
        .unwrap()
        .into_data_stream_block()
        .unwrap();

    assert_eq!(
        block.event.new_data.unwrap().offset_interval,
        OffsetInterval { start: 3, end: 4 }
    );

    ///////////////////////////////////////////////////////////////////////////
    // Verify
    ///////////////////////////////////////////////////////////////////////////

    let verify_result = transform_svc
        .verify_transform(&deriv_alias.as_local_ref(), (None, None), None)
        .await;

    assert_matches!(verify_result, Ok(()));
}

#[test_group::group(containerized, engine, transform, datafusion)]
#[test_log::test(tokio::test)]
async fn test_engine_io_local_file_mount() {
    let tempdir = tempfile::tempdir().unwrap();
    let run_info_dir = tempdir.path().join("run");
    let cache_dir = tempdir.path().join("cache");
    let datasets_dir = tempdir.path().join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();
    std::fs::create_dir(&run_info_dir).unwrap();
    std::fs::create_dir(&cache_dir).unwrap();

    let catalog = dill::CatalogBuilder::new()
        .add::<SystemTimeSourceDefault>()
        .add::<EventBus>()
        .add::<kamu_core::auth::AlwaysHappyDatasetActionAuthorizer>()
        .add::<kamu::DependencyGraphServiceInMemory>()
        .add::<DatasetKeyValueServiceSysEnv>()
        .add_value(CurrentAccountSubject::new_test())
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(datasets_dir)
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .build();

    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

    test_engine_io_common(
        vec![Arc::new(ObjectStoreBuilderLocalFs::new())],
        dataset_repo,
        &run_info_dir,
        &cache_dir,
        MetadataFactory::transform()
            .engine("datafusion")
            .query(
                "SELECT event_time, city, cast(population * 10 as int) as population_x10 FROM root",
            )
            .build(),
    )
    .await;
}

#[test_group::group(containerized, engine, transform, datafusion)]
#[test_log::test(tokio::test)]
async fn test_engine_io_s3_to_local_file_mount_proxy() {
    let s3 = LocalS3Server::new().await;
    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let run_info_dir = tmp_workspace_dir.path().join("run");
    let cache_dir = tmp_workspace_dir.path().join("cache");
    std::fs::create_dir(&run_info_dir).unwrap();
    std::fs::create_dir(&cache_dir).unwrap();

    let s3_context = kamu::utils::s3_context::S3Context::from_url(&s3.url).await;

    let catalog = dill::CatalogBuilder::new()
        .add::<SystemTimeSourceDefault>()
        .add::<EventBus>()
        .add::<kamu_core::auth::AlwaysHappyDatasetActionAuthorizer>()
        .add::<kamu::DependencyGraphServiceInMemory>()
        .add_value(CurrentAccountSubject::new_test())
        .add_builder(
            DatasetRepositoryS3::builder()
                .with_s3_context(s3_context.clone())
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryS3>()
        .build();

    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

    test_engine_io_common(
        vec![
            Arc::new(ObjectStoreBuilderLocalFs::new()),
            // Note that DataFusion ingest will use S3 object store directly, but transform engine
            // will use the IO proxying
            Arc::new(ObjectStoreBuilderS3::new(s3_context, true)),
        ],
        dataset_repo,
        &run_info_dir,
        &cache_dir,
        MetadataFactory::transform()
            .engine("datafusion")
            .query(
                "SELECT event_time, city, cast(population * 10 as int) as population_x10 FROM root",
            )
            .build(),
    )
    .await;
}
