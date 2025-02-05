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
use indoc::indoc;
use kamu::domain::*;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets_services::DatasetKeyValueServiceSysEnv;
use odf::dataset::testing::create_test_dataset_fron_snapshot;
use odf::metadata::testing::MetadataFactory;
use test_utils::LocalS3Server;
use time_source::{SystemTimeSource, SystemTimeSourceDefault};

use crate::TransformTestHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_engine_io_common<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter + 'static,
>(
    object_stores: Vec<Arc<dyn ObjectStoreBuilder>>,
    storage_unit: Arc<TDatasetStorageUnit>,
    did_generator: Arc<dyn DidGenerator>,
    run_info_dir: &Path,
    cache_dir: &Path,
    transform: odf::metadata::Transform,
) {
    let run_info_dir = Arc::new(RunInfoDir::new(run_info_dir.to_path_buf()));
    let cache_dir = Arc::new(CacheDir::new(cache_dir.to_path_buf()));

    let dataset_registry = DatasetRegistrySoloUnitBridge::new(storage_unit.clone());

    let engine_provisioner = Arc::new(EngineProvisionerLocal::new(
        EngineProvisionerLocalConfig::default(),
        Arc::new(ContainerRuntime::default()),
        run_info_dir.clone(),
    ));

    let object_store_registry = Arc::new(ObjectStoreRegistryImpl::new(object_stores));
    let time_source = Arc::new(SystemTimeSourceDefault);
    let dataset_env_var_sys_env = Arc::new(DatasetKeyValueServiceSysEnv::new());

    let ingest_svc = PollingIngestServiceImpl::new(
        Arc::new(FetchService::new(
            Arc::new(ContainerRuntime::default()),
            None,
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

    let transform_helper = TransformTestHelper::build(
        Arc::new(DatasetRegistrySoloUnitBridge::new(storage_unit.clone())),
        time_source.clone(),
        Arc::new(CompactionPlannerImpl {}),
        Arc::new(CompactionExecutorImpl::new(
            object_store_registry.clone(),
            time_source.clone(),
            run_info_dir.clone(),
        )),
        engine_provisioner.clone(),
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
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch_file(&src_path)
                .read(odf::metadata::ReadStepCsv {
                    header: Some(true),
                    schema: Some(vec![
                        "city STRING".to_string(),
                        "population INT".to_string(),
                    ]),
                    ..odf::metadata::ReadStepCsv::default()
                })
                .merge(odf::metadata::MergeStrategySnapshot {
                    primary_key: vec!["city".to_string()],
                    compare_columns: None,
                })
                .build(),
        )
        .build();

    let root_alias = root_snapshot.name.clone();

    let root_created = create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        root_snapshot,
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap();

    let root_target = ResolvedDataset::from(&root_created);

    let root_metadata_state =
        DataWriterMetadataState::build(root_target.clone(), &odf::BlockRef::Head, None)
            .await
            .unwrap();

    ingest_svc
        .ingest(
            root_target.clone(),
            Box::new(root_metadata_state),
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
        .kind(odf::DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs([&root_alias.dataset_name])
                .transform(transform)
                .build(),
        )
        .build();

    let deriv_created = create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        deriv_snapshot,
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap();

    let block_hash = match transform_helper.transform_dataset(&deriv_created).await {
        TransformResult::Updated { new_head, .. } => new_head,
        v => panic!("Unexpected result: {v:?}"),
    };

    use odf::metadata::IntoDataStreamBlock;
    let block = deriv_created
        .dataset
        .as_metadata_chain()
        .get_block(&block_hash)
        .await
        .unwrap()
        .into_data_stream_block()
        .unwrap();

    assert_eq!(
        block.event.new_data.unwrap().offset_interval,
        odf::metadata::OffsetInterval { start: 0, end: 2 }
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

    let root_metadata_state =
        DataWriterMetadataState::build(root_target.clone(), &odf::BlockRef::Head, None)
            .await
            .unwrap();

    ingest_svc
        .ingest(
            root_target,
            Box::new(root_metadata_state),
            PollingIngestOptions::default(),
            None,
        )
        .await
        .unwrap();

    let block_hash = match transform_helper.transform_dataset(&deriv_created).await {
        TransformResult::Updated { new_head, .. } => new_head,
        v => panic!("Unexpected result: {v:?}"),
    };

    let block = deriv_created
        .dataset
        .as_metadata_chain()
        .get_block(&block_hash)
        .await
        .unwrap()
        .into_data_stream_block()
        .unwrap();

    assert_eq!(
        block.event.new_data.unwrap().offset_interval,
        odf::metadata::OffsetInterval { start: 3, end: 4 }
    );

    ///////////////////////////////////////////////////////////////////////////
    // Verify
    ///////////////////////////////////////////////////////////////////////////

    let verify_result = transform_helper.verify_transform(&deriv_created).await;
    assert_matches!(verify_result, Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        .add::<DidGeneratorDefault>()
        .add::<SystemTimeSourceDefault>()
        .add::<kamu_core::auth::AlwaysHappyDatasetActionAuthorizer>()
        .add::<DatasetKeyValueServiceSysEnv>()
        .add_value(CurrentAccountSubject::new_test())
        .add_value(TenancyConfig::SingleTenant)
        .add_builder(DatasetStorageUnitLocalFs::builder().with_root(datasets_dir))
        .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitLocalFs>()
        .build();

    let storage_unit = catalog.get_one::<DatasetStorageUnitLocalFs>().unwrap();
    let did_generator = catalog.get_one::<dyn DidGenerator>().unwrap();

    test_engine_io_common(
        vec![Arc::new(ObjectStoreBuilderLocalFs::new())],
        storage_unit,
        did_generator,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, transform, datafusion)]
#[test_log::test(tokio::test)]
async fn test_engine_io_s3_to_local_file_mount_proxy() {
    let s3 = LocalS3Server::new().await;
    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let run_info_dir = tmp_workspace_dir.path().join("run");
    let cache_dir = tmp_workspace_dir.path().join("cache");
    std::fs::create_dir(&run_info_dir).unwrap();
    std::fs::create_dir(&cache_dir).unwrap();

    let s3_context = s3_utils::S3Context::from_url(&s3.url).await;

    let catalog = dill::CatalogBuilder::new()
        .add::<DidGeneratorDefault>()
        .add::<SystemTimeSourceDefault>()
        .add::<kamu_core::auth::AlwaysHappyDatasetActionAuthorizer>()
        .add_value(CurrentAccountSubject::new_test())
        .add_value(TenancyConfig::SingleTenant)
        .add_builder(DatasetStorageUnitS3::builder().with_s3_context(s3_context.clone()))
        .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitS3>()
        .build();

    let storage_unit = catalog.get_one::<DatasetStorageUnitS3>().unwrap();
    let did_generator = catalog.get_one::<dyn DidGenerator>().unwrap();

    test_engine_io_common(
        vec![
            Arc::new(ObjectStoreBuilderLocalFs::new()),
            // Note that DataFusion ingest will use S3 object store directly, but transform engine
            // will use the IO proxying
            Arc::new(ObjectStoreBuilderS3::new(s3_context, true)),
        ],
        storage_unit,
        did_generator,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
