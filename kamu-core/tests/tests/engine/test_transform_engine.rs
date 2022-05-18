// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use container_runtime::ContainerRuntime;
use indoc::indoc;
use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;

use datafusion::parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use futures::TryStreamExt;
use std::assert_matches::assert_matches;
use std::fs::File;
use std::sync::Arc;

async fn block_count(
    local_repo: &dyn LocalDatasetRepository,
    dataset_ref: impl Into<DatasetRefLocal>,
) -> usize {
    let ds = local_repo.get_dataset(&dataset_ref.into()).await.unwrap();
    let blocks: Vec<_> = ds
        .as_metadata_chain()
        .iter_blocks()
        .try_collect()
        .await
        .unwrap();
    blocks.len()
}

async fn get_data_of_block(
    local_repo: &dyn LocalDatasetRepository,
    dataset_ref: impl Into<DatasetRefLocal>,
    dataset_layout: &DatasetLayout,
    block_hash: &Multihash,
) -> SerializedFileReader<File> {
    let dataset = local_repo.get_dataset(&dataset_ref.into()).await.unwrap();
    let block = dataset
        .as_metadata_chain()
        .get_block(block_hash)
        .await
        .unwrap();
    let part_file = dataset_layout.data_slice_path(
        block
            .as_data_stream_block()
            .unwrap()
            .event
            .output_data
            .unwrap(),
    );
    SerializedFileReader::new(File::open(&part_file).unwrap()).unwrap()
}

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_transform_with_engine_spark() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());

    let local_repo = Arc::new(LocalDatasetRepositoryImpl::new(workspace_layout.clone()));
    let engine_provisioner = Arc::new(EngineProvisionerLocal::new(
        EngineProvisionerLocalConfig::default(),
        workspace_layout.clone(),
        ContainerRuntime::default(),
    ));

    let ingest_svc = IngestServiceImpl::new(
        workspace_layout.clone(),
        local_repo.clone(),
        engine_provisioner.clone(),
    );

    let transform_svc = TransformServiceImpl::new(
        local_repo.clone(),
        engine_provisioner.clone(),
        workspace_layout.clone(),
    );

    ///////////////////////////////////////////////////////////////////////////
    // Root setup
    ///////////////////////////////////////////////////////////////////////////

    let src_path = tempdir.path().join("data.csv");
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
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["city STRING", "population INT"]
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    ),
                    ..ReadStepCsv::default()
                }))
                .build(),
        )
        .build();

    let root_name = root_snapshot.name.clone();

    local_repo
        .create_dataset_from_snapshot(root_snapshot)
        .await
        .unwrap();

    ingest_svc
        .ingest(&root_name.as_local_ref(), IngestOptions::default(), None)
        .await
        .unwrap();

    ///////////////////////////////////////////////////////////////////////////
    // Derivative setup
    ///////////////////////////////////////////////////////////////////////////

    let deriv_snapshot = MetadataFactory::dataset_snapshot()
        .name("deriv")
        .kind(DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform([&root_name])
                .transform(
                    MetadataFactory::transform()
                        .engine("spark")
                        .query(
                            "SELECT event_time, city, population * 10 as population_x10 FROM root",
                        )
                        .build(),
                )
                .build(),
        )
        .build();

    let deriv_name = deriv_snapshot.name.clone();

    local_repo
        .create_dataset_from_snapshot(deriv_snapshot)
        .await
        .unwrap();

    let block_hash = match transform_svc
        .transform(&deriv_name.as_local_ref(), None)
        .await
        .unwrap()
    {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let dataset_layout = workspace_layout.dataset_layout(&deriv_name);
    assert!(dataset_layout.data_dir.exists());
    assert_eq!(block_count(local_repo.as_ref(), &deriv_name).await, 3);

    let parquet_reader = get_data_of_block(
        local_repo.as_ref(),
        &deriv_name,
        &dataset_layout,
        &block_hash,
    )
    .await;
    let columns: Vec<_> = parquet_reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|cd| cd.path().string())
        .collect();

    assert_eq!(
        columns,
        [
            "offset",
            "system_time",
            "event_time",
            "city",
            "population_x10"
        ]
    );

    let records: Vec<_> = parquet_reader
        .get_row_iter(None)
        .unwrap()
        .map(|r| {
            (
                r.get_long(0).unwrap().clone(),
                r.get_string(3).unwrap().clone(),
                r.get_int(4).unwrap(),
            )
        })
        .collect();

    assert_eq!(
        records,
        [
            (0, "A".to_owned(), 10000),
            (1, "B".to_owned(), 20000),
            (2, "C".to_owned(), 30000)
        ]
    );

    ///////////////////////////////////////////////////////////////////////////
    // Round 2
    ///////////////////////////////////////////////////////////////////////////

    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            D,4000
            E,5000
            "
        ),
    )
    .unwrap();

    ingest_svc
        .ingest(&root_name.as_local_ref(), IngestOptions::default(), None)
        .await
        .unwrap();

    let block_hash = match transform_svc
        .transform(&deriv_name.as_local_ref(), None)
        .await
        .unwrap()
    {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let parquet_reader = get_data_of_block(
        local_repo.as_ref(),
        &deriv_name,
        &dataset_layout,
        &block_hash,
    )
    .await;
    let records: Vec<_> = parquet_reader
        .get_row_iter(None)
        .unwrap()
        .map(|r| {
            (
                r.get_long(0).unwrap().clone(),
                r.get_string(3).unwrap().clone(),
                r.get_int(4).unwrap(),
            )
        })
        .collect();

    assert_eq!(
        records,
        [(3, "D".to_owned(), 40000), (4, "E".to_owned(), 50000),]
    );

    ///////////////////////////////////////////////////////////////////////////
    // Verify
    ///////////////////////////////////////////////////////////////////////////

    let verify_result = transform_svc
        .verify_transform(
            &deriv_name.as_local_ref(),
            (None, None),
            VerificationOptions::default(),
            None,
        )
        .await;

    assert_matches!(verify_result, Ok(VerificationResult::Valid));
}

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_transform_with_engine_flink() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());

    let local_repo = Arc::new(LocalDatasetRepositoryImpl::new(workspace_layout.clone()));
    let engine_provisioner = Arc::new(EngineProvisionerLocal::new(
        EngineProvisionerLocalConfig::default(),
        workspace_layout.clone(),
        ContainerRuntime::default(),
    ));

    let ingest_svc = IngestServiceImpl::new(
        workspace_layout.clone(),
        local_repo.clone(),
        engine_provisioner.clone(),
    );

    let transform_svc = TransformServiceImpl::new(
        local_repo.clone(),
        engine_provisioner.clone(),
        workspace_layout.clone(),
    );

    ///////////////////////////////////////////////////////////////////////////
    // Root setup
    ///////////////////////////////////////////////////////////////////////////

    let src_path = tempdir.path().join("data.csv");
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
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["city STRING", "population INT"]
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    ),
                    ..ReadStepCsv::default()
                }))
                .build(),
        )
        .build();

    let root_name = root_snapshot.name.clone();

    local_repo
        .create_dataset_from_snapshot(root_snapshot)
        .await
        .unwrap();

    ingest_svc
        .ingest(&root_name.as_local_ref(), IngestOptions::default(), None)
        .await
        .unwrap();

    ///////////////////////////////////////////////////////////////////////////
    // Derivative setup
    ///////////////////////////////////////////////////////////////////////////

    let deriv_snapshot = MetadataFactory::dataset_snapshot()
        .name("deriv")
        .kind(DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform([&root_name])
                .transform(
                    MetadataFactory::transform()
                        .engine("flink")
                        .query(
                            "SELECT event_time, city, population * 10 as population_x10 FROM root",
                        )
                        .build(),
                )
                .build(),
        )
        .build();

    let deriv_name = deriv_snapshot.name.clone();

    local_repo
        .create_dataset_from_snapshot(deriv_snapshot)
        .await
        .unwrap();

    let block_hash = match transform_svc
        .transform(&deriv_name.as_local_ref(), None)
        .await
        .unwrap()
    {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let dataset_layout = workspace_layout.dataset_layout(&deriv_name);
    assert!(dataset_layout.data_dir.exists());
    assert_eq!(block_count(local_repo.as_ref(), &deriv_name).await, 3);

    let parquet_reader = get_data_of_block(
        local_repo.as_ref(),
        &deriv_name,
        &dataset_layout,
        &block_hash,
    )
    .await;
    let columns: Vec<_> = parquet_reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|cd| cd.path().string())
        .collect();

    assert_eq!(
        columns,
        [
            "offset",
            "system_time",
            "event_time",
            "city",
            "population_x10"
        ]
    );

    let records: Vec<_> = parquet_reader
        .get_row_iter(None)
        .unwrap()
        .map(|r| {
            (
                r.get_long(0).unwrap().clone(),
                r.get_string(3).unwrap().clone(),
                r.get_int(4).unwrap(),
            )
        })
        .collect();

    assert_eq!(
        records,
        [
            (0, "A".to_owned(), 10000),
            (1, "B".to_owned(), 20000),
            (2, "C".to_owned(), 30000)
        ]
    );

    ///////////////////////////////////////////////////////////////////////////
    // Round 2
    ///////////////////////////////////////////////////////////////////////////

    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            D,4000
            E,5000
            "
        ),
    )
    .unwrap();

    ingest_svc
        .ingest(&root_name.as_local_ref(), IngestOptions::default(), None)
        .await
        .unwrap();

    let block_hash = match transform_svc
        .transform(&deriv_name.as_local_ref(), None)
        .await
        .unwrap()
    {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let parquet_reader = get_data_of_block(
        local_repo.as_ref(),
        &deriv_name,
        &dataset_layout,
        &block_hash,
    )
    .await;
    let records: Vec<_> = parquet_reader
        .get_row_iter(None)
        .unwrap()
        .map(|r| {
            (
                r.get_long(0).unwrap().clone(),
                r.get_string(3).unwrap().clone(),
                r.get_int(4).unwrap(),
            )
        })
        .collect();

    assert_eq!(
        records,
        [(3, "D".to_owned(), 40000), (4, "E".to_owned(), 50000),]
    );

    ///////////////////////////////////////////////////////////////////////////
    // Verify
    ///////////////////////////////////////////////////////////////////////////

    let verify_result = transform_svc
        .verify_transform(
            &deriv_name.as_local_ref(),
            (None, None),
            VerificationOptions::default(),
            None,
        )
        .await;

    assert_matches!(verify_result, Ok(VerificationResult::Valid));
}
