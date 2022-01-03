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

use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use std::assert_matches::assert_matches;
use std::fs::File;
use std::sync::Arc;

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_transform_with_engine_spark() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    let metadata_repo = Arc::new(MetadataRepositoryImpl::new(workspace_layout.clone()));
    let engine_provisioner = Arc::new(EngineProvisionerLocal::new(
        EngineProvisionerLocalConfig::default(),
        workspace_layout.clone(),
        ContainerRuntime::default(),
    ));

    let ingest_svc = IngestServiceImpl::new(
        &volume_layout,
        metadata_repo.clone(),
        engine_provisioner.clone(),
    );

    let transform_svc = TransformServiceImpl::new(
        metadata_repo.clone(),
        engine_provisioner.clone(),
        &volume_layout,
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

    metadata_repo.add_dataset(root_snapshot).unwrap();

    ingest_svc
        .ingest(&root_name.as_local_ref(), IngestOptions::default(), None)
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

    metadata_repo.add_dataset(deriv_snapshot).unwrap();

    let block_hash = match transform_svc
        .transform(&deriv_name.as_local_ref(), None)
        .unwrap()
    {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let dataset_layout = DatasetLayout::new(&volume_layout, &deriv_name);
    assert!(dataset_layout.data_dir.exists());
    assert_eq!(
        metadata_repo
            .get_metadata_chain(&deriv_name.as_local_ref())
            .unwrap()
            .iter_blocks()
            .count(),
        3
    );

    let part_file = dataset_layout.data_dir.join(block_hash.to_string());
    let parquet_reader = SerializedFileReader::new(File::open(&part_file).unwrap()).unwrap();
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
        .unwrap();

    let block_hash = match transform_svc
        .transform(&deriv_name.as_local_ref(), None)
        .unwrap()
    {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let part_file = dataset_layout.data_dir.join(block_hash.to_string());

    let parquet_reader = SerializedFileReader::new(File::open(&part_file).unwrap()).unwrap();
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

    let verify_result = transform_svc.verify_transform(
        &deriv_name.as_local_ref(),
        (None, None),
        VerificationOptions::default(),
        None,
    );

    assert_matches!(verify_result, Ok(VerificationResult::Valid));
}

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_transform_with_engine_flink() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    let metadata_repo = Arc::new(MetadataRepositoryImpl::new(workspace_layout.clone()));
    let engine_provisioner = Arc::new(EngineProvisionerLocal::new(
        EngineProvisionerLocalConfig::default(),
        workspace_layout.clone(),
        ContainerRuntime::default(),
    ));

    let ingest_svc = IngestServiceImpl::new(
        &volume_layout,
        metadata_repo.clone(),
        engine_provisioner.clone(),
    );

    let transform_svc = TransformServiceImpl::new(
        metadata_repo.clone(),
        engine_provisioner.clone(),
        &volume_layout,
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

    metadata_repo.add_dataset(root_snapshot).unwrap();

    ingest_svc
        .ingest(&root_name.as_local_ref(), IngestOptions::default(), None)
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

    metadata_repo.add_dataset(deriv_snapshot).unwrap();

    let block_hash = match transform_svc
        .transform(&deriv_name.as_local_ref(), None)
        .unwrap()
    {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let dataset_layout = DatasetLayout::new(&volume_layout, &deriv_name);
    assert!(dataset_layout.data_dir.exists());
    assert_eq!(
        metadata_repo
            .get_metadata_chain(&deriv_name.as_local_ref())
            .unwrap()
            .iter_blocks()
            .count(),
        3
    );

    let part_file = dataset_layout.data_dir.join(block_hash.to_string());
    let parquet_reader = SerializedFileReader::new(File::open(&part_file).unwrap()).unwrap();
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
        .unwrap();

    let block_hash = match transform_svc
        .transform(&deriv_name.as_local_ref(), None)
        .unwrap()
    {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let part_file = dataset_layout.data_dir.join(block_hash.to_string());

    let parquet_reader = SerializedFileReader::new(File::open(&part_file).unwrap()).unwrap();
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

    let verify_result = transform_svc.verify_transform(
        &deriv_name.as_local_ref(),
        (None, None),
        VerificationOptions::default(),
        None,
    );

    assert_matches!(verify_result, Ok(VerificationResult::Valid));
}
