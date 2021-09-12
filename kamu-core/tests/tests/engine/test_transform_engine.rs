// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indoc::indoc;
use kamu::domain::*;
use kamu::infra::utils::docker_client::DockerClient;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;

use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use std::fs::File;
use std::sync::Arc;

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_transform_with_engine_spark() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    let metadata_repo = Arc::new(MetadataRepositoryImpl::new(workspace_layout.clone()));
    let engine_factory = Arc::new(EngineFactoryImpl::new(
        &workspace_layout,
        DockerClient::default(),
        slog::Logger::root(slog::Discard, slog::o!()),
    ));

    let ingest_svc = IngestServiceImpl::new(
        &volume_layout,
        metadata_repo.clone(),
        engine_factory.clone(),
        slog::Logger::root(slog::Discard, slog::o!()),
    );

    let transform_svc = TransformServiceImpl::new(
        metadata_repo.clone(),
        engine_factory.clone(),
        &volume_layout,
        slog::Logger::root(slog::Discard, slog::o!()),
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
        .id("root")
        .source(
            MetadataFactory::dataset_source_root()
                .fetch_file(&src_path)
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["city STRING", "population INT"]
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    ),
                    separator: None,
                    encoding: None,
                    quote: None,
                    escape: None,
                    comment: None,
                    enforce_schema: None,
                    infer_schema: None,
                    ignore_leading_white_space: None,
                    ignore_trailing_white_space: None,
                    null_value: None,
                    empty_value: None,
                    nan_value: None,
                    positive_inf: None,
                    negative_inf: None,
                    date_format: None,
                    timestamp_format: None,
                    multi_line: None,
                }))
                .build(),
        )
        .build();

    let root_id = root_snapshot.id.clone();

    metadata_repo.add_dataset(root_snapshot).unwrap();

    ingest_svc
        .ingest(&root_id, IngestOptions::default(), None)
        .unwrap();

    ///////////////////////////////////////////////////////////////////////////
    // Derivative setup
    ///////////////////////////////////////////////////////////////////////////

    let deriv_snapshot = MetadataFactory::dataset_snapshot()
        .id("deriv")
        .source(
            MetadataFactory::dataset_source_deriv([&root_id].iter())
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

    let deriv_id = deriv_snapshot.id.clone();

    metadata_repo.add_dataset(deriv_snapshot).unwrap();

    let block_hash = match transform_svc.transform(&deriv_id, None).unwrap() {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let dataset_layout = DatasetLayout::new(&volume_layout, &deriv_id);
    assert!(dataset_layout.data_dir.exists());
    assert_eq!(
        metadata_repo
            .get_metadata_chain(&deriv_id)
            .unwrap()
            .iter_blocks()
            .count(),
        2
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
        ["system_time", "event_time", "city", "population_x10"]
    );

    let records: Vec<_> = parquet_reader
        .get_row_iter(None)
        .unwrap()
        .map(|r| (r.get_string(2).unwrap().clone(), r.get_int(3).unwrap()))
        .collect();

    assert_eq!(
        records,
        [
            ("A".to_owned(), 10000),
            ("B".to_owned(), 20000),
            ("C".to_owned(), 30000)
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
        .ingest(&root_id, IngestOptions::default(), None)
        .unwrap();

    let block_hash = match transform_svc.transform(&deriv_id, None).unwrap() {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let part_file = dataset_layout.data_dir.join(block_hash.to_string());

    let parquet_reader = SerializedFileReader::new(File::open(&part_file).unwrap()).unwrap();
    let records: Vec<_> = parquet_reader
        .get_row_iter(None)
        .unwrap()
        .map(|r| (r.get_string(2).unwrap().clone(), r.get_int(3).unwrap()))
        .collect();

    assert_eq!(records, [("D".to_owned(), 40000), ("E".to_owned(), 50000),]);
}

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_transform_with_engine_flink() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    let metadata_repo = Arc::new(MetadataRepositoryImpl::new(workspace_layout.clone()));
    let engine_factory = Arc::new(EngineFactoryImpl::new(
        &workspace_layout,
        DockerClient::default(),
        slog::Logger::root(slog::Discard, slog::o!()),
    ));

    let ingest_svc = IngestServiceImpl::new(
        &volume_layout,
        metadata_repo.clone(),
        engine_factory.clone(),
        slog::Logger::root(slog::Discard, slog::o!()),
    );

    let transform_svc = TransformServiceImpl::new(
        metadata_repo.clone(),
        engine_factory.clone(),
        &volume_layout,
        slog::Logger::root(slog::Discard, slog::o!()),
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
        .id("root")
        .source(
            MetadataFactory::dataset_source_root()
                .fetch_file(&src_path)
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["city STRING", "population INT"]
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    ),
                    separator: None,
                    encoding: None,
                    quote: None,
                    escape: None,
                    comment: None,
                    enforce_schema: None,
                    infer_schema: None,
                    ignore_leading_white_space: None,
                    ignore_trailing_white_space: None,
                    null_value: None,
                    empty_value: None,
                    nan_value: None,
                    positive_inf: None,
                    negative_inf: None,
                    date_format: None,
                    timestamp_format: None,
                    multi_line: None,
                }))
                .build(),
        )
        .build();

    let root_id = root_snapshot.id.clone();

    metadata_repo.add_dataset(root_snapshot).unwrap();

    ingest_svc
        .ingest(&root_id, IngestOptions::default(), None)
        .unwrap();

    ///////////////////////////////////////////////////////////////////////////
    // Derivative setup
    ///////////////////////////////////////////////////////////////////////////

    let deriv_snapshot = MetadataFactory::dataset_snapshot()
        .id("deriv")
        .source(
            MetadataFactory::dataset_source_deriv([&root_id].iter())
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

    let deriv_id = deriv_snapshot.id.clone();

    metadata_repo.add_dataset(deriv_snapshot).unwrap();

    let block_hash = match transform_svc.transform(&deriv_id, None).unwrap() {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let dataset_layout = DatasetLayout::new(&volume_layout, &deriv_id);
    assert!(dataset_layout.data_dir.exists());
    assert_eq!(
        metadata_repo
            .get_metadata_chain(&deriv_id)
            .unwrap()
            .iter_blocks()
            .count(),
        2
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
        ["system_time", "event_time", "city", "population_x10"]
    );

    let records: Vec<_> = parquet_reader
        .get_row_iter(None)
        .unwrap()
        .map(|r| (r.get_string(2).unwrap().clone(), r.get_int(3).unwrap()))
        .collect();

    assert_eq!(
        records,
        [
            ("A".to_owned(), 10000),
            ("B".to_owned(), 20000),
            ("C".to_owned(), 30000)
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
        .ingest(&root_id, IngestOptions::default(), None)
        .unwrap();

    let block_hash = match transform_svc.transform(&deriv_id, None).unwrap() {
        TransformResult::Updated { new_head, .. } => new_head,
        v @ _ => panic!("Unexpected result: {:?}", v),
    };

    let part_file = dataset_layout.data_dir.join(block_hash.to_string());

    let parquet_reader = SerializedFileReader::new(File::open(&part_file).unwrap()).unwrap();
    let records: Vec<_> = parquet_reader
        .get_row_iter(None)
        .unwrap()
        .map(|r| (r.get_string(2).unwrap().clone(), r.get_int(3).unwrap()))
        .collect();

    assert_eq!(records, [("D".to_owned(), 40000), ("E".to_owned(), 50000),]);
}
