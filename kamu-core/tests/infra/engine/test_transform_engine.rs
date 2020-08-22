use indoc::indoc;
use kamu::domain::*;
use kamu::infra::serde::yaml::*;
use kamu::infra::*;
use kamu_test::*;

use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use std::cell::RefCell;
use std::fs::File;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_transform_with_engine_spark() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    let metadata_repo = Rc::new(RefCell::new(MetadataRepositoryImpl::new(&workspace_layout)));
    let engine_factory = Arc::new(Mutex::new(EngineFactory::new(&workspace_layout)));

    let mut ingest_svc = IngestServiceImpl::new(
        metadata_repo.clone(),
        engine_factory.clone(),
        &volume_layout,
        slog::Logger::root(slog::Discard, slog::o!()),
    );

    let mut transform_svc = TransformServiceImpl::new(
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

    metadata_repo
        .borrow_mut()
        .add_dataset(root_snapshot)
        .unwrap();

    ingest_svc.ingest(&root_id, None).unwrap();

    ///////////////////////////////////////////////////////////////////////////
    // Derivative setup
    ///////////////////////////////////////////////////////////////////////////

    let deriv_snapshot = MetadataFactory::dataset_snapshot()
        .id("deriv")
        .source(
            MetadataFactory::dataset_source_deriv([&root_id].iter())
                .transform(
                    MetadataFactory::transform()
                        .engine("sparkSQL")
                        .query(
                            "SELECT event_time, city, population * 10 as population_x10 FROM root",
                        )
                        .build(),
                )
                .build(),
        )
        .build();

    let deriv_id = deriv_snapshot.id.clone();

    metadata_repo
        .borrow_mut()
        .add_dataset(deriv_snapshot)
        .unwrap();

    let res = transform_svc.transform(&deriv_id, None).unwrap();
    assert!(matches!(res, TransformResult::Updated {.. }));

    let dataset_layout = DatasetLayout::new(&volume_layout, &deriv_id);
    assert!(dataset_layout.data_dir.exists());
    assert_eq!(
        metadata_repo
            .borrow_mut()
            .get_metadata_chain(&deriv_id)
            .unwrap()
            .iter_blocks()
            .count(),
        2
    );

    let part_file = match dataset_layout.data_dir.read_dir().unwrap().next() {
        Some(Ok(entry)) => entry.path(),
        _ => panic!(
            "Data file not found in {}",
            dataset_layout.data_dir.display()
        ),
    };

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
}

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_transform_with_engine_flink() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    let metadata_repo = Rc::new(RefCell::new(MetadataRepositoryImpl::new(&workspace_layout)));
    let engine_factory = Arc::new(Mutex::new(EngineFactory::new(&workspace_layout)));

    let mut ingest_svc = IngestServiceImpl::new(
        metadata_repo.clone(),
        engine_factory.clone(),
        &volume_layout,
        slog::Logger::root(slog::Discard, slog::o!()),
    );

    let mut transform_svc = TransformServiceImpl::new(
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

    metadata_repo
        .borrow_mut()
        .add_dataset(root_snapshot)
        .unwrap();

    ingest_svc.ingest(&root_id, None).unwrap();

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

    metadata_repo
        .borrow_mut()
        .add_dataset(deriv_snapshot)
        .unwrap();

    let res = transform_svc.transform(&deriv_id, None).unwrap();
    assert!(matches!(res, TransformResult::Updated {.. }));

    let dataset_layout = DatasetLayout::new(&volume_layout, &deriv_id);
    assert!(dataset_layout.data_dir.exists());
    assert_eq!(
        metadata_repo
            .borrow_mut()
            .get_metadata_chain(&deriv_id)
            .unwrap()
            .iter_blocks()
            .count(),
        2
    );

    let part_file = dataset_layout
        .data_dir
        .read_dir()
        .unwrap()
        .map(|r| r.unwrap())
        .filter_map(|e| {
            let p = e.path();
            if p.file_name().unwrap().to_str().unwrap().starts_with(".") {
                None
            } else {
                Some(p)
            }
        })
        .next()
        .expect("Data file not found");

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
}
