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
fn test_ingest_with_engine() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    let metadata_repo = Rc::new(RefCell::new(MetadataRepositoryImpl::new(&workspace_layout)));
    let ingest_svc = Rc::new(RefCell::new(IngestServiceImpl::new(
        metadata_repo.clone(),
        Arc::new(Mutex::new(EngineFactory::new(
            &workspace_layout,
            slog::Logger::root(slog::Discard, slog::o!()),
        ))),
        &volume_layout,
        slog::Logger::root(slog::Discard, slog::o!()),
    )));

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

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .id("foo.bar")
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

    let dataset_id = dataset_snapshot.id.clone();

    metadata_repo
        .borrow_mut()
        .add_dataset(dataset_snapshot)
        .unwrap();

    let res = ingest_svc.borrow_mut().ingest(&dataset_id, None);
    assert_ok!(res, IngestResult::Updated {..});

    let dataset_layout = DatasetLayout::new(&volume_layout, &dataset_id);
    assert!(dataset_layout.data_dir.exists());

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

    assert_eq!(columns, ["system_time", "event_time", "city", "population"]);

    let records: Vec<_> = parquet_reader
        .get_row_iter(None)
        .unwrap()
        .map(|r| (r.get_string(2).unwrap().clone(), r.get_int(3).unwrap()))
        .collect();

    assert_eq!(
        records,
        [
            ("A".to_owned(), 1000),
            ("B".to_owned(), 2000),
            ("C".to_owned(), 3000)
        ]
    );
}
