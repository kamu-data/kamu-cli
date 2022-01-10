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

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_ingest_with_engine() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    let dataset_reg = Arc::new(DatasetRegistryImpl::new(workspace_layout.clone()));

    let engine_provisioner = Arc::new(EngineProvisionerLocal::new(
        EngineProvisionerLocalConfig::default(),
        workspace_layout.clone(),
        ContainerRuntime::default(),
    ));

    let ingest_svc = Arc::new(IngestServiceImpl::new(
        &volume_layout,
        dataset_reg.clone(),
        engine_provisioner,
    ));

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
        .name("foo.bar")
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

    let dataset_name = dataset_snapshot.name.clone();

    dataset_reg.add_dataset(dataset_snapshot).unwrap();

    let res = ingest_svc
        .ingest(&dataset_name.as_local_ref(), IngestOptions::default(), None)
        .await;
    assert_matches!(res, Ok(IngestResult::Updated { .. }));

    let dataset_layout = DatasetLayout::new(&volume_layout, &dataset_name);
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

    assert_eq!(
        columns,
        ["offset", "system_time", "event_time", "city", "population"]
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
            (0, "A".to_owned(), 1000),
            (1, "B".to_owned(), 2000),
            (2, "C".to_owned(), 3000)
        ]
    );
}
