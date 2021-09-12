// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;

use chrono::{TimeZone, Utc};
use std::sync::Arc;

macro_rules! map(
    { } => { ::std::collections::BTreeMap::new() };
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = ::std::collections::BTreeMap::new();
            $(
                m.insert($key, $value);
            )+
            m
        }
     };
);

fn new_root(metadata_repo: &MetadataRepositoryImpl, id_str: &'static str) -> &'static DatasetID {
    let id = DatasetID::new_unchecked(id_str);
    let snap = MetadataFactory::dataset_snapshot()
        .id(id)
        .source(MetadataFactory::dataset_source_root().build())
        .build();

    metadata_repo.add_dataset(snap).unwrap();
    id
}

fn new_deriv(
    metadata_repo: &MetadataRepositoryImpl,
    id_str: &'static str,
    inputs: &[&'static DatasetID],
) -> (&'static DatasetID, DatasetSourceDerivative) {
    let id = DatasetID::new_unchecked(id_str);
    let source = MetadataFactory::dataset_source_deriv(inputs.iter()).build_inner();
    let snap = MetadataFactory::dataset_snapshot()
        .id(id)
        .source(DatasetSource::Derivative(source.clone()))
        .build();

    metadata_repo.add_dataset(snap).unwrap();
    (id, source)
}

fn append_data_block(metadata_repo: &MetadataRepositoryImpl, id: &DatasetID) -> MetadataBlock {
    let mut chain = metadata_repo.get_metadata_chain(id).unwrap();
    chain.append(
        MetadataFactory::metadata_block()
            .prev(&chain.read_ref(&BlockRef::Head).unwrap())
            .output_slice(DataSlice {
                hash: Sha3_256::zero(),
                num_records: 100,
                interval: TimeInterval::singleton(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
            })
            .output_watermark(Utc.ymd(2020, 1, 1).and_hms(10, 0, 0))
            .build(),
    );
    chain.iter_blocks().next().unwrap()
}

#[test]
fn test_get_next_operation() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);
    let metadata_repo = Arc::new(MetadataRepositoryImpl::new(workspace_layout.clone()));
    let transform_svc = TransformServiceImpl::new(
        metadata_repo.clone(),
        Arc::new(EngineFactoryNull),
        &volume_layout,
        slog::Logger::root(slog::Discard, slog::o!()),
    );

    let foo = new_root(&metadata_repo, "foo");
    let foo_layout = DatasetLayout::new(&volume_layout, foo);

    let (bar, bar_source) = new_deriv(&metadata_repo, "bar", &[foo]);

    // No data - no work
    assert_eq!(transform_svc.get_next_operation(bar).unwrap(), None);

    let foo_block = append_data_block(&metadata_repo, foo);

    assert!(matches!(
        transform_svc.get_next_operation(bar).unwrap(),
        Some(ExecuteQueryRequest { source, input_slices, .. })
        if source == bar_source &&
        input_slices == map! { foo.to_owned() => InputDataSlice {
            interval: TimeInterval::unbounded_closed_right(foo_block.system_time),
            data_paths: vec![foo_layout.data_dir.join(foo_block.block_hash.to_string())],
            schema_file: foo_layout.data_dir.join(foo_block.block_hash.to_string()),
            explicit_watermarks: vec![Watermark {
                system_time: foo_block.system_time,
                event_time: Utc.ymd(2020, 1, 1).and_hms(10, 0, 0),
            }],
        }}
    ));
}

#[test]
fn test_get_verification_plan_one_to_one() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);
    let metadata_repo = Arc::new(MetadataRepositoryImpl::new(workspace_layout.clone()));
    let transform_svc = TransformServiceImpl::new(
        metadata_repo.clone(),
        Arc::new(EngineFactoryNull),
        &volume_layout,
        slog::Logger::root(slog::Discard, slog::o!()),
    );

    // Create root dataset
    let t0 = Utc.ymd(2020, 1, 1).and_hms(11, 0, 0);
    let root_id = DatasetID::new_unchecked("foo");
    let root_layout = DatasetLayout::create(&volume_layout, root_id).unwrap();
    let root_head_src = metadata_repo
        .add_dataset_from_block(
            root_id,
            MetadataFactory::metadata_block()
                .system_time(t0)
                .source(MetadataFactory::dataset_source_root().build())
                .build(),
        )
        .unwrap();

    // Create derivative
    let deriv_id = DatasetID::new_unchecked("bar");
    let deriv_head_src = metadata_repo
        .add_dataset_from_block(
            deriv_id,
            MetadataFactory::metadata_block()
                .system_time(t0)
                .source(MetadataFactory::dataset_source_deriv([root_id].iter()).build())
                .build(),
        )
        .unwrap();

    // T1: Root data added
    let t1 = Utc.ymd(2020, 1, 1).and_hms(12, 0, 0);
    let root_head_t1 = metadata_repo.get_metadata_chain(root_id).unwrap().append(
        MetadataFactory::metadata_block()
            .system_time(t1)
            .prev(&root_head_src)
            .output_slice(DataSlice {
                hash: Sha3_256::zero(),
                num_records: 100,
                interval: TimeInterval::singleton(t1),
            })
            .output_watermark(t0)
            .build(),
    );
    std::fs::write(
        root_layout.data_dir.join(root_head_t1.to_string()),
        "<data>",
    )
    .unwrap();

    // T2: Transform (-inf; T1]
    let t2 = Utc.ymd(2020, 1, 2).and_hms(12, 0, 0);
    let deriv_req_t2 = transform_svc.get_next_operation(deriv_id).unwrap().unwrap();
    let deriv_head_t2 = metadata_repo.get_metadata_chain(deriv_id).unwrap().append(
        MetadataFactory::metadata_block()
            .system_time(t2)
            .prev(&deriv_head_src)
            .input_slice(DataSlice {
                hash: Sha3_256::zero(),
                num_records: 100,
                interval: TimeInterval::unbounded_closed_right(t1),
            })
            .output_slice(DataSlice {
                hash: Sha3_256::zero(),
                num_records: 100,
                interval: TimeInterval::singleton(t2),
            })
            .output_watermark(t0)
            .build(),
    );

    // T3: More root data
    let t3 = Utc.ymd(2020, 1, 3).and_hms(12, 0, 0);
    let root_head_t3 = metadata_repo.get_metadata_chain(root_id).unwrap().append(
        MetadataFactory::metadata_block()
            .system_time(t3)
            .prev(&root_head_t1)
            .output_slice(DataSlice {
                hash: Sha3_256::zero(),
                num_records: 10,
                interval: TimeInterval::singleton(t3),
            })
            .output_watermark(t2)
            .build(),
    );
    std::fs::write(
        root_layout.data_dir.join(root_head_t3.to_string()),
        "<data>",
    )
    .unwrap();

    // T4: Transform (T1; T3]
    let t4 = Utc.ymd(2020, 1, 4).and_hms(12, 0, 0);
    let deriv_req_t4 = transform_svc.get_next_operation(deriv_id).unwrap().unwrap();
    let deriv_head_t4 = metadata_repo.get_metadata_chain(deriv_id).unwrap().append(
        MetadataFactory::metadata_block()
            .system_time(t4)
            .prev(&deriv_head_t2)
            .input_slice(DataSlice {
                hash: Sha3_256::zero(),
                num_records: 100,
                interval: TimeInterval::left_half_open(t1, t3).unwrap(),
            })
            .output_slice(DataSlice {
                hash: Sha3_256::zero(),
                num_records: 100,
                interval: TimeInterval::singleton(t4),
            })
            .output_watermark(t2)
            .build(),
    );

    // T5: Root watermark update only
    let t5 = Utc.ymd(2020, 1, 5).and_hms(12, 0, 0);
    let _root_head_t5 = metadata_repo.get_metadata_chain(root_id).unwrap().append(
        MetadataFactory::metadata_block()
            .system_time(t5)
            .prev(&root_head_t3)
            .output_watermark(t4)
            .build(),
    );

    // T6: Transform (T3; T5]
    let t6 = Utc.ymd(2020, 1, 6).and_hms(12, 0, 0);
    let deriv_req_t6 = transform_svc.get_next_operation(deriv_id).unwrap().unwrap();
    let deriv_head_t6 = metadata_repo.get_metadata_chain(deriv_id).unwrap().append(
        MetadataFactory::metadata_block()
            .system_time(t6)
            .prev(&deriv_head_t4)
            .input_slice(DataSlice {
                hash: Sha3_256::zero(),
                num_records: 0,
                interval: TimeInterval::left_half_open(t3, t5).unwrap(),
            })
            .output_slice(DataSlice {
                hash: Sha3_256::zero(),
                num_records: 10,
                interval: TimeInterval::singleton(t6),
            })
            .output_watermark(t4)
            .build(),
    );

    let plan = transform_svc
        .get_verification_plan(deriv_id, (None, None))
        .unwrap();

    assert_eq!(plan.len(), 3);

    assert_eq!(
        plan[0].expected,
        metadata_repo
            .get_metadata_chain(deriv_id)
            .unwrap()
            .get_block(&deriv_head_t2)
            .unwrap()
    );

    assert_eq!(
        plan[1].expected,
        metadata_repo
            .get_metadata_chain(deriv_id)
            .unwrap()
            .get_block(&deriv_head_t4)
            .unwrap()
    );

    assert_eq!(
        plan[2].expected,
        metadata_repo
            .get_metadata_chain(deriv_id)
            .unwrap()
            .get_block(&deriv_head_t6)
            .unwrap()
    );

    assert_eq!(plan[0].request.input_slices, deriv_req_t2.input_slices);
    assert_eq!(plan[0].request, deriv_req_t2);
    assert_eq!(plan[1].request.input_slices, deriv_req_t4.input_slices);
    assert_eq!(plan[1].request, deriv_req_t4);
    assert_eq!(plan[2].request.input_slices, deriv_req_t6.input_slices);
    assert_eq!(plan[2].request, deriv_req_t6);
}
