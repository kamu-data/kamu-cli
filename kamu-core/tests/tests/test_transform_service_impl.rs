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
use digest::Digest;
use std::sync::Arc;

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

fn append_data_block(
    metadata_repo: &MetadataRepositoryImpl,
    id: &DatasetID,
    records: i64,
) -> MetadataBlock {
    let mut chain = metadata_repo.get_metadata_chain(id).unwrap();
    let offset = chain
        .iter_blocks()
        .filter_map(|b| b.output_slice)
        .map(|s| s.data_interval.end + 1)
        .next()
        .unwrap_or(0);
    chain.append(
        MetadataFactory::metadata_block()
            .prev(&chain.read_ref(&BlockRef::Head).unwrap())
            .output_slice(OutputSlice {
                data_logical_hash: Multihash::new(
                    MulticodecCode::Sha3_256,
                    &sha3::Sha3_256::digest(b"foo"),
                ),
                data_physical_hash: Multihash::new(
                    MulticodecCode::Sha3_256,
                    &sha3::Sha3_256::digest(b"bar"),
                ),
                data_interval: OffsetInterval {
                    start: offset,
                    end: offset + records - 1,
                },
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
        Arc::new(EngineProvisionerNull),
        &volume_layout,
    );

    let foo = new_root(&metadata_repo, "foo");
    let foo_layout = DatasetLayout::new(&volume_layout, foo);

    let (bar, bar_source) = new_deriv(&metadata_repo, "bar", &[foo]);

    // No data - no work
    assert_eq!(
        transform_svc.get_next_operation(bar, Utc::now()).unwrap(),
        None
    );

    let foo_block = append_data_block(&metadata_repo, foo, 10);

    assert!(matches!(
        transform_svc.get_next_operation(bar, Utc::now()).unwrap(),
        Some(TransformOperation{ request: ExecuteQueryRequest { transform, inputs, .. }, ..})
        if transform == bar_source.transform &&
        inputs == vec![QueryInput {
            dataset_id: foo.to_owned(),
            vocab: DatasetVocabulary::default(),
            data_interval: Some(OffsetInterval {start: 0, end: 9}),
            data_paths: vec![foo_layout.data_dir.join(foo_block.block_hash.to_string())],
            schema_file: foo_layout.data_dir.join(foo_block.block_hash.to_string()),
            explicit_watermarks: vec![Watermark {
                system_time: foo_block.system_time,
                event_time: Utc.ymd(2020, 1, 1).and_hms(10, 0, 0),
            }],
        }]
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
        Arc::new(EngineProvisionerNull),
        &volume_layout,
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
            .output_slice(OutputSlice {
                data_logical_hash: Multihash::new(
                    MulticodecCode::Sha3_256,
                    &sha3::Sha3_256::digest(b"foo"),
                ),
                data_physical_hash: Multihash::new(
                    MulticodecCode::Sha3_256,
                    &sha3::Sha3_256::digest(b"bar"),
                ),
                data_interval: OffsetInterval { start: 0, end: 99 },
            })
            .output_watermark(t0)
            .build(),
    );
    std::fs::write(
        root_layout.data_dir.join(root_head_t1.to_string()),
        "<data>",
    )
    .unwrap();

    // T2: Transform [SRC; T1]
    let t2 = Utc.ymd(2020, 1, 2).and_hms(12, 0, 0);
    let deriv_req_t2 = transform_svc
        .get_next_operation(deriv_id, t2)
        .unwrap()
        .unwrap();
    let deriv_head_t2 = metadata_repo.get_metadata_chain(deriv_id).unwrap().append(
        MetadataFactory::metadata_block()
            .system_time(t2)
            .prev(&deriv_head_src)
            .input_slice(InputSlice {
                dataset_id: root_id.to_owned(),
                block_interval: Some(BlockInterval {
                    start: root_head_src,
                    end: root_head_t1,
                }),
                data_interval: Some(OffsetInterval { start: 0, end: 99 }),
            })
            .output_slice(OutputSlice {
                data_logical_hash: Multihash::new(
                    MulticodecCode::Sha3_256,
                    &sha3::Sha3_256::digest(b"foo"),
                ),
                data_physical_hash: Multihash::new(
                    MulticodecCode::Sha3_256,
                    &sha3::Sha3_256::digest(b"bar"),
                ),
                data_interval: OffsetInterval { start: 0, end: 99 },
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
            .output_slice(OutputSlice {
                data_logical_hash: Multihash::new(
                    MulticodecCode::Sha3_256,
                    &sha3::Sha3_256::digest(b"foo"),
                ),
                data_physical_hash: Multihash::new(
                    MulticodecCode::Sha3_256,
                    &sha3::Sha3_256::digest(b"bar"),
                ),
                data_interval: OffsetInterval {
                    start: 100,
                    end: 109,
                },
            })
            .output_watermark(t2)
            .build(),
    );
    std::fs::write(
        root_layout.data_dir.join(root_head_t3.to_string()),
        "<data>",
    )
    .unwrap();

    // T4: Transform [T3; T3]
    let t4 = Utc.ymd(2020, 1, 4).and_hms(12, 0, 0);
    let deriv_req_t4 = transform_svc
        .get_next_operation(deriv_id, t4)
        .unwrap()
        .unwrap();
    let deriv_head_t4 = metadata_repo.get_metadata_chain(deriv_id).unwrap().append(
        MetadataFactory::metadata_block()
            .system_time(t4)
            .prev(&deriv_head_t2)
            .input_slice(InputSlice {
                dataset_id: root_id.to_owned(),
                block_interval: Some(BlockInterval {
                    start: root_head_t3,
                    end: root_head_t3,
                }),
                data_interval: Some(OffsetInterval {
                    start: 100,
                    end: 109,
                }),
            })
            .output_slice(OutputSlice {
                data_logical_hash: Multihash::new(
                    MulticodecCode::Sha3_256,
                    &sha3::Sha3_256::digest(b"foo"),
                ),
                data_physical_hash: Multihash::new(
                    MulticodecCode::Sha3_256,
                    &sha3::Sha3_256::digest(b"bar"),
                ),
                data_interval: OffsetInterval {
                    start: 100,
                    end: 109,
                },
            })
            .output_watermark(t2)
            .build(),
    );

    // T5: Root watermark update only
    let t5 = Utc.ymd(2020, 1, 5).and_hms(12, 0, 0);
    let root_head_t5 = metadata_repo.get_metadata_chain(root_id).unwrap().append(
        MetadataFactory::metadata_block()
            .system_time(t5)
            .prev(&root_head_t3)
            .output_watermark(t4)
            .build(),
    );

    // T6: Transform [T5; T5]
    let t6 = Utc.ymd(2020, 1, 6).and_hms(12, 0, 0);
    let deriv_req_t6 = transform_svc
        .get_next_operation(deriv_id, t6)
        .unwrap()
        .unwrap();
    let deriv_head_t6 = metadata_repo.get_metadata_chain(deriv_id).unwrap().append(
        MetadataFactory::metadata_block()
            .system_time(t6)
            .prev(&deriv_head_t4)
            .input_slice(InputSlice {
                dataset_id: root_id.to_owned(),
                block_interval: Some(BlockInterval {
                    start: root_head_t5,
                    end: root_head_t5,
                }),
                data_interval: None,
            })
            .output_slice(OutputSlice {
                data_logical_hash: Multihash::new(
                    MulticodecCode::Sha3_256,
                    &sha3::Sha3_256::digest(b"foo"),
                ),
                data_physical_hash: Multihash::new(
                    MulticodecCode::Sha3_256,
                    &sha3::Sha3_256::digest(b"bar"),
                ),
                data_interval: OffsetInterval {
                    start: 110,
                    end: 119,
                },
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

    assert_eq!(
        plan[0].operation.request.inputs,
        deriv_req_t2.request.inputs
    );
    assert_eq!(plan[0].operation.request, deriv_req_t2.request);
    assert_eq!(
        plan[1].operation.request.inputs,
        deriv_req_t4.request.inputs
    );
    assert_eq!(plan[1].operation.request, deriv_req_t4.request);
    assert_eq!(
        plan[2].operation.request.inputs,
        deriv_req_t6.request.inputs
    );
    assert_eq!(plan[2].operation.request, deriv_req_t6.request);
}
