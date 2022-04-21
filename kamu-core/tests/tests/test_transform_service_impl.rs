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

fn new_root(dataset_reg: &DatasetRegistryImpl, name: &str) -> DatasetHandle {
    let name = DatasetName::try_from(name).unwrap();
    let snap = MetadataFactory::dataset_snapshot()
        .name(name)
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let (handle, _head) = dataset_reg.add_dataset(snap).unwrap();
    handle
}

fn new_deriv(
    dataset_reg: &DatasetRegistryImpl,
    name: &str,
    inputs: &[DatasetName],
) -> (DatasetHandle, SetTransform) {
    let name = DatasetName::try_from(name).unwrap();
    let transform = MetadataFactory::set_transform(inputs).build();
    let snap = MetadataFactory::dataset_snapshot()
        .name(name)
        .kind(DatasetKind::Derivative)
        .push_event(transform.clone())
        .build();

    let (handle, _head) = dataset_reg.add_dataset(snap).unwrap();
    (handle, transform)
}

fn append_data_block(
    dataset_reg: &DatasetRegistryImpl,
    name: &DatasetName,
    records: i64,
) -> (Multihash, MetadataBlockTyped<AddData>) {
    let mut chain = dataset_reg
        .get_metadata_chain(&name.as_local_ref())
        .unwrap();
    let offset = chain
        .iter_blocks()
        .filter_map(|(_, b)| b.event.into_variant::<AddData>())
        .map(|e| e.output_data.interval.end + 1)
        .next()
        .unwrap_or(0);

    let block = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .interval(offset, offset + records - 1)
            .watermark(Utc.ymd(2020, 1, 1).and_hms(10, 0, 0))
            .build(),
    )
    .prev(&chain.read_ref(&BlockRef::Head).unwrap())
    .build();

    let block_hash = chain.append(block.clone());
    (block_hash, block.into_typed::<AddData>().unwrap())
}

#[test]
fn test_get_next_operation() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);
    let dataset_reg = Arc::new(DatasetRegistryImpl::new(workspace_layout.clone()));
    let transform_svc = TransformServiceImpl::new(
        dataset_reg.clone(),
        Arc::new(EngineProvisionerNull),
        &volume_layout,
    );

    let foo = new_root(&dataset_reg, "foo");
    let foo_layout = DatasetLayout::new(&volume_layout, &foo.name);

    let (bar, bar_source) = new_deriv(&dataset_reg, "bar", &[foo.name.clone()]);

    // No data - no work
    assert_eq!(
        transform_svc.get_next_operation(&bar, Utc::now()).unwrap(),
        None
    );

    let (_, foo_block) = append_data_block(&dataset_reg, &foo.name, 10);
    let data_path = foo_layout.data_slice_path(&foo_block.event.output_data);

    assert!(matches!(
        transform_svc.get_next_operation(&bar, Utc::now()).unwrap(),
        Some(TransformOperation{ request: ExecuteQueryRequest { transform, inputs, .. }, ..})
        if transform == bar_source.transform &&
        inputs == vec![ExecuteQueryInput {
            dataset_id: foo.id.clone(),
            dataset_name: foo.name.clone(),
            vocab: DatasetVocabulary::default(),
            data_interval: Some(OffsetInterval {start: 0, end: 9}),
            data_paths: vec![data_path.clone()],
            schema_file: data_path,
            explicit_watermarks: vec![Watermark {
                system_time: foo_block.system_time,
                event_time: Utc.ymd(2020, 1, 1).and_hms(10, 0, 0),
            }],
        }]
    ));
}

#[test_log::test]
fn test_get_verification_plan_one_to_one() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);
    let dataset_reg = Arc::new(DatasetRegistryImpl::new(workspace_layout.clone()));
    let transform_svc = TransformServiceImpl::new(
        dataset_reg.clone(),
        Arc::new(EngineProvisionerNull),
        &volume_layout,
    );

    // Create root dataset
    let t0 = Utc.ymd(2020, 1, 1).and_hms(11, 0, 0);
    let root_name = DatasetName::new_unchecked("foo");
    let root_layout = DatasetLayout::create(&volume_layout, &root_name).unwrap();
    let (root_hdl, root_head_src) = dataset_reg
        .add_dataset_from_blocks(
            &root_name,
            &mut [
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(DatasetKind::Root)
                        .id_from(root_name.as_str())
                        .build(),
                )
                .system_time(t0)
                .build(),
                MetadataFactory::metadata_block(MetadataFactory::set_polling_source().build())
                    .system_time(t0)
                    .build(),
            ]
            .into_iter(),
        )
        .unwrap();

    // Create derivative
    let deriv_name = DatasetName::new_unchecked("bar");
    let (deriv_hdl, deriv_head_src) = dataset_reg
        .add_dataset_from_blocks(
            &deriv_name,
            &mut [
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(DatasetKind::Derivative)
                        .id_from(deriv_name.as_str())
                        .build(),
                )
                .system_time(t0)
                .build(),
                MetadataFactory::metadata_block(
                    MetadataFactory::set_transform([&root_name])
                        .input_ids_from_names()
                        .build(),
                )
                .system_time(t0)
                .build(),
            ]
            .into_iter(),
        )
        .unwrap();

    // T1: Root data added
    let t1 = Utc.ymd(2020, 1, 1).and_hms(12, 0, 0);
    let root_head_t1 = dataset_reg
        .get_metadata_chain(&root_hdl.as_local_ref())
        .unwrap()
        .append(
            MetadataFactory::metadata_block(AddData {
                input_checkpoint: None,
                output_data: DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                    interval: OffsetInterval { start: 0, end: 99 },
                },
                output_checkpoint: None,
                output_watermark: Some(t0),
            })
            .system_time(t1)
            .prev(&root_head_src)
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
        .get_next_operation(&deriv_hdl, t2)
        .unwrap()
        .unwrap();
    let deriv_head_t2 = dataset_reg
        .get_metadata_chain(&deriv_hdl.as_local_ref())
        .unwrap()
        .append(
            MetadataFactory::metadata_block(ExecuteQuery {
                input_slices: vec![InputSlice {
                    dataset_id: root_hdl.id.clone(),
                    block_interval: Some(BlockInterval {
                        start: root_head_src,
                        end: root_head_t1.clone(),
                    }),
                    data_interval: Some(OffsetInterval { start: 0, end: 99 }),
                }],
                input_checkpoint: None,
                output_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                    interval: OffsetInterval { start: 0, end: 99 },
                }),
                output_checkpoint: None,
                output_watermark: Some(t0),
            })
            .system_time(t2)
            .prev(&deriv_head_src)
            .build(),
        );

    // T3: More root data
    let t3 = Utc.ymd(2020, 1, 3).and_hms(12, 0, 0);
    let root_head_t3 = dataset_reg
        .get_metadata_chain(&root_hdl.as_local_ref())
        .unwrap()
        .append(
            MetadataFactory::metadata_block(AddData {
                input_checkpoint: None,
                output_data: DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                    interval: OffsetInterval {
                        start: 100,
                        end: 109,
                    },
                },
                output_checkpoint: None,
                output_watermark: Some(t2),
            })
            .system_time(t3)
            .prev(&root_head_t1)
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
        .get_next_operation(&deriv_hdl, t4)
        .unwrap()
        .unwrap();
    let deriv_head_t4 = dataset_reg
        .get_metadata_chain(&deriv_hdl.as_local_ref())
        .unwrap()
        .append(
            MetadataFactory::metadata_block(ExecuteQuery {
                input_slices: vec![InputSlice {
                    dataset_id: root_hdl.id.clone(),
                    block_interval: Some(BlockInterval {
                        start: root_head_t3.clone(),
                        end: root_head_t3.clone(),
                    }),
                    data_interval: Some(OffsetInterval {
                        start: 100,
                        end: 109,
                    }),
                }],
                input_checkpoint: None,
                output_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                    interval: OffsetInterval {
                        start: 100,
                        end: 109,
                    },
                }),
                output_checkpoint: None,
                output_watermark: Some(t2),
            })
            .system_time(t4)
            .prev(&deriv_head_t2)
            .build(),
        );

    // T5: Root watermark update only
    let t5 = Utc.ymd(2020, 1, 5).and_hms(12, 0, 0);
    let root_head_t5 = dataset_reg
        .get_metadata_chain(&root_hdl.as_local_ref())
        .unwrap()
        .append(
            MetadataFactory::metadata_block(SetWatermark {
                output_watermark: t4,
            })
            .system_time(t5)
            .prev(&root_head_t3)
            .build(),
        );

    // T6: Transform [T5; T5]
    let t6 = Utc.ymd(2020, 1, 6).and_hms(12, 0, 0);
    let deriv_req_t6 = transform_svc
        .get_next_operation(&deriv_hdl, t6)
        .unwrap()
        .unwrap();
    let deriv_head_t6 = dataset_reg
        .get_metadata_chain(&deriv_hdl.as_local_ref())
        .unwrap()
        .append(
            MetadataFactory::metadata_block(ExecuteQuery {
                input_slices: vec![InputSlice {
                    dataset_id: root_hdl.id.clone(),
                    block_interval: Some(BlockInterval {
                        start: root_head_t5.clone(),
                        end: root_head_t5.clone(),
                    }),
                    data_interval: None,
                }],
                input_checkpoint: None,
                output_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                    interval: OffsetInterval {
                        start: 110,
                        end: 119,
                    },
                }),
                output_checkpoint: None,
                output_watermark: Some(t4),
            })
            .system_time(t6)
            .prev(&deriv_head_t4)
            .build(),
        );

    let plan = transform_svc
        .get_verification_plan(&deriv_hdl, (None, None))
        .unwrap();

    let deriv_chain = dataset_reg
        .get_metadata_chain(&deriv_hdl.as_local_ref())
        .unwrap();

    assert_eq!(plan.len(), 3);

    assert_eq!(plan[0].expected_hash, deriv_head_t2);
    assert_eq!(
        plan[0].expected_block,
        deriv_chain.get_block(&deriv_head_t2).unwrap()
    );

    assert_eq!(plan[1].expected_hash, deriv_head_t4);
    assert_eq!(
        plan[1].expected_block,
        deriv_chain.get_block(&deriv_head_t4).unwrap()
    );

    assert_eq!(plan[2].expected_hash, deriv_head_t6);
    assert_eq!(
        plan[2].expected_block,
        deriv_chain.get_block(&deriv_head_t6).unwrap()
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
