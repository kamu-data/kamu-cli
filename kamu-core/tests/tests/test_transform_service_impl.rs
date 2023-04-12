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
use futures::TryStreamExt;
use std::sync::Arc;

async fn new_root(local_repo: &dyn DatasetRepository, name: &str) -> DatasetHandle {
    let name = DatasetName::try_from(name).unwrap();
    let snap = MetadataFactory::dataset_snapshot()
        .name(name)
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let create_result = local_repo.create_dataset_from_snapshot(snap).await.unwrap();
    create_result.dataset_handle
}

async fn new_deriv(
    local_repo: &dyn DatasetRepository,
    name: &str,
    inputs: &[DatasetAlias],
) -> (DatasetHandle, SetTransform) {
    let name = DatasetName::try_from(name).unwrap();
    let transform =
        MetadataFactory::set_transform(inputs.iter().map(|d| d.dataset_name.clone())).build();
    let snap = MetadataFactory::dataset_snapshot()
        .name(name)
        .kind(DatasetKind::Derivative)
        .push_event(transform.clone())
        .build();

    let create_result = local_repo.create_dataset_from_snapshot(snap).await.unwrap();
    (create_result.dataset_handle, transform)
}

async fn append_block(
    local_repo: &dyn DatasetRepository,
    dataset_ref: impl Into<DatasetRef>,
    block: MetadataBlock,
) -> Multihash {
    let ds = local_repo.get_dataset(&dataset_ref.into()).await.unwrap();
    ds.as_metadata_chain()
        .append(block, AppendOpts::default())
        .await
        .unwrap()
}

async fn append_data_block(
    local_repo: &dyn DatasetRepository,
    alias: &DatasetAlias,
    records: i64,
) -> (Multihash, MetadataBlockTyped<AddData>) {
    let ds = local_repo.get_dataset(&alias.as_local_ref()).await.unwrap();
    let chain = ds.as_metadata_chain();
    let offset = chain
        .iter_blocks()
        .filter_map_ok(|(_, b)| b.event.into_variant::<AddData>())
        .map_ok(|e| e.output_data.interval.end + 1)
        .try_first()
        .await
        .unwrap()
        .unwrap_or(0);

    let prev_head = chain.get_ref(&BlockRef::Head).await.unwrap();
    let prev_block = chain.get_block(&prev_head).await.unwrap();

    let block = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .interval(offset, offset + records - 1)
            .watermark(Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 0).unwrap())
            .build(),
    )
    .prev(&prev_head, prev_block.sequence_number)
    .build();

    let block_hash = chain
        .append(block.clone(), AppendOpts::default())
        .await
        .unwrap();
    (block_hash, block.into_typed::<AddData>().unwrap())
}

#[test_log::test(tokio::test)]
async fn test_get_next_operation() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let local_repo = Arc::new(DatasetRepositoryLocalFs::new(workspace_layout.clone()));
    let transform_svc = TransformServiceImpl::new(
        local_repo.clone(),
        Arc::new(EngineProvisionerNull),
        workspace_layout.clone(),
    );

    let foo = new_root(local_repo.as_ref(), "foo").await;
    let foo_layout = workspace_layout.dataset_layout(&foo.alias);

    let (bar, bar_source) = new_deriv(local_repo.as_ref(), "bar", &[foo.alias.clone()]).await;

    // No data - no work
    assert_eq!(
        transform_svc
            .get_next_operation(&bar, Utc::now())
            .await
            .unwrap(),
        None
    );

    let (_, foo_block) = append_data_block(local_repo.as_ref(), &foo.alias, 10).await;
    let data_path = foo_layout.data_slice_path(&foo_block.event.output_data);

    assert!(matches!(
        transform_svc.get_next_operation(&bar, Utc::now()).await.unwrap(),
        Some(TransformOperation{ request: ExecuteQueryRequest { transform, inputs, .. }, ..})
        if transform == bar_source.transform &&
        inputs == vec![ExecuteQueryInput {
            dataset_id: foo.id.clone(),
            dataset_name: foo.alias.dataset_name.clone(),
            vocab: DatasetVocabulary::default(),
            data_interval: Some(OffsetInterval {start: 0, end: 9}),
            data_paths: vec![data_path.clone()],
            schema_file: data_path,
            explicit_watermarks: vec![Watermark {
                system_time: foo_block.system_time,
                event_time: Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 0).unwrap(),
            }],
        }]
    ));
}

#[test_log::test(tokio::test)]
async fn test_get_verification_plan_one_to_one() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());
    let local_repo = Arc::new(DatasetRepositoryLocalFs::new(workspace_layout.clone()));
    let transform_svc = TransformServiceImpl::new(
        local_repo.clone(),
        Arc::new(EngineProvisionerNull),
        workspace_layout.clone(),
    );

    // Create root dataset
    let t0 = Utc.with_ymd_and_hms(2020, 1, 1, 11, 0, 0).unwrap();
    let root_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let root_layout = workspace_layout.dataset_layout(&root_alias);
    let root_create_result = local_repo
        .create_dataset_from_blocks(
            &root_alias,
            [
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(DatasetKind::Root)
                        .id_from(root_alias.dataset_name.as_str())
                        .build(),
                )
                .system_time(t0)
                .build(),
                MetadataFactory::metadata_block(MetadataFactory::set_polling_source().build())
                    .system_time(t0)
                    .build(),
            ],
        )
        .await
        .unwrap();

    let root_hdl = root_create_result.dataset_handle;
    let root_head_src = root_create_result.head;
    let root_initial_sequence_number = root_create_result.head_sequence_number;

    // Create derivative
    let deriv_alias = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));
    let deriv_create_result = local_repo
        .create_dataset_from_blocks(
            &deriv_alias,
            [
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(DatasetKind::Derivative)
                        .id_from(deriv_alias.dataset_name.as_str())
                        .build(),
                )
                .system_time(t0)
                .build(),
                MetadataFactory::metadata_block(
                    MetadataFactory::set_transform([&root_alias.dataset_name])
                        .input_ids_from_names()
                        .build(),
                )
                .system_time(t0)
                .build(),
            ],
        )
        .await
        .unwrap();

    let deriv_hdl = deriv_create_result.dataset_handle;
    let deriv_head_src = deriv_create_result.head;
    let deriv_initial_sequence_number = deriv_create_result.head_sequence_number;

    // T1: Root data added
    let t1 = Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap();
    let root_head_t1 = append_block(
        local_repo.as_ref(),
        &root_hdl,
        MetadataFactory::metadata_block(AddData {
            input_checkpoint: None,
            output_data: DataSlice {
                logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                interval: OffsetInterval { start: 0, end: 99 },
                size: 10,
            },
            output_checkpoint: None,
            output_watermark: Some(t0),
        })
        .system_time(t1)
        .prev(&root_head_src, root_initial_sequence_number)
        .build(),
    )
    .await;
    std::fs::write(
        root_layout.data_dir.join(root_head_t1.to_string()),
        "<data>",
    )
    .unwrap();

    // T2: Transform [SRC; T1]
    let t2 = Utc.with_ymd_and_hms(2020, 1, 2, 12, 0, 0).unwrap();
    let deriv_req_t2 = transform_svc
        .get_next_operation(&deriv_hdl, t2)
        .await
        .unwrap()
        .unwrap();
    let deriv_head_t2 = append_block(
        local_repo.as_ref(),
        &deriv_hdl,
        MetadataFactory::metadata_block(ExecuteQuery {
            input_slices: vec![InputSlice {
                dataset_id: root_hdl.id.clone(),
                block_interval: Some(BlockInterval {
                    start: root_head_src.clone(),
                    end: root_head_t1.clone(),
                }),
                data_interval: Some(OffsetInterval { start: 0, end: 99 }),
            }],
            input_checkpoint: None,
            output_data: Some(DataSlice {
                logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                interval: OffsetInterval { start: 0, end: 99 },
                size: 10,
            }),
            output_checkpoint: None,
            output_watermark: Some(t0),
        })
        .system_time(t2)
        .prev(&deriv_head_src, deriv_initial_sequence_number)
        .build(),
    )
    .await;

    // T3: More root data
    let t3 = Utc.with_ymd_and_hms(2020, 1, 3, 12, 0, 0).unwrap();
    let root_head_t3 = append_block(
        local_repo.as_ref(),
        &root_hdl,
        MetadataFactory::metadata_block(AddData {
            input_checkpoint: None,
            output_data: DataSlice {
                logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                interval: OffsetInterval {
                    start: 100,
                    end: 109,
                },
                size: 10,
            },
            output_checkpoint: None,
            output_watermark: Some(t2),
        })
        .system_time(t3)
        .prev(&root_head_t1, root_initial_sequence_number + 1)
        .build(),
    )
    .await;
    std::fs::write(
        root_layout.data_dir.join(root_head_t3.to_string()),
        "<data>",
    )
    .unwrap();

    // T4: Transform [T3; T3]
    let t4 = Utc.with_ymd_and_hms(2020, 1, 4, 12, 0, 0).unwrap();
    let deriv_req_t4 = transform_svc
        .get_next_operation(&deriv_hdl, t4)
        .await
        .unwrap()
        .unwrap();
    let deriv_head_t4 = append_block(
        local_repo.as_ref(),
        &deriv_hdl,
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
                size: 10,
            }),
            output_checkpoint: None,
            output_watermark: Some(t2),
        })
        .system_time(t4)
        .prev(&deriv_head_t2, deriv_initial_sequence_number + 1)
        .build(),
    )
    .await;

    // T5: Root watermark update only
    let t5 = Utc.with_ymd_and_hms(2020, 1, 5, 12, 0, 0).unwrap();
    let root_head_t5 = append_block(
        local_repo.as_ref(),
        &root_hdl,
        MetadataFactory::metadata_block(SetWatermark {
            output_watermark: t4,
        })
        .system_time(t5)
        .prev(&root_head_t3, root_initial_sequence_number + 2)
        .build(),
    )
    .await;

    // T6: Transform [T5; T5]
    let t6 = Utc.with_ymd_and_hms(2020, 1, 6, 12, 0, 0).unwrap();
    let deriv_req_t6 = transform_svc
        .get_next_operation(&deriv_hdl, t6)
        .await
        .unwrap()
        .unwrap();
    let deriv_head_t6 = append_block(
        local_repo.as_ref(),
        &deriv_hdl,
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
                size: 10,
            }),
            output_checkpoint: None,
            output_watermark: Some(t4),
        })
        .system_time(t6)
        .prev(&deriv_head_t4, deriv_initial_sequence_number + 2)
        .build(),
    )
    .await;

    let plan = transform_svc
        .get_verification_plan(&deriv_hdl, (None, None))
        .await
        .unwrap();

    let deriv_ds = local_repo
        .get_dataset(&deriv_hdl.as_local_ref())
        .await
        .unwrap();
    let deriv_chain = deriv_ds.as_metadata_chain();

    assert_eq!(plan.len(), 3);

    assert_eq!(plan[0].expected_hash, deriv_head_t2);
    assert_eq!(
        plan[0].expected_block,
        deriv_chain.get_block(&deriv_head_t2).await.unwrap()
    );

    assert_eq!(plan[1].expected_hash, deriv_head_t4);
    assert_eq!(
        plan[1].expected_block,
        deriv_chain.get_block(&deriv_head_t4).await.unwrap()
    );

    assert_eq!(plan[2].expected_hash, deriv_head_t6);
    assert_eq!(
        plan[2].expected_block,
        deriv_chain.get_block(&deriv_head_t6).await.unwrap()
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
