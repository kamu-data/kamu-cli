// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use futures::TryStreamExt;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use opendatafabric::*;
use tempfile::TempDir;

use crate::mock_engine_provisioner;

/////////////////////////////////////////////////////////////////////////////////////////

struct TransformTestHarness {
    _tempdir: TempDir,
    dataset_repo: Arc<dyn DatasetRepository>,
    transform_service: TransformServiceImpl,
}

impl TransformTestHarness {
    pub fn new_custom(
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
    ) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let dataset_repo = Arc::new(
            DatasetRepositoryLocalFs::create(
                tempdir.path().join("datasets"),
                Arc::new(CurrentAccountSubject::new_test()),
                dataset_action_authorizer.clone(),
                false,
            )
            .unwrap(),
        );
        let transform_service = TransformServiceImpl::new(
            dataset_repo.clone(),
            dataset_action_authorizer.clone(),
            engine_provisioner,
        );

        Self {
            _tempdir: tempdir,
            dataset_repo,
            transform_service,
        }
    }

    pub fn new() -> Self {
        Self::new_custom(
            Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
            Arc::new(EngineProvisionerNull),
        )
    }

    pub async fn new_root(&self, name: &str) -> DatasetHandle {
        let name = DatasetName::try_from(name).unwrap();
        let snap = MetadataFactory::dataset_snapshot()
            .name(name)
            .kind(DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build();

        let create_result = self
            .dataset_repo
            .create_dataset_from_snapshot(None, snap)
            .await
            .unwrap();
        create_result.dataset_handle
    }

    async fn new_deriv(
        &self,
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

        let create_result = self
            .dataset_repo
            .create_dataset_from_snapshot(None, snap)
            .await
            .unwrap();
        (create_result.dataset_handle, transform)
    }

    pub async fn append_block(
        &self,
        dataset_ref: impl Into<DatasetRef>,
        block: MetadataBlock,
    ) -> Multihash {
        let ds = self
            .dataset_repo
            .get_dataset(&dataset_ref.into())
            .await
            .unwrap();
        ds.as_metadata_chain()
            .append(block, AppendOpts::default())
            .await
            .unwrap()
    }

    pub async fn append_data_block(
        &self,
        alias: &DatasetAlias,
        records: i64,
    ) -> (Multihash, MetadataBlockTyped<AddData>) {
        let ds = self
            .dataset_repo
            .get_dataset(&alias.as_local_ref())
            .await
            .unwrap();
        let chain = ds.as_metadata_chain();
        let offset = chain
            .iter_blocks()
            .filter_map_ok(|(_, b)| b.event.into_variant::<AddData>())
            .map_ok(|e| e.output_data.unwrap().interval.end + 1)
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
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_next_operation() {
    let harness = TransformTestHarness::new();

    let foo = harness.new_root("foo").await;
    let foo_seed = harness
        .dataset_repo
        .get_dataset(&foo.as_local_ref())
        .await
        .unwrap()
        .as_metadata_chain()
        .iter_blocks()
        .try_last()
        .await
        .unwrap()
        .unwrap()
        .0;

    let (bar, bar_source) = harness.new_deriv("bar", &[foo.alias.clone()]).await;

    // No data - no work
    assert_eq!(
        harness
            .transform_service
            .get_next_operation(&bar, Utc::now())
            .await
            .unwrap(),
        None
    );

    let (foo_head, foo_block) = harness.append_data_block(&foo.alias, 10).await;
    let foo_slice = foo_block.event.output_data.as_ref().unwrap();

    assert!(matches!(
        harness.transform_service.get_next_operation(&bar, Utc::now()).await.unwrap(),
        Some(TransformRequest{ transform, inputs, .. })
        if transform == bar_source.transform &&
        inputs == vec![TransformRequestInput {
            dataset_handle: foo.clone(),
            alias: foo.alias.dataset_name.to_string(),
            vocab: DatasetVocabulary::default(),
            block_interval: Some(BlockInterval { start: foo_seed, end: foo_head }),
            data_interval: Some(OffsetInterval {start: 0, end: 9}),
            data_slices: vec![foo_slice.physical_hash.clone()],
            schema_slice: foo_slice.physical_hash.clone(),
            explicit_watermarks: vec![Watermark {
                system_time: foo_block.system_time,
                event_time: Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 0).unwrap(),
            }],
        }]
    ));
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transform_enforces_authorization() {
    let mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new()
        .expect_check_read_dataset(
            DatasetAlias::new(None, DatasetName::new_unchecked("foo")),
            1,
        )
        .expect_check_write_dataset(
            DatasetAlias::new(None, DatasetName::new_unchecked("bar")),
            1,
        );

    let harness = TransformTestHarness::new_custom(
        Arc::new(mock_dataset_action_authorizer),
        Arc::new(mock_engine_provisioner::MockEngineProvisioner::new().stub_provision_engine()),
    );

    let foo = harness.new_root("foo").await;
    let (_, _) = harness.append_data_block(&foo.alias, 10).await;

    let (bar, _) = harness.new_deriv("bar", &[foo.alias.clone()]).await;

    let transform_result = harness
        .transform_service
        .transform(&bar.as_local_ref(), None)
        .await;

    assert_matches!(transform_result, Ok(_));
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transform_unauthorized() {
    let harness = TransformTestHarness::new_custom(
        Arc::new(MockDatasetActionAuthorizer::denying()),
        Arc::new(EngineProvisionerNull),
    );

    let foo = harness.new_root("foo").await;
    let (bar, _) = harness.new_deriv("bar", &[foo.alias.clone()]).await;

    let transform_result = harness
        .transform_service
        .transform(&bar.as_local_ref(), None)
        .await;

    assert_matches!(
        transform_result,
        Err(TransformError::Access(AccessError::Forbidden(_)))
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_verification_plan_one_to_one() {
    let harness = TransformTestHarness::new();

    // Create root dataset
    let t0 = Utc.with_ymd_and_hms(2020, 1, 1, 11, 0, 0).unwrap();
    let root_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let root_create_result = harness
        .dataset_repo
        .create_dataset(
            &root_alias,
            MetadataFactory::metadata_block(
                MetadataFactory::seed(DatasetKind::Root)
                    .id_from(root_alias.dataset_name.as_str())
                    .build(),
            )
            .system_time(t0)
            .build_typed(),
        )
        .await
        .unwrap();

    let root_head_seed = root_create_result.head;
    let root_head_src = root_create_result
        .dataset
        .commit_event(
            MetadataEvent::SetPollingSource(MetadataFactory::set_polling_source().build()),
            CommitOpts {
                system_time: Some(t0),
                ..CommitOpts::default()
            },
        )
        .await
        .unwrap()
        .new_head;

    let root_hdl = root_create_result.dataset_handle;
    let root_initial_sequence_number = 1;

    // Create derivative
    let deriv_alias = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));
    let deriv_create_result = harness
        .dataset_repo
        .create_dataset(
            &deriv_alias,
            MetadataFactory::metadata_block(
                MetadataFactory::seed(DatasetKind::Derivative)
                    .id_from(deriv_alias.dataset_name.as_str())
                    .build(),
            )
            .system_time(t0)
            .build_typed(),
        )
        .await
        .unwrap();

    let deriv_head_src = deriv_create_result
        .dataset
        .commit_event(
            MetadataEvent::SetTransform(
                MetadataFactory::set_transform([&root_alias.dataset_name])
                    .input_ids_from_names()
                    .build(),
            ),
            CommitOpts {
                system_time: Some(t0),
                ..CommitOpts::default()
            },
        )
        .await
        .unwrap()
        .new_head;

    let deriv_hdl = deriv_create_result.dataset_handle;
    let deriv_initial_sequence_number = 1;

    // T1: Root data added
    let t1 = Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap();
    let root_head_t1 = harness
        .append_block(
            &root_hdl,
            MetadataFactory::metadata_block(AddData {
                input_checkpoint: None,
                output_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                    interval: OffsetInterval { start: 0, end: 99 },
                    size: 10,
                }),
                output_checkpoint: None,
                output_watermark: Some(t0),
                source_state: None,
            })
            .system_time(t1)
            .prev(&root_head_src, root_initial_sequence_number)
            .build(),
        )
        .await;

    let root_head_t1_path = kamu_data_utils::data::local_url::into_local_path(
        root_create_result
            .dataset
            .as_data_repo()
            .get_internal_url(&root_head_t1)
            .await,
    )
    .unwrap();
    std::fs::write(root_head_t1_path, "<data>").unwrap();

    // T2: Transform [SEED; T1]
    let t2 = Utc.with_ymd_and_hms(2020, 1, 2, 12, 0, 0).unwrap();
    let deriv_req_t2 = harness
        .transform_service
        .get_next_operation(&deriv_hdl, t2)
        .await
        .unwrap()
        .unwrap();
    let deriv_head_t2 = harness
        .append_block(
            &deriv_hdl,
            MetadataFactory::metadata_block(ExecuteQuery {
                input_slices: vec![InputSlice {
                    dataset_id: root_hdl.id.clone(),
                    block_interval: Some(BlockInterval {
                        start: root_head_seed.clone(),
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
    let root_head_t3 = harness
        .append_block(
            &root_hdl,
            MetadataFactory::metadata_block(AddData {
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
                source_state: None,
            })
            .system_time(t3)
            .prev(&root_head_t1, root_initial_sequence_number + 1)
            .build(),
        )
        .await;
    let root_head_t3_path = kamu_data_utils::data::local_url::into_local_path(
        root_create_result
            .dataset
            .as_data_repo()
            .get_internal_url(&root_head_t3)
            .await,
    )
    .unwrap();
    std::fs::write(root_head_t3_path, "<data>").unwrap();

    // T4: Transform [T3; T3]
    let t4 = Utc.with_ymd_and_hms(2020, 1, 4, 12, 0, 0).unwrap();
    let deriv_req_t4 = harness
        .transform_service
        .get_next_operation(&deriv_hdl, t4)
        .await
        .unwrap()
        .unwrap();
    let deriv_head_t4 = harness
        .append_block(
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
    let root_head_t5 = harness
        .append_block(
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
    let deriv_req_t6 = harness
        .transform_service
        .get_next_operation(&deriv_hdl, t6)
        .await
        .unwrap()
        .unwrap();
    let deriv_head_t6 = harness
        .append_block(
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

    let plan = harness
        .transform_service
        .get_verification_plan(&deriv_hdl, (None, None))
        .await
        .unwrap();

    let deriv_ds = harness
        .dataset_repo
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

    assert_requests_eqivalent(&plan[0].request, deriv_req_t2);
    assert_requests_eqivalent(&plan[1].request, deriv_req_t4);
    assert_requests_eqivalent(&plan[2].request, deriv_req_t6);
}

/////////////////////////////////////////////////////////////////////////////////////////

fn assert_requests_eqivalent(lhs: &TransformRequest, mut rhs: TransformRequest) {
    // Operation IDs are randomly generated, so ignoring them for this check
    rhs.operation_id = lhs.operation_id.clone();

    assert_eq!(lhs.inputs, rhs.inputs);
    assert_eq!(*lhs, rhs);
}

/////////////////////////////////////////////////////////////////////////////////////////
