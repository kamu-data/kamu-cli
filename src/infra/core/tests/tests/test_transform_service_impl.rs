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
use dill::Component;
use event_bus::EventBus;
use futures::TryStreamExt;
use indoc::indoc;
use kamu::domain::engine::*;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use opendatafabric::*;
use tempfile::TempDir;

use crate::mock_engine_provisioner;

/////////////////////////////////////////////////////////////////////////////////////////

struct TransformTestHarness {
    _tempdir: TempDir,
    dataset_repo: Arc<dyn DatasetRepository>,
    transform_service: Arc<TransformServiceImpl>,
    compaction_service: Arc<dyn CompactionService>,
    push_ingest_svc: Arc<PushIngestServiceImpl>,
}

impl TransformTestHarness {
    pub fn new_custom<
        TAuthorizer: auth::DatasetActionAuthorizer + 'static,
        TEngineProvisioner: EngineProvisioner + 'static,
    >(
        dataset_action_authorizer: TAuthorizer,
        engine_provisioner: TEngineProvisioner,
    ) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        let run_info_dir = tempdir.path().join("run");
        std::fs::create_dir(&datasets_dir).unwrap();
        std::fs::create_dir(&run_info_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add_value(RunInfoDir::new(run_info_dir))
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(CurrentAccountSubject::new_test())
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(dataset_action_authorizer)
            .bind::<dyn auth::DatasetActionAuthorizer, TAuthorizer>()
            .add::<SystemTimeSourceDefault>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<CompactionServiceImpl>()
            .add::<DataFormatRegistryImpl>()
            .add::<PushIngestServiceImpl>()
            .bind::<dyn PushIngestService, PushIngestServiceImpl>()
            .add_value(engine_provisioner)
            .bind::<dyn EngineProvisioner, TEngineProvisioner>()
            .add::<TransformServiceImpl>()
            .add::<VerificationServiceImpl>()
            .build();

        Self {
            _tempdir: tempdir,
            dataset_repo: catalog.get_one().unwrap(),
            transform_service: catalog.get_one().unwrap(),
            compaction_service: catalog.get_one().unwrap(),
            push_ingest_svc: catalog.get_one().unwrap(),
        }
    }

    pub fn new() -> Self {
        Self::new_custom(
            auth::AlwaysHappyDatasetActionAuthorizer::new(),
            EngineProvisionerNull,
        )
    }

    pub async fn new_root(&self, name: &str) -> DatasetHandle {
        let snap = MetadataFactory::dataset_snapshot()
            .name(name)
            .kind(DatasetKind::Root)
            .push_event(MetadataFactory::set_data_schema().build())
            .build();

        let create_result = self
            .dataset_repo
            .create_dataset_from_snapshot(snap)
            .await
            .unwrap();
        create_result.dataset_handle
    }

    async fn new_deriv(
        &self,
        name: &str,
        inputs: &[DatasetAlias],
    ) -> (DatasetHandle, SetTransform) {
        let transform = MetadataFactory::set_transform()
            .inputs_from_refs(inputs)
            .build();
        let snap = MetadataFactory::dataset_snapshot()
            .name(name)
            .kind(DatasetKind::Derivative)
            .push_event(transform.clone())
            .push_event(MetadataFactory::set_data_schema().build())
            .build();

        let create_result = self
            .dataset_repo
            .create_dataset_from_snapshot(snap)
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
        records: u64,
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
            .map_ok(|e| e.new_data.unwrap().offset_interval.end + 1)
            .try_first()
            .await
            .unwrap()
            .unwrap_or(0);

        let prev_head = chain.resolve_ref(&BlockRef::Head).await.unwrap();
        let prev_block = chain.get_block(&prev_head).await.unwrap();

        let block = MetadataFactory::metadata_block(
            MetadataFactory::add_data()
                .new_offset_interval(offset, offset + records - 1)
                .new_watermark(Some(Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 0).unwrap()))
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

    async fn ingest_data(&self, data_str: String, dataset_ref: &DatasetRef) {
        let data = std::io::Cursor::new(data_str);

        self.push_ingest_svc
            .ingest_from_file_stream(
                dataset_ref,
                None,
                Box::new(data),
                PushIngestOpts::default(),
                None,
            )
            .await
            .unwrap();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_next_operation() {
    let harness = TransformTestHarness::new();

    let foo = harness.new_root("foo").await;
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
    let foo_slice = foo_block.event.new_data.as_ref().unwrap();

    assert!(matches!(
        harness.transform_service.get_next_operation(&bar, Utc::now()).await.unwrap(),
        Some(TransformRequestExt{ transform, inputs, .. })
        if transform == bar_source.transform &&
        inputs == vec![TransformRequestInputExt {
            dataset_handle: foo.clone(),
            alias: foo.alias.dataset_name.to_string(),
            vocab: DatasetVocabulary::default(),
            prev_block_hash: None,
            new_block_hash: Some(foo_head),
            prev_offset: None,
            new_offset: Some(9),
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
        mock_dataset_action_authorizer,
        mock_engine_provisioner::MockEngineProvisioner::new().stub_provision_engine(),
    );

    let foo = harness.new_root("foo").await;
    let (_, _) = harness.append_data_block(&foo.alias, 10).await;

    let (bar, _) = harness.new_deriv("bar", &[foo.alias.clone()]).await;

    let transform_result = harness
        .transform_service
        .transform(&bar.as_local_ref(), TransformOptions::default(), None)
        .await;

    assert_matches!(transform_result, Ok(_));
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transform_unauthorized() {
    let harness = TransformTestHarness::new_custom(
        MockDatasetActionAuthorizer::denying(),
        EngineProvisionerNull,
    );

    let foo = harness.new_root("foo").await;
    let (bar, _) = harness.new_deriv("bar", &[foo.alias.clone()]).await;

    let transform_result = harness
        .transform_service
        .transform(&bar.as_local_ref(), TransformOptions::default(), None)
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

    let root_head_schema = root_create_result
        .dataset
        .commit_event(
            MetadataFactory::set_data_schema().build().into(),
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

    deriv_create_result
        .dataset
        .commit_event(
            MetadataFactory::set_transform()
                .inputs_from_aliases_and_seeded_ids([&root_alias.dataset_name])
                .build()
                .into(),
            CommitOpts {
                system_time: Some(t0),
                ..CommitOpts::default()
            },
        )
        .await
        .unwrap();

    let deriv_head_schema = deriv_create_result
        .dataset
        .commit_event(
            MetadataFactory::set_data_schema().build().into(),
            CommitOpts {
                system_time: Some(t0),
                ..CommitOpts::default()
            },
        )
        .await
        .unwrap()
        .new_head;

    let deriv_hdl = deriv_create_result.dataset_handle;
    let deriv_initial_sequence_number = 2;

    // T1: Root data added
    let t1 = Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap();
    let root_head_t1 = harness
        .append_block(
            &root_hdl,
            MetadataFactory::metadata_block(AddData {
                prev_checkpoint: None,
                prev_offset: None,
                new_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                    offset_interval: OffsetInterval { start: 0, end: 99 },
                    size: 10,
                }),
                new_checkpoint: None,
                new_watermark: Some(t0),
                new_source_state: None,
            })
            .system_time(t1)
            .prev(&root_head_schema, root_initial_sequence_number)
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
            MetadataFactory::metadata_block(ExecuteTransform {
                query_inputs: vec![ExecuteTransformInput {
                    dataset_id: root_hdl.id.clone(),
                    prev_block_hash: None,
                    new_block_hash: Some(root_head_t1.clone()),
                    prev_offset: None,
                    new_offset: Some(99),
                }],
                prev_checkpoint: None,
                prev_offset: None,
                new_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                    offset_interval: OffsetInterval { start: 0, end: 99 },
                    size: 10,
                }),
                new_checkpoint: None,
                new_watermark: Some(t0),
            })
            .system_time(t2)
            .prev(&deriv_head_schema, deriv_initial_sequence_number)
            .build(),
        )
        .await;

    // T3: More root data
    let t3 = Utc.with_ymd_and_hms(2020, 1, 3, 12, 0, 0).unwrap();
    let root_head_t3 = harness
        .append_block(
            &root_hdl,
            MetadataFactory::metadata_block(AddData {
                prev_checkpoint: None,
                prev_offset: Some(99),
                new_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                    offset_interval: OffsetInterval {
                        start: 100,
                        end: 109,
                    },
                    size: 10,
                }),
                new_checkpoint: None,
                new_watermark: Some(t2),
                new_source_state: None,
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

    // T4: Transform (T1; T3]
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
            MetadataFactory::metadata_block(ExecuteTransform {
                query_inputs: vec![ExecuteTransformInput {
                    dataset_id: root_hdl.id.clone(),
                    prev_block_hash: Some(root_head_t1.clone()),
                    new_block_hash: Some(root_head_t3.clone()),
                    prev_offset: Some(99),
                    new_offset: Some(109),
                }],
                prev_checkpoint: None,
                prev_offset: Some(99),
                new_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                    offset_interval: OffsetInterval {
                        start: 100,
                        end: 109,
                    },
                    size: 10,
                }),
                new_checkpoint: None,
                new_watermark: Some(t2),
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
            MetadataFactory::metadata_block(AddData {
                prev_checkpoint: None,
                prev_offset: Some(109),
                new_data: None,
                new_checkpoint: None,
                new_watermark: Some(t4),
                new_source_state: None,
            })
            .system_time(t5)
            .prev(&root_head_t3, root_initial_sequence_number + 2)
            .build(),
        )
        .await;

    // T6: Transform (T3; T5]
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
            MetadataFactory::metadata_block(ExecuteTransform {
                query_inputs: vec![ExecuteTransformInput {
                    dataset_id: root_hdl.id.clone(),
                    prev_block_hash: Some(root_head_t3.clone()),
                    new_block_hash: Some(root_head_t5.clone()),
                    prev_offset: Some(109),
                    new_offset: None,
                }],
                prev_checkpoint: None,
                prev_offset: Some(109),
                new_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                    offset_interval: OffsetInterval {
                        start: 110,
                        end: 119,
                    },
                    size: 10,
                }),
                new_checkpoint: None,
                new_watermark: Some(t4),
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

    assert_requests_equivalent(&plan[0].request, deriv_req_t2);
    assert_requests_equivalent(&plan[1].request, deriv_req_t4);
    assert_requests_equivalent(&plan[2].request, deriv_req_t6);
}

#[test_log::test(tokio::test)]
async fn test_transform_with_compaction_retry() {
    let harness = TransformTestHarness::new_custom(
        auth::AlwaysHappyDatasetActionAuthorizer::new(),
        mock_engine_provisioner::MockEngineProvisioner::new().always_provision_engine(),
    );
    let root_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let foo_created_result = harness
        .dataset_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name(root_alias)
                .kind(DatasetKind::Root)
                .push_event(
                    MetadataFactory::add_push_source()
                        .read(ReadStepCsv {
                            header: Some(true),
                            schema: Some(
                                ["date TIMESTAMP", "city STRING", "population BIGINT"]
                                    .iter()
                                    .map(|s| (*s).to_string())
                                    .collect(),
                            ),
                            ..ReadStepCsv::default()
                        })
                        .merge(MergeStrategyLedger {
                            primary_key: vec!["date".to_string(), "city".to_string()],
                        })
                        .build(),
                )
                .push_event(SetVocab {
                    event_time_column: Some("date".to_string()),
                    ..Default::default()
                })
                .build(),
        )
        .await
        .unwrap();

    let data_str = indoc!(
        "
            date,city,population
            2020-01-01,A,1000
            2020-01-02,B,2000
            2020-01-03,C,3000
            "
    );
    harness
        .ingest_data(
            data_str.to_string(),
            &foo_created_result.dataset_handle.as_local_ref(),
        )
        .await;
    let data_str = indoc!(
        "
            date,city,population
            2020-01-04,A,4000
            2020-01-05,B,5000
            2020-01-06,C,6000
            "
    );
    harness
        .ingest_data(
            data_str.to_string(),
            &foo_created_result.dataset_handle.as_local_ref(),
        )
        .await;

    let (bar, _) = harness
        .new_deriv("bar", &[foo_created_result.dataset_handle.alias.clone()])
        .await;

    let transform_result = harness
        .transform_service
        .transform(&bar.as_local_ref(), TransformOptions::default(), None)
        .await;

    assert_matches!(transform_result, Ok(TransformResult::Updated { .. }));

    harness
        .compaction_service
        .compact_dataset(
            &foo_created_result.dataset_handle,
            CompactionOptions::default(),
            None,
        )
        .await
        .unwrap();

    let transform_result = harness
        .transform_service
        .transform(&bar.as_local_ref(), TransformOptions::default(), None)
        .await;

    assert_matches!(
        transform_result,
        Err(TransformError::InvalidInterval(InvalidIntervalError { .. }))
    );

    let transform_result = harness
        .transform_service
        .transform(
            &bar.as_local_ref(),
            TransformOptions {
                reset_derivatives_on_diverged_input: true,
            },
            None,
        )
        .await;

    assert_matches!(transform_result, Ok(TransformResult::Updated { .. }));
}

/////////////////////////////////////////////////////////////////////////////////////////

fn assert_requests_equivalent(lhs: &TransformRequestExt, mut rhs: TransformRequestExt) {
    // Operation IDs are randomly generated, so ignoring them for this check
    rhs.operation_id = lhs.operation_id.clone();

    assert_eq!(lhs.inputs, rhs.inputs);
    assert_eq!(*lhs, rhs);
}

/////////////////////////////////////////////////////////////////////////////////////////
