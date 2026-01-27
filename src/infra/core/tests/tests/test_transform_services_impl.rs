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

use chrono::{DateTime, TimeZone, Utc};
use futures::TryStreamExt;
use indoc::indoc;
use kamu::domain::engine::*;
use kamu::domain::*;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_accounts_inmem::{InMemoryAccountQuotaEventStore, InMemoryAccountRepository};
use kamu_accounts_services::{AccountQuotaServiceImpl, AccountServiceImpl};
use kamu_datasets::{
    AlwaysHappyDatasetActionAuthorizer,
    DatasetRegistry,
    DatasetRegistryExt,
    ResolvedDataset,
};
use kamu_datasets_inmem::InMemoryDatasetStatisticsRepository;
use kamu_datasets_services::{
    AccountQuotaCheckerStorageImpl,
    DatasetStatisticsServiceImpl,
    QuotaDefaultsConfig,
};
use messaging_outbox::DummyOutboxImpl;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::testing::MetadataFactory;
use tempfile::TempDir;
use time_source::{SystemTimeSource, SystemTimeSourceDefault};

use crate::mock_engine_provisioner;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TransformTestHarness {
    _tempdir: TempDir,
    system_time_source: Arc<dyn SystemTimeSource>,
    did_generator: Arc<dyn DidGenerator>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    transform_request_planner: Arc<dyn TransformRequestPlanner>,
    transform_elab_svc: Arc<dyn TransformElaborationService>,
    transform_executor: Arc<dyn TransformExecutor>,
    compaction_planner: Arc<dyn CompactionPlanner>,
    compaction_executor: Arc<dyn CompactionExecutor>,
    ingest_data_use_case: Arc<dyn PushIngestDataUseCase>,
}

impl TransformTestHarness {
    pub fn new_custom<TEngineProvisioner: EngineProvisioner + 'static>(
        engine_provisioner: TEngineProvisioner,
    ) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        let run_info_dir = tempdir.path().join("run");
        std::fs::create_dir(&datasets_dir).unwrap();
        std::fs::create_dir(&run_info_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<DidGeneratorDefault>()
            .add_value(RunInfoDir::new(run_info_dir))
            .add_value(CurrentAccountSubject::new_test())
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                datasets_dir,
            ))
            .add::<DatasetRegistrySoloUnitBridge>()
            .add::<odf::dataset::DatasetLfsBuilderDefault>()
            .add::<AlwaysHappyDatasetActionAuthorizer>()
            .add::<SystemTimeSourceDefault>()
            .add::<AccountServiceImpl>()
            .add::<InMemoryAccountRepository>()
            .add::<InMemoryAccountQuotaEventStore>()
            .add::<AccountQuotaServiceImpl>()
            .add::<InMemoryDatasetStatisticsRepository>()
            .add::<DatasetStatisticsServiceImpl>()
            .add_value(QuotaDefaultsConfig::default())
            .add::<AccountQuotaCheckerStorageImpl>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add_value(EngineConfigDatafusionEmbeddedCompaction::default())
            .add::<CompactionPlannerImpl>()
            .add::<CompactionExecutorImpl>()
            .add::<DataFormatRegistryImpl>()
            .add_value(EngineConfigDatafusionEmbeddedIngest::default())
            .add::<PushIngestExecutorImpl>()
            .add::<PushIngestPlannerImpl>()
            .add::<PushIngestDataUseCaseImpl>()
            .add::<DummyOutboxImpl>()
            .add_value(engine_provisioner)
            .bind::<dyn EngineProvisioner, TEngineProvisioner>()
            .add::<TransformRequestPlannerImpl>()
            .add::<TransformElaborationServiceImpl>()
            .add::<TransformExecutorImpl>()
            .add::<VerificationServiceImpl>()
            .build();

        Self {
            _tempdir: tempdir,
            system_time_source: catalog.get_one().unwrap(),
            did_generator: catalog.get_one().unwrap(),
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
            compaction_planner: catalog.get_one().unwrap(),
            compaction_executor: catalog.get_one().unwrap(),
            ingest_data_use_case: catalog.get_one().unwrap(),
            transform_request_planner: catalog.get_one().unwrap(),
            transform_elab_svc: catalog.get_one().unwrap(),
            transform_executor: catalog.get_one().unwrap(),
        }
    }

    pub fn new() -> Self {
        Self::new_custom(EngineProvisionerNull)
    }

    pub async fn new_root(&self, name: &str, system_time: DateTime<Utc>) -> ResolvedDataset {
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(name)
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::set_data_schema().build())
            .build();

        let alias = snapshot.name.clone();

        let stored = create_test_dataset_from_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            snapshot,
            self.did_generator.generate_dataset_id().0,
            system_time,
        )
        .await
        .unwrap();

        ResolvedDataset::from_stored(&stored, &alias)
    }

    async fn new_deriv(
        &self,
        name: &str,
        inputs: &[odf::DatasetAlias],
        system_time: DateTime<Utc>,
    ) -> (ResolvedDataset, odf::metadata::SetTransform) {
        let transform = MetadataFactory::set_transform()
            .inputs_from_refs(inputs)
            .build();
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(name)
            .kind(odf::DatasetKind::Derivative)
            .push_event(transform.clone())
            .push_event(MetadataFactory::set_data_schema().build())
            .build();

        let dataset_alias = snapshot.name.clone();

        let stored = create_test_dataset_from_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            snapshot,
            self.did_generator.generate_dataset_id().0,
            system_time,
        )
        .await
        .unwrap();

        let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

        (target, transform)
    }

    pub async fn append_block(
        &self,
        dataset_ref: impl Into<odf::DatasetRef>,
        block: odf::MetadataBlock,
    ) -> odf::Multihash {
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_ref(&dataset_ref.into())
            .await
            .unwrap();
        resolved_dataset
            .as_metadata_chain()
            .append(block, odf::dataset::AppendOpts::default())
            .await
            .unwrap()
    }

    // TODO: Simplify using writer
    pub async fn append_data_block(
        &self,
        target: ResolvedDataset,
        records: u64,
    ) -> (
        odf::Multihash,
        odf::MetadataBlockTyped<odf::metadata::AddData>,
    ) {
        use odf::dataset::{MetadataChainExt, TryStreamExtExt};
        use odf::metadata::{AsTypedBlock, EnumWithVariants};

        let chain = target.as_metadata_chain();
        let offset = chain
            .iter_blocks()
            .filter_map_ok(|(_, b)| b.event.into_variant::<odf::metadata::AddData>())
            .map_ok(|e| e.new_data.unwrap().offset_interval.end + 1)
            .try_first()
            .await
            .unwrap()
            .unwrap_or(0);

        let prev_head = chain.resolve_ref(&odf::BlockRef::Head).await.unwrap();
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
            .append(block.clone(), odf::dataset::AppendOpts::default())
            .await
            .unwrap();
        (
            block_hash,
            block.into_typed::<odf::metadata::AddData>().unwrap(),
        )
    }

    async fn ingest_data(&self, data_str: String, target: ResolvedDataset) {
        let data = std::io::Cursor::new(data_str);

        self.ingest_data_use_case
            .execute(
                target,
                DataSource::Stream(Box::new(data)),
                PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: None,
                    expected_head: None,
                },
                None,
            )
            .await
            .unwrap();
    }

    async fn evaluate_transform_status(&self, target: ResolvedDataset) -> TransformStatus {
        self.transform_request_planner
            .evaluate_transform_status(target)
            .await
            .unwrap()
    }

    async fn elaborate_transform(
        &self,
        target: ResolvedDataset,
        options: TransformOptions,
    ) -> Result<TransformElaboration, TransformElaborateError> {
        self.transform_elab_svc
            .elaborate_transform(
                target.clone(),
                self.transform_request_planner
                    .build_transform_preliminary_plan(target)
                    .await
                    .unwrap(),
                options,
                None,
            )
            .await
    }

    async fn transform(
        &self,
        target: ResolvedDataset,
        options: TransformOptions,
    ) -> Result<TransformResult, TransformError> {
        let elaboration = self
            .elaborate_transform(target.clone(), options)
            .await
            .map_err(TransformError::Elaborate)?;

        match elaboration {
            TransformElaboration::UpToDate => Ok(TransformResult::UpToDate),
            TransformElaboration::Elaborated(plan) => {
                let transform_result = self
                    .transform_executor
                    .execute_transform(target.clone(), plan, None)
                    .await
                    .1
                    .map_err(TransformError::Execute)?;

                if let TransformResult::Updated { old_head, new_head } = &transform_result {
                    target
                        .as_metadata_chain()
                        .set_ref(
                            &odf::BlockRef::Head,
                            new_head,
                            odf::dataset::SetRefOpts {
                                validate_block_present: true,
                                check_ref_is: Some(Some(old_head)),
                            },
                        )
                        .await
                        .unwrap();
                }

                Ok(transform_result)
            }
        }
    }

    async fn compact(&self, target: ResolvedDataset) {
        let compaction_plan = self
            .compaction_planner
            .plan_compaction(target.clone(), CompactionOptions::default(), None)
            .await
            .unwrap();

        let compaction_result = self
            .compaction_executor
            .execute(target.clone(), compaction_plan, None)
            .await
            .unwrap();

        if let CompactionResult::Success {
            old_head,
            new_head: proposed_new_head,
            ..
        } = &compaction_result
        {
            target
                .as_metadata_chain()
                .set_ref(
                    &odf::BlockRef::Head,
                    proposed_new_head,
                    odf::dataset::SetRefOpts {
                        validate_block_present: true,
                        check_ref_is: Some(Some(old_head)),
                    },
                )
                .await
                .unwrap();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_next_operation() {
    let harness = TransformTestHarness::new();

    let foo_target = harness
        .new_root("foo", harness.system_time_source.now())
        .await;
    let (bar_target, bar_source) = harness
        .new_deriv(
            "bar",
            &[foo_target.get_alias().clone()],
            harness.system_time_source.now(),
        )
        .await;

    // No data - no work
    let elaboration = harness
        .elaborate_transform(bar_target.clone(), TransformOptions::default())
        .await
        .unwrap();
    assert_matches!(elaboration, TransformElaboration::UpToDate);

    let (foo_head, foo_block) = harness.append_data_block(foo_target.clone(), 10).await;
    let foo_slice = foo_block.event.new_data.as_ref().unwrap();

    let elaboration = harness
        .elaborate_transform(bar_target, TransformOptions::default())
        .await
        .unwrap();
    assert!(matches!(
        elaboration,
        TransformElaboration::Elaborated(TransformPlan { request: TransformRequestExt{ transform, inputs, .. }, datasets_map: _ } )
        if transform == bar_source.transform &&
        inputs == vec![TransformRequestInputExt {
            dataset_handle: foo_target.get_handle().clone(),
            alias: foo_target.get_alias().dataset_name.to_string(),
            vocab: odf::metadata::DatasetVocabulary::default(),
            prev_block_hash: None,
            new_block_hash: Some(foo_head),
            prev_offset: None,
            new_offset: Some(9),
            data_slices: vec![foo_slice.physical_hash.clone()],
            schema: MetadataFactory::set_data_schema().build().schema_as_arrow(&odf::metadata::ToArrowSettings::default()).map(Arc::new).unwrap(),
            explicit_watermarks: vec![odf::metadata::Watermark {
                system_time: foo_block.system_time,
                event_time: Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 0).unwrap(),
            }],
        }]
    ));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_verification_plan_one_to_one() {
    let harness = TransformTestHarness::new();

    // Create root dataset
    let t0 = Utc.with_ymd_and_hms(2020, 1, 1, 11, 0, 0).unwrap();
    let root_target = harness.new_root("foo", t0).await;
    let root_head_schema = root_target
        .as_metadata_chain()
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .unwrap();

    let root_initial_sequence_number = 1;

    // Create derivative
    let (deriv_target, _) = harness
        .new_deriv("bar", &[root_target.get_alias().clone()], t0)
        .await;

    let deriv_head_schema = deriv_target
        .as_metadata_chain()
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .unwrap();

    let deriv_initial_sequence_number = 2;

    // T1: Root data added
    let t1 = Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap();
    let root_head_t1 = harness
        .append_block(
            root_target.get_handle(),
            MetadataFactory::metadata_block(odf::metadata::AddData {
                prev_checkpoint: None,
                prev_offset: None,
                new_data: Some(odf::DataSlice {
                    logical_hash: odf::Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: odf::Multihash::from_digest_sha3_256(b"bar"),
                    offset_interval: odf::metadata::OffsetInterval { start: 0, end: 99 },
                    size: 10,
                }),
                new_checkpoint: None,
                new_watermark: Some(t0),
                new_source_state: None,
                extra: None,
            })
            .system_time(t1)
            .prev(&root_head_schema, root_initial_sequence_number)
            .build(),
        )
        .await;

    let root_head_t1_path = odf::utils::data::local_url::into_local_path(
        root_target
            .as_data_repo()
            .get_internal_url(&root_head_t1)
            .await,
    )
    .unwrap();
    std::fs::write(root_head_t1_path, "<data>").unwrap();

    // T2: Transform [SEED; T1]
    let t2 = Utc.with_ymd_and_hms(2020, 1, 2, 12, 0, 0).unwrap();
    let deriv_req_t2 = match harness
        .elaborate_transform(deriv_target.clone(), TransformOptions::default())
        .await
        .unwrap()
    {
        TransformElaboration::Elaborated(plan) => TransformRequestExt {
            system_time: t2,
            ..plan.request
        },
        TransformElaboration::UpToDate => panic!("Unexpected transform elab status"),
    };

    let deriv_head_t2 = harness
        .append_block(
            deriv_target.get_handle(),
            MetadataFactory::metadata_block(odf::metadata::ExecuteTransform {
                query_inputs: vec![odf::metadata::ExecuteTransformInput {
                    dataset_id: root_target.get_id().clone(),
                    prev_block_hash: None,
                    new_block_hash: Some(root_head_t1.clone()),
                    prev_offset: None,
                    new_offset: Some(99),
                }],
                prev_checkpoint: None,
                prev_offset: None,
                new_data: Some(odf::DataSlice {
                    logical_hash: odf::Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: odf::Multihash::from_digest_sha3_256(b"bar"),
                    offset_interval: odf::metadata::OffsetInterval { start: 0, end: 99 },
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
            root_target.get_handle(),
            MetadataFactory::metadata_block(odf::metadata::AddData {
                prev_checkpoint: None,
                prev_offset: Some(99),
                new_data: Some(odf::DataSlice {
                    logical_hash: odf::Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: odf::Multihash::from_digest_sha3_256(b"bar"),
                    offset_interval: odf::metadata::OffsetInterval {
                        start: 100,
                        end: 109,
                    },
                    size: 10,
                }),
                new_checkpoint: None,
                new_watermark: Some(t2),
                new_source_state: None,
                extra: None,
            })
            .system_time(t3)
            .prev(&root_head_t1, root_initial_sequence_number + 1)
            .build(),
        )
        .await;
    let root_head_t3_path = odf::utils::data::local_url::into_local_path(
        root_target
            .as_data_repo()
            .get_internal_url(&root_head_t3)
            .await,
    )
    .unwrap();
    std::fs::write(root_head_t3_path, "<data>").unwrap();

    // T4: Transform (T1; T3]
    let t4 = Utc.with_ymd_and_hms(2020, 1, 4, 12, 0, 0).unwrap();
    let deriv_req_t4 = match harness
        .elaborate_transform(deriv_target.clone(), TransformOptions::default())
        .await
        .unwrap()
    {
        TransformElaboration::Elaborated(plan) => TransformRequestExt {
            system_time: t4,
            ..plan.request
        },
        TransformElaboration::UpToDate => panic!("Unexpected transform elab status"),
    };
    let deriv_head_t4 = harness
        .append_block(
            deriv_target.get_handle(),
            MetadataFactory::metadata_block(odf::metadata::ExecuteTransform {
                query_inputs: vec![odf::metadata::ExecuteTransformInput {
                    dataset_id: root_target.get_id().clone(),
                    prev_block_hash: Some(root_head_t1.clone()),
                    new_block_hash: Some(root_head_t3.clone()),
                    prev_offset: Some(99),
                    new_offset: Some(109),
                }],
                prev_checkpoint: None,
                prev_offset: Some(99),
                new_data: Some(odf::DataSlice {
                    logical_hash: odf::Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: odf::Multihash::from_digest_sha3_256(b"bar"),
                    offset_interval: odf::metadata::OffsetInterval {
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
            root_target.get_handle(),
            MetadataFactory::metadata_block(odf::metadata::AddData {
                prev_checkpoint: None,
                prev_offset: Some(109),
                new_data: None,
                new_checkpoint: None,
                new_watermark: Some(t4),
                new_source_state: None,
                extra: None,
            })
            .system_time(t5)
            .prev(&root_head_t3, root_initial_sequence_number + 2)
            .build(),
        )
        .await;

    // T6: Transform (T3; T5]
    let t6 = Utc.with_ymd_and_hms(2020, 1, 6, 12, 0, 0).unwrap();
    let deriv_req_t6 = match harness
        .elaborate_transform(deriv_target.clone(), TransformOptions::default())
        .await
        .unwrap()
    {
        TransformElaboration::Elaborated(plan) => TransformRequestExt {
            system_time: t6,
            ..plan.request
        },
        TransformElaboration::UpToDate => panic!("Unexpected transform elab status"),
    };
    let deriv_head_t6 = harness
        .append_block(
            deriv_target.get_handle(),
            MetadataFactory::metadata_block(odf::metadata::ExecuteTransform {
                query_inputs: vec![odf::metadata::ExecuteTransformInput {
                    dataset_id: root_target.get_id().clone(),
                    prev_block_hash: Some(root_head_t3.clone()),
                    new_block_hash: Some(root_head_t5.clone()),
                    prev_offset: Some(109),
                    new_offset: None,
                }],
                prev_checkpoint: None,
                prev_offset: Some(109),
                new_data: Some(odf::DataSlice {
                    logical_hash: odf::Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: odf::Multihash::from_digest_sha3_256(b"bar"),
                    offset_interval: odf::metadata::OffsetInterval {
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

    let operation: VerifyTransformOperation = harness
        .transform_request_planner
        .build_transform_verification_plan(deriv_target.clone(), (None, None))
        .await
        .unwrap();

    let deriv_chain = deriv_target.as_metadata_chain();

    assert_eq!(operation.steps.len(), 3);

    assert_eq!(operation.steps[0].expected_hash, deriv_head_t2);
    assert_eq!(
        operation.steps[0].expected_block,
        deriv_chain.get_block(&deriv_head_t2).await.unwrap()
    );

    assert_eq!(operation.steps[1].expected_hash, deriv_head_t4);
    assert_eq!(
        operation.steps[1].expected_block,
        deriv_chain.get_block(&deriv_head_t4).await.unwrap()
    );

    assert_eq!(operation.steps[2].expected_hash, deriv_head_t6);
    assert_eq!(
        operation.steps[2].expected_block,
        deriv_chain.get_block(&deriv_head_t6).await.unwrap()
    );

    assert_requests_equivalent(&operation.steps[0].request, deriv_req_t2);
    assert_requests_equivalent(&operation.steps[1].request, deriv_req_t4);
    assert_requests_equivalent(&operation.steps[2].request, deriv_req_t6);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transform_with_compaction_retry() {
    let harness = TransformTestHarness::new_custom(
        mock_engine_provisioner::MockEngineProvisioner::new().always_provision_engine(),
    );
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let foo_stored = create_test_dataset_from_snapshot(
        harness.dataset_registry.as_ref(),
        harness.dataset_storage_unit_writer.as_ref(),
        MetadataFactory::dataset_snapshot()
            .name(foo_alias.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(
                MetadataFactory::add_push_source()
                    .read(odf::metadata::ReadStepCsv {
                        header: Some(true),
                        schema: Some(
                            ["date TIMESTAMP", "city STRING", "population BIGINT"]
                                .iter()
                                .map(|s| (*s).to_string())
                                .collect(),
                        ),
                        ..odf::metadata::ReadStepCsv::default()
                    })
                    .merge(odf::metadata::MergeStrategyLedger {
                        primary_key: vec!["date".to_string(), "city".to_string()],
                    })
                    .build(),
            )
            .push_event(odf::metadata::SetVocab {
                event_time_column: Some("date".to_string()),
                ..Default::default()
            })
            .build(),
        harness.did_generator.generate_dataset_id().0,
        harness.system_time_source.now(),
    )
    .await
    .unwrap();

    let foo_target = ResolvedDataset::from_stored(&foo_stored, &foo_alias);

    let data_str = indoc!(
        "
            date,city,population
            2020-01-01,A,1000
            2020-01-02,B,2000
            2020-01-03,C,3000
            "
    );
    harness
        .ingest_data(data_str.to_string(), foo_target.clone())
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
        .ingest_data(data_str.to_string(), foo_target.clone())
        .await;

    let (bar_target, _) = harness
        .new_deriv(
            "bar",
            std::slice::from_ref(&foo_alias),
            harness.system_time_source.now(),
        )
        .await;

    let transform_result = harness
        .transform(bar_target.clone(), TransformOptions::default())
        .await;
    assert_matches!(transform_result, Ok(TransformResult::Updated { .. }));

    harness.compact(foo_target).await;

    let transform_result = harness
        .transform(bar_target.clone(), TransformOptions::default())
        .await;

    assert_matches!(
        transform_result,
        Err(TransformError::Elaborate(
            TransformElaborateError::InvalidInputInterval(InvalidInputIntervalError { .. })
        ))
    );

    let transform_result = harness
        .transform(
            bar_target,
            TransformOptions {
                reset_derivatives_on_diverged_input: true,
            },
        )
        .await;
    assert_matches!(transform_result, Ok(TransformResult::Updated { .. }));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transform_status() {
    let harness = TransformTestHarness::new();

    // Create root dataset
    let t0 = Utc.with_ymd_and_hms(2020, 1, 1, 11, 0, 0).unwrap();
    let root_target = harness.new_root("foo", t0).await;
    let root_head_schema = root_target
        .as_metadata_chain()
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .unwrap();

    let root_initial_sequence_number = 1;

    // Create derivative
    let (deriv_target, _) = harness
        .new_deriv("bar", &[root_target.get_alias().clone()], t0)
        .await;

    let deriv_head_schema = deriv_target
        .as_metadata_chain()
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .unwrap();
    let deriv_initial_sequence_number = 2;

    // Currently there is no data in root, so we have nothing to transform
    let status = harness
        .evaluate_transform_status(deriv_target.clone())
        .await;
    assert_matches!(status, TransformStatus::UpToDate);

    // T1: Root data added
    let t1 = Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap();
    let root_head_t1 = harness
        .append_block(
            root_target.get_handle(),
            MetadataFactory::metadata_block(odf::metadata::AddData {
                prev_checkpoint: None,
                prev_offset: None,
                new_data: Some(odf::DataSlice {
                    logical_hash: odf::Multihash::from_digest_sha3_256(b"new-logical"),
                    physical_hash: odf::Multihash::from_digest_sha3_256(b"new-physical"),
                    offset_interval: odf::metadata::OffsetInterval { start: 0, end: 99 },
                    size: 10,
                }),
                new_checkpoint: None,
                new_watermark: Some(t0),
                new_source_state: None,
                extra: None,
            })
            .system_time(t1)
            .prev(&root_head_schema, root_initial_sequence_number)
            .build(),
        )
        .await;

    let root_head_t1_path = odf::utils::data::local_url::into_local_path(
        root_target
            .as_data_repo()
            .get_internal_url(&root_head_t1)
            .await,
    )
    .unwrap();
    std::fs::write(root_head_t1_path, "<data>").unwrap();

    // Evaluate again - should have an advacement
    let status = harness
        .evaluate_transform_status(deriv_target.clone())
        .await;
    assert_matches!(
        status,
        TransformStatus::NewInputDataAvailable { input_advancements }
        if input_advancements.len() == 1 && input_advancements.first().is_some_and(|advancement| {
            advancement.dataset_id == *root_target.get_id() &&
            advancement.prev_offset.is_none() &&
            advancement.new_offset.is_some_and(|offset| offset == 99) &&
            advancement.prev_block_hash.is_none() &&
            advancement.new_block_hash == Some(root_head_t1.clone())
        })
    );

    // T2: Transform [SEED; T1]
    let t2 = Utc.with_ymd_and_hms(2020, 1, 2, 12, 0, 0).unwrap();
    match harness
        .elaborate_transform(deriv_target.clone(), TransformOptions::default())
        .await
        .unwrap()
    {
        TransformElaboration::Elaborated(_) => {}
        TransformElaboration::UpToDate => panic!("Unexpected transform elab status"),
    }
    let deriv_head_t2 = harness
        .append_block(
            deriv_target.get_handle(),
            MetadataFactory::metadata_block(odf::metadata::ExecuteTransform {
                query_inputs: vec![odf::metadata::ExecuteTransformInput {
                    dataset_id: root_target.get_id().clone(),
                    prev_block_hash: None,
                    new_block_hash: Some(root_head_t1.clone()),
                    prev_offset: None,
                    new_offset: Some(99),
                }],
                prev_checkpoint: None,
                prev_offset: None,
                new_data: Some(odf::DataSlice {
                    logical_hash: odf::Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: odf::Multihash::from_digest_sha3_256(b"bar"),
                    offset_interval: odf::metadata::OffsetInterval { start: 0, end: 99 },
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

    // We've just transformed the target, so it should be up to date now
    let status = harness
        .evaluate_transform_status(deriv_target.clone())
        .await;
    assert_matches!(status, TransformStatus::UpToDate);

    // T3: More root data
    let t3 = Utc.with_ymd_and_hms(2020, 1, 3, 12, 0, 0).unwrap();
    let root_head_t3 = harness
        .append_block(
            root_target.get_handle(),
            MetadataFactory::metadata_block(odf::metadata::AddData {
                prev_checkpoint: None,
                prev_offset: Some(99),
                new_data: Some(odf::DataSlice {
                    logical_hash: odf::Multihash::from_digest_sha3_256(b"new-more-logical"),
                    physical_hash: odf::Multihash::from_digest_sha3_256(b"new-more-physical"),
                    offset_interval: odf::metadata::OffsetInterval {
                        start: 100,
                        end: 109,
                    },
                    size: 10,
                }),
                new_checkpoint: None,
                new_watermark: Some(t2),
                new_source_state: None,
                extra: None,
            })
            .system_time(t3)
            .prev(&root_head_t1, root_initial_sequence_number + 1)
            .build(),
        )
        .await;
    let root_head_t3_path = odf::utils::data::local_url::into_local_path(
        root_target
            .as_data_repo()
            .get_internal_url(&root_head_t3)
            .await,
    )
    .unwrap();
    std::fs::write(root_head_t3_path, "<data>").unwrap();

    // Evaluate again - should have an advacement
    let status = harness
        .evaluate_transform_status(deriv_target.clone())
        .await;
    assert_matches!(
        status,
        TransformStatus::NewInputDataAvailable { input_advancements }
        if input_advancements.len() == 1 && input_advancements.first().is_some_and(|advancement| {
            advancement.dataset_id == *root_target.get_id() &&
            advancement.prev_offset.is_some_and(|offset| offset == 99) &&
            advancement.new_offset.is_some_and(|offset| offset == 109) &&
            advancement.prev_block_hash == Some(root_head_t1.clone()) &&
            advancement.new_block_hash == Some(root_head_t3.clone())
        })
    );

    // T4: Transform (T1; T3]
    let t4 = Utc.with_ymd_and_hms(2020, 1, 4, 12, 0, 0).unwrap();
    match harness
        .elaborate_transform(deriv_target.clone(), TransformOptions::default())
        .await
        .unwrap()
    {
        TransformElaboration::Elaborated(_) => {}
        TransformElaboration::UpToDate => panic!("Unexpected transform elab status"),
    }
    let _deriv_head_t4 = harness
        .append_block(
            deriv_target.get_handle(),
            MetadataFactory::metadata_block(odf::metadata::ExecuteTransform {
                query_inputs: vec![odf::metadata::ExecuteTransformInput {
                    dataset_id: root_target.get_id().clone(),
                    prev_block_hash: Some(root_head_t1.clone()),
                    new_block_hash: Some(root_head_t3.clone()),
                    prev_offset: Some(99),
                    new_offset: Some(109),
                }],
                prev_checkpoint: None,
                prev_offset: Some(99),
                new_data: Some(odf::DataSlice {
                    logical_hash: odf::Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: odf::Multihash::from_digest_sha3_256(b"bar"),
                    offset_interval: odf::metadata::OffsetInterval {
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

    // We've just transformed the target, so it should be up to date now
    let status = harness
        .evaluate_transform_status(deriv_target.clone())
        .await;
    assert_matches!(status, TransformStatus::UpToDate);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_requests_equivalent(lhs: &TransformRequestExt, mut rhs: TransformRequestExt) {
    // Operation IDs are randomly generated, so ignoring them for this check
    rhs.operation_id.clone_from(&lhs.operation_id);

    assert_eq!(lhs.inputs, rhs.inputs);
    assert_eq!(*lhs, rhs);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
