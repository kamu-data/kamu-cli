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

use chrono::{NaiveDate, TimeDelta, TimeZone, Utc};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use dill::Component;
use domain::compact_service::{CompactError, CompactService, NullCompactionMultiListener};
use event_bus::EventBus;
use futures::TryStreamExt;
use indoc::{formatdoc, indoc};
use kamu::domain::*;
use kamu::testing::{DatasetDataHelper, MetadataFactory};
use kamu::*;
use kamu_core::{auth, CurrentAccountSubject};
use opendatafabric::*;

const MAX_SLICE_SIZE: u64 = 1024 * 1024 * 1024;
const MAX_SLICE_RECORDS: u64 = 10000;

#[test_group::group(engine, ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_compact() {
    let harness = CompactTestHarness::new();

    let root_dataset_name = DatasetName::new_unchecked("foo");
    let root_dataset_alias = DatasetAlias::new(None, root_dataset_name.clone());

    harness
        .create_dataset(
            MetadataFactory::dataset_snapshot()
                .name(root_dataset_name.as_str())
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
        .await;

    let data_helper = harness.dataset_data_helper(&root_dataset_alias).await;

    let dataset_handle = harness
        .dataset_repo
        .resolve_dataset_ref(&root_dataset_alias.as_local_ref())
        .await
        .unwrap();

    let data_str = indoc!(
        "
        date,city,population
        2020-01-01,A,1000
        2020-01-01,B,2000
        2020-01-01,C,3000
        "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
        .await;

    let old_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

    assert_matches!(
        harness
            .compact_svc
            .compact_dataset(
                &dataset_handle,
                MAX_SLICE_SIZE,
                MAX_SLICE_RECORDS,
                Some(Arc::new(NullCompactionMultiListener {}))
            )
            .await,
        Ok(()),
    );

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+------+------------+
                | offset | op | system_time          | date                 | city | population |
                +--------+----+----------------------+----------------------+------+------------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A    | 1000       |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | B    | 2000       |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | C    | 3000       |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    let new_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;
    assert_eq!(old_blocks.len(), new_blocks.len());

    let data_str = indoc!(
        "
        date,city,population
        2020-01-02,A,4000
        2020-01-02,B,5000
        2020-01-02,C,6000
        "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
        .await;

    let old_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

    assert_matches!(
        harness
            .compact_svc
            .compact_dataset(
                &dataset_handle,
                MAX_SLICE_SIZE,
                MAX_SLICE_RECORDS,
                Some(Arc::new(NullCompactionMultiListener {}))
            )
            .await,
        Ok(()),
    );

    // 2 Dataslices will be merged in a one slice
    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+------+------------+
                | offset | op | system_time          | date                 | city | population |
                +--------+----+----------------------+----------------------+------+------------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A    | 1000       |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | B    | 2000       |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | C    | 3000       |
                | 3      | 0  | 2050-01-01T12:00:00Z | 2020-01-02T00:00:00Z | C    | 6000       |
                | 4      | 0  | 2050-01-01T12:00:00Z | 2020-01-02T00:00:00Z | B    | 5000       |
                | 5      | 0  | 2050-01-01T12:00:00Z | 2020-01-02T00:00:00Z | A    | 4000       |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    let new_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

    // We compacted two data slices and blocks into one
    assert_eq!(old_blocks.len() - 1, new_blocks.len());

    let data_str = indoc!(
        "
        date,city,population
        2020-01-03,A,1000
        2020-01-03,B,2000
        2020-01-03,C,3000
        "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
        .await;

    let old_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

    assert_matches!(
        harness
            .compact_svc
            .compact_dataset(
                &dataset_handle,
                1024,
                MAX_SLICE_RECORDS,
                Some(Arc::new(NullCompactionMultiListener {}))
            )
            .await,
        Ok(()),
    );

    // Due to limit we will left last added slice as it is
    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+------+------------+
                | offset | op | system_time          | date                 | city | population |
                +--------+----+----------------------+----------------------+------+------------+
                | 6      | 0  | 2050-01-01T12:00:00Z | 2020-01-03T00:00:00Z | A    | 1000       |
                | 7      | 0  | 2050-01-01T12:00:00Z | 2020-01-03T00:00:00Z | B    | 2000       |
                | 8      | 0  | 2050-01-01T12:00:00Z | 2020-01-03T00:00:00Z | C    | 3000       |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    let new_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

    // Shoud save original amount of blocks
    // such as size is equal to max-slice-size
    assert_eq!(old_blocks.len(), new_blocks.len());

    let derive_dataset_name = DatasetName::new_unchecked("derive-foo");
    let derive_dataset_alias = DatasetAlias::new(None, derive_dataset_name.clone());

    harness
        .create_dataset(
            MetadataFactory::dataset_snapshot()
                .name(derive_dataset_name.as_str())
                .kind(DatasetKind::Derivative)
                .push_event(MetadataFactory::set_data_schema().build())
                .build(),
        )
        .await;

    let dataset_handle = harness
        .dataset_repo
        .resolve_dataset_ref(&derive_dataset_alias.as_local_ref())
        .await
        .unwrap();

    assert_matches!(
        harness
            .compact_svc
            .compact_dataset(
                &dataset_handle,
                MAX_SLICE_SIZE,
                MAX_SLICE_RECORDS,
                Some(Arc::new(NullCompactionMultiListener {}))
            )
            .await,
        Err(CompactError::InvalidDatasetKind(_)),
    );
}

#[test_group::group(engine, ingest, datafusion, compact)]
#[tokio::test]
async fn test_large_dataset_compact() {
    let harness = CompactTestHarness::new();

    let root_dataset_name = DatasetName::new_unchecked("foo");
    let root_dataset_alias = DatasetAlias::new(None, root_dataset_name.clone());

    harness
        .create_dataset(
            MetadataFactory::dataset_snapshot()
                .name(root_dataset_name.as_str())
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
        .await;

    harness
        .ingest_multiple_blocks(&root_dataset_alias.as_local_ref(), 100)
        .await;

    let data_helper = harness.dataset_data_helper(&root_dataset_alias).await;

    let dataset_handle = harness
        .dataset_repo
        .resolve_dataset_ref(&root_dataset_alias.as_local_ref())
        .await
        .unwrap();

    let old_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

    assert_eq!(old_blocks.len(), 104);

    // check the last block data and offsets
    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+------+------------+
                | offset | op | system_time          | date                 | city | population |
                +--------+----+----------------------+----------------------+------+------------+
                | 198    | 0  | 2050-01-01T12:00:00Z | 2033-07-21T00:00:00Z | A    | 1000       |
                | 199    | 0  | 2050-01-01T12:00:00Z | 2033-07-21T00:00:00Z | B    | 2000       |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    assert_matches!(
        harness
            .compact_svc
            .compact_dataset(
                &dataset_handle,
                MAX_SLICE_SIZE,
                10,
                Some(Arc::new(NullCompactionMultiListener {}))
            )
            .await,
        Ok(()),
    );

    let new_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

    // We compacted all data slices and blocks into 20
    assert_eq!(new_blocks.len(), 24);

    // check the last block data and offsets
    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
             message arrow_schema {
               OPTIONAL INT64 offset;
               REQUIRED INT32 op;
               REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
               OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
               OPTIONAL BYTE_ARRAY city (STRING);
               OPTIONAL INT64 population;
             }
             "#
            ),
            indoc!(
                r#"
             +--------+----+----------------------+----------------------+------+------------+
             | offset | op | system_time          | date                 | city | population |
             +--------+----+----------------------+----------------------+------+------------+
             | 196    | 0  | 2050-01-01T12:00:00Z | 2033-04-13T00:00:00Z | B    | 2000       |
             | 197    | 0  | 2050-01-01T12:00:00Z | 2033-04-13T00:00:00Z | A    | 1000       |
             | 194    | 0  | 2050-01-01T12:00:00Z | 2033-01-05T00:00:00Z | A    | 1000       |
             | 195    | 0  | 2050-01-01T12:00:00Z | 2033-01-05T00:00:00Z | B    | 2000       |
             | 190    | 0  | 2050-01-01T12:00:00Z | 2032-06-26T00:00:00Z | A    | 1000       |
             | 191    | 0  | 2050-01-01T12:00:00Z | 2032-06-26T00:00:00Z | B    | 2000       |
             | 192    | 0  | 2050-01-01T12:00:00Z | 2032-09-30T00:00:00Z | B    | 2000       |
             | 193    | 0  | 2050-01-01T12:00:00Z | 2032-09-30T00:00:00Z | A    | 1000       |
             | 198    | 0  | 2050-01-01T12:00:00Z | 2033-07-21T00:00:00Z | A    | 1000       |
             | 199    | 0  | 2050-01-01T12:00:00Z | 2033-07-21T00:00:00Z | B    | 2000       |
             +--------+----+----------------------+----------------------+------+------------+
             "#
            ),
        )
        .await;
}

struct CompactTestHarness {
    dataset_repo: Arc<dyn DatasetRepository>,
    compact_svc: Arc<dyn CompactService>,
    push_ingest_svc: Arc<dyn PushIngestService>,
    ctx: SessionContext,
}

impl CompactTestHarness {
    fn new() -> Self {
        Self::new_local_with_authorizer(kamu_core::auth::AlwaysHappyDatasetActionAuthorizer::new())
    }

    fn new_local_with_authorizer<TDatasetAuthorizer: auth::DatasetActionAuthorizer + 'static>(
        dataset_action_authorizer: TDatasetAuthorizer,
    ) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(dataset_action_authorizer)
            .bind::<dyn auth::DatasetActionAuthorizer, TDatasetAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(temp_dir.path().join("datasets"))
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(SystemTimeSourceStub::new_set(
                Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
            ))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<EngineProvisionerNull>()
            .add_builder(CompactServiceImpl::builder().with_run_info_dir(run_info_dir.clone()))
            .bind::<dyn CompactService, CompactServiceImpl>()
            .add_builder(
                PushIngestServiceImpl::builder()
                    .with_object_store_registry(Arc::new(ObjectStoreRegistryImpl::new(vec![
                        Arc::new(ObjectStoreBuilderLocalFs::new()),
                    ])))
                    .with_data_format_registry(Arc::new(DataFormatRegistryImpl::new()))
                    .with_run_info_dir(run_info_dir),
            )
            .bind::<dyn PushIngestService, PushIngestServiceImpl>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        let compact_svc = catalog.get_one::<dyn CompactService>().unwrap();
        let push_ingest_svc = catalog.get_one::<dyn PushIngestService>().unwrap();

        Self {
            dataset_repo,
            compact_svc,
            push_ingest_svc,
            ctx: SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1)),
        }
    }

    async fn get_dataset_head(&self, dataset_ref: &DatasetRef) -> Multihash {
        let dataset = self.dataset_repo.get_dataset(dataset_ref).await.unwrap();

        dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
            .unwrap()
    }

    async fn get_dataset_blocks(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Vec<(Multihash, MetadataBlock)> {
        let dataset = self.dataset_repo.get_dataset(dataset_ref).await.unwrap();
        let head = self.get_dataset_head(dataset_ref).await;

        dataset
            .as_metadata_chain()
            .iter_blocks_interval(&head, None, false)
            .try_collect()
            .await
            .unwrap()
    }

    async fn create_dataset(&self, dataset_snapshot: DatasetSnapshot) {
        self.dataset_repo
            .create_dataset_from_snapshot(dataset_snapshot)
            .await
            .unwrap();
    }

    async fn dataset_data_helper(&self, dataset_alias: &DatasetAlias) -> DatasetDataHelper {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_alias.as_local_ref())
            .await
            .unwrap();

        DatasetDataHelper::new_with_context(dataset, self.ctx.clone())
    }

    async fn ingest_multiple_blocks(&self, dataset_ref: &DatasetRef, amount: i64) {
        let mut start_date = NaiveDate::parse_from_str("2020-01-01", "%Y-%m-%d").unwrap();

        for i in 0..amount {
            start_date += TimeDelta::try_days(i).unwrap();

            let start_date_str = formatdoc!(
                "
                date,city,population
                {},A,1000
                {},B,2000
                ",
                start_date.to_string(),
                start_date.to_string()
            );
            self.ingest_data(start_date_str, dataset_ref).await;
        }
    }

    async fn ingest_data(&self, data_str: String, dataset_ref: &DatasetRef) {
        let data = std::io::Cursor::new(data_str);

        self.push_ingest_svc
            .ingest_from_file_stream(dataset_ref, None, Box::new(data), None, None)
            .await
            .unwrap();
    }
}
