// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, NaiveDate, TimeDelta, TimeZone, Utc};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use dill::Component;
use domain::compact_service::{
    CompactError,
    CompactResult,
    CompactService,
    NullCompactionMultiListener,
};
use event_bus::EventBus;
use futures::TryStreamExt;
use indoc::{formatdoc, indoc};
use kamu::domain::*;
use kamu::testing::{DatasetDataHelper, MetadataFactory};
use kamu::*;
use kamu_core::{auth, CurrentAccountSubject};
use opendatafabric::*;

use super::test_pull_service_impl::TestTransformService;

const MAX_SLICE_SIZE: u64 = 1024 * 1024 * 1024;
const MAX_SLICE_RECORDS: u64 = 10000;

#[test_group::group(ingest, datafusion, compact)]
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
        2020-01-02,B,2000
        2020-01-03,C,3000
        "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
        .await;

    // seed <- add_push_source <- set_vocab <- set_data_schema <- add_data(3
    // records)
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
        Ok(CompactResult::NothingToDo)
    );
    assert!(
        harness
            .verify_dataset(&root_dataset_alias.as_local_ref())
            .await
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
                | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-02T00:00:00Z | B    | 2000       |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-03T00:00:00Z | C    | 3000       |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    // seed <- add_push_source <- set_vocab <- add_data(3 records)
    let new_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

    let (_, last_old_block) = old_blocks.first().unwrap();
    let (_, last_new_block) = new_blocks.first().unwrap();
    assert!(CompactTestHarness::assert_last_add_data_block_event(
        &last_old_block.event,
        &last_new_block.event
    ));

    let data_str = indoc!(
        "
        date,city,population
        2020-01-04,A,4000
        2020-01-05,B,5000
        2020-01-06,C,6000
        "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
        .await;

    // seed <- add_push_source <- set_vocab <- set_schema <- add_data(3 records) <-
    // add_data(3 records)
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
        Ok(CompactResult::Success {
            new_head,
            old_head,
            new_num_blocks: 5,
            old_num_blocks: 6
        }) if new_head != old_head,
    );
    assert!(
        harness
            .verify_dataset(&root_dataset_alias.as_local_ref())
            .await
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
                | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-02T00:00:00Z | B    | 2000       |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-03T00:00:00Z | C    | 3000       |
                | 3      | 0  | 2050-01-01T12:00:00Z | 2020-01-04T00:00:00Z | A    | 4000       |
                | 4      | 0  | 2050-01-01T12:00:00Z | 2020-01-05T00:00:00Z | B    | 5000       |
                | 5      | 0  | 2050-01-01T12:00:00Z | 2020-01-06T00:00:00Z | C    | 6000       |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    // seed <- add_push_source <- set_vocab <- add_data(6 records)
    let new_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

    // We compacted two data slices and blocks into one
    let (_, last_old_block) = old_blocks.first().unwrap();
    let (_, last_new_block) = new_blocks.first().unwrap();
    assert!(CompactTestHarness::assert_last_add_data_block_event(
        &last_old_block.event,
        &last_new_block.event
    ));
}

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_compact_limits() {
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
            2020-01-02,B,2000
            2020-01-03,C,3000
            "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
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
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
        .await;

    let data_str = indoc!(
        "
            date,city,population
            2020-01-07,A,7000
            2020-01-08,B,8000
            2020-01-09,C,9000
            2020-01-10,D,10000
            2020-01-11,F,11000
            2020-01-12,G,12000
            "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
        .await;

    let data_str = indoc!(
        "
            date,city,population
            2020-01-13,D,13000
            2020-01-14,F,14000
            2020-01-15,G,15000
            "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
        .await;

    let data_str = indoc!(
        "
            date,city,population
            2020-01-16,A,16000
            "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
        .await;

    // seed <- add_push_source <- set_vocab <- set_schema <- add_data(3r) <-
    // add_data(3r) <- add_data(6r) <- add_data(3r) <- add_data(1r)
    let old_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

    assert_matches!(
        harness
            .compact_svc
            .compact_dataset(
                &dataset_handle,
                MAX_SLICE_SIZE,
                6,
                Some(Arc::new(NullCompactionMultiListener {}))
            )
            .await,
        Ok(CompactResult::Success {
            new_head,
            old_head,
            new_num_blocks: 7,
            old_num_blocks: 9
        }) if new_head != old_head,
    );
    assert!(
        harness
            .verify_dataset(&root_dataset_alias.as_local_ref())
            .await
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
                    | 12     | 0  | 2050-01-01T12:00:00Z | 2020-01-13T00:00:00Z | D    | 13000      |
                    | 13     | 0  | 2050-01-01T12:00:00Z | 2020-01-14T00:00:00Z | F    | 14000      |
                    | 14     | 0  | 2050-01-01T12:00:00Z | 2020-01-15T00:00:00Z | G    | 15000      |
                    | 15     | 0  | 2050-01-01T12:00:00Z | 2020-01-16T00:00:00Z | A    | 16000      |
                    +--------+----+----------------------+----------------------+------+------------+
                    "#
                ),
            )
            .await;

    // seed <- add_push_source <- set_vocab <- add_data(6r) <- add_data(6r) <-
    // add_data(4r)
    let new_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

    // Shoud save original amount of blocks
    // such as size is equal to max-slice-size
    let (_, last_old_block) = old_blocks.first().unwrap();
    let (_, last_new_block) = new_blocks.first().unwrap();
    assert!(CompactTestHarness::assert_last_add_data_block_event(
        &last_old_block.event,
        &last_new_block.event
    ));
}

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_compact_keep_all_non_data_blocks() {
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
                2020-01-02,B,2000
                2020-01-03,C,3000
                "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
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
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
        .await;

    let current_head = harness
        .get_dataset_head(&dataset_handle.as_local_ref())
        .await;
    harness
        .commit_set_licence_block(&root_dataset_alias.as_local_ref(), &current_head)
        .await;

    let data_str = indoc!(
        "
        date,city,population
        2020-01-07,A,7000
        2020-01-08,B,8000
        2020-01-09,C,9000
        "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
        .await;

    let data_str = indoc!(
        "
            date,city,population
            2020-01-10,A,10000
            2020-01-11,B,11000
            2020-01-12,C,12000
            "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_alias.as_local_ref())
        .await;

    // seed <- add_push_source <- set_vocab <- set_schema <- add_data(3r) <-
    // add_data(3r) <- set_licence <- add_data(3r) <- add_data(3r)
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
        Ok(CompactResult::Success {
            new_head,
            old_head,
            new_num_blocks: 7,
            old_num_blocks: 9
        }) if new_head != old_head,
    );
    assert!(
        harness
            .verify_dataset(&root_dataset_alias.as_local_ref())
            .await
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
                | 6      | 0  | 2050-01-01T12:00:00Z | 2020-01-07T00:00:00Z | A    | 7000       |
                | 7      | 0  | 2050-01-01T12:00:00Z | 2020-01-08T00:00:00Z | B    | 8000       |
                | 8      | 0  | 2050-01-01T12:00:00Z | 2020-01-09T00:00:00Z | C    | 9000       |
                | 9      | 0  | 2050-01-01T12:00:00Z | 2020-01-10T00:00:00Z | A    | 10000      |
                | 10     | 0  | 2050-01-01T12:00:00Z | 2020-01-11T00:00:00Z | B    | 11000      |
                | 11     | 0  | 2050-01-01T12:00:00Z | 2020-01-12T00:00:00Z | C    | 12000      |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    // seed <- add_push_source <- set_vocab <- add_data(6r) <- set_licence <-
    // add_data(6r)
    let new_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

    // The first two and the last two dataslices were merged
    let (_, last_old_block) = old_blocks.first().unwrap();
    let (_, last_new_block) = new_blocks.first().unwrap();
    assert!(CompactTestHarness::assert_last_add_data_block_event(
        &last_old_block.event,
        &last_new_block.event
    ));
    assert!(CompactTestHarness::assert_new_block_propagates_extra_info(
        &old_blocks,
        &new_blocks
    ));
}

#[test_group::group(compact)]
#[tokio::test]
async fn test_dataset_compact_derive_error() {
    let harness = CompactTestHarness::new();

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

#[test_group::group(ingest, datafusion, compact)]
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
                | 198    | 0  | 2050-01-01T12:00:00Z | 2020-04-09T00:00:00Z | A    | 1000       |
                | 199    | 0  | 2050-01-01T12:00:00Z | 2020-04-10T00:00:00Z | B    | 2000       |
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
        Ok(CompactResult::Success {
            new_head,
            old_head,
            new_num_blocks: 24,
            old_num_blocks: 104
        }) if new_head != old_head,
    );
    assert!(
        harness
            .verify_dataset(&root_dataset_alias.as_local_ref())
            .await
    );

    let new_blocks = harness
        .get_dataset_blocks(&root_dataset_alias.as_local_ref())
        .await;

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
             | 190    | 0  | 2050-01-01T12:00:00Z | 2020-04-05T00:00:00Z | A    | 1000       |
             | 191    | 0  | 2050-01-01T12:00:00Z | 2020-04-06T00:00:00Z | B    | 2000       |
             | 192    | 0  | 2050-01-01T12:00:00Z | 2020-04-06T00:00:00Z | A    | 1000       |
             | 193    | 0  | 2050-01-01T12:00:00Z | 2020-04-07T00:00:00Z | B    | 2000       |
             | 194    | 0  | 2050-01-01T12:00:00Z | 2020-04-07T00:00:00Z | A    | 1000       |
             | 195    | 0  | 2050-01-01T12:00:00Z | 2020-04-08T00:00:00Z | B    | 2000       |
             | 196    | 0  | 2050-01-01T12:00:00Z | 2020-04-08T00:00:00Z | A    | 1000       |
             | 197    | 0  | 2050-01-01T12:00:00Z | 2020-04-09T00:00:00Z | B    | 2000       |
             | 198    | 0  | 2050-01-01T12:00:00Z | 2020-04-09T00:00:00Z | A    | 1000       |
             | 199    | 0  | 2050-01-01T12:00:00Z | 2020-04-10T00:00:00Z | B    | 2000       |
             +--------+----+----------------------+----------------------+------+------------+
            "#
            ),
        )
        .await;

    let (_, last_old_block) = old_blocks.first().unwrap();
    let (_, last_new_block) = new_blocks.first().unwrap();
    assert!(CompactTestHarness::assert_last_add_data_block_event(
        &last_old_block.event,
        &last_new_block.event
    ));
    assert!(CompactTestHarness::assert_new_block_propagates_extra_info(
        &old_blocks,
        &new_blocks
    ));
}

struct CompactTestHarness {
    dataset_repo: Arc<dyn DatasetRepository>,
    compact_svc: Arc<dyn CompactService>,
    push_ingest_svc: Arc<dyn PushIngestService>,
    verification_svc: Arc<dyn VerificationService>,
    current_date_tame: DateTime<Utc>,
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
        let current_date_tame = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();

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
            .add_value(SystemTimeSourceStub::new_set(current_date_tame))
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
            .add_value(TestTransformService::new(Arc::new(Mutex::new(Vec::new()))))
            .bind::<dyn TransformService, TestTransformService>()
            .add::<VerificationServiceImpl>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        let compact_svc = catalog.get_one::<dyn CompactService>().unwrap();
        let push_ingest_svc = catalog.get_one::<dyn PushIngestService>().unwrap();
        let verification_svc = catalog.get_one::<dyn VerificationService>().unwrap();

        Self {
            dataset_repo,
            compact_svc,
            push_ingest_svc,
            verification_svc,
            current_date_tame,
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
        let start_date = NaiveDate::parse_from_str("2020-01-01", "%Y-%m-%d").unwrap();

        for i in 0..amount {
            let a_date = start_date + TimeDelta::try_days(i).unwrap();
            let b_date = start_date + TimeDelta::try_days(i + 1).unwrap();

            let start_date_str = formatdoc!(
                "
                date,city,population
                {},A,1000
                {},B,2000
                ",
                a_date.to_string(),
                b_date.to_string()
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

    async fn commit_set_licence_block(&self, dataset_ref: &DatasetRef, head: &Multihash) {
        let dataset = self.dataset_repo.get_dataset(dataset_ref).await.unwrap();
        let event = SetLicense {
            short_name: "sl1".to_owned(),
            name: "set_license1".to_owned(),
            spdx_id: None,
            website_url: "http://set-license.com".to_owned(),
        };

        dataset
            .commit_event(
                event.into(),
                CommitOpts {
                    block_ref: &BlockRef::Head,
                    system_time: Some(self.current_date_tame),
                    prev_block_hash: Some(Some(head)),
                    check_object_refs: false,
                    update_block_ref: true,
                },
            )
            .await
            .unwrap();
    }

    fn assert_last_add_data_block_event(
        old_block_event: &MetadataEvent,
        new_block_event: &MetadataEvent,
    ) -> bool {
        if let MetadataEvent::AddData(old_add_data_event) = old_block_event
            && let MetadataEvent::AddData(new_add_data_event) = new_block_event
        {
            return old_add_data_event.prev_checkpoint == new_add_data_event.prev_checkpoint
                && old_add_data_event
                    .new_data
                    .as_ref()
                    .unwrap()
                    .offset_interval
                    .end
                    == new_add_data_event
                        .new_data
                        .as_ref()
                        .unwrap()
                        .offset_interval
                        .end
                && old_add_data_event.new_source_state == new_add_data_event.new_source_state
                && old_add_data_event.new_watermark == new_add_data_event.new_watermark;
        }
        false
    }

    fn assert_new_block_propagates_extra_info(
        old_chain: &[(Multihash, MetadataBlock)],
        new_chain: &[(Multihash, MetadataBlock)],
    ) -> bool {
        let mut new_non_add_data_event_index = 0;
        for (_, old_block) in old_chain {
            if let MetadataEvent::AddData(_) = old_block.event {
                continue;
            }
            let mut old_add_data_event_index = 0;
            for (_, new_block) in new_chain {
                if let MetadataEvent::AddData(_) = new_block.event {
                    continue;
                }
                if old_add_data_event_index < new_non_add_data_event_index {
                    old_add_data_event_index += 1;
                    continue;
                }
                if old_block.event != new_block.event {
                    return false;
                }
                break;
            }
            new_non_add_data_event_index += 1;
        }
        true
    }

    async fn verify_dataset(&self, dataset_ref: &DatasetRef) -> bool {
        let result = self
            .verification_svc
            .verify(
                dataset_ref,
                (None, None),
                VerificationOptions::default(),
                None,
            )
            .await;

        result.outcome.is_ok()
    }
}
