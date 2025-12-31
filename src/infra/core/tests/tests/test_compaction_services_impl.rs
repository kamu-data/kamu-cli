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
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use domain::{CompactionError, CompactionOptions, CompactionResult};
use futures::TryStreamExt;
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::DatasetDataHelper;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_accounts_inmem::{InMemoryAccountQuotaEventStore, InMemoryAccountRepository};
use kamu_accounts_services::{AccountQuotaServiceImpl, AccountServiceImpl};
use kamu_core::auth;
use kamu_datasets::*;
use kamu_datasets_inmem::InMemoryDatasetStatisticsRepository;
use kamu_datasets_services::{
    AccountQuotaCheckerStorageImpl,
    DatasetStatisticsServiceImpl,
    QuotaDefaultsConfig,
};
use messaging_outbox::DummyOutboxImpl;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::testing::MetadataFactory;
use s3_utils::S3Context;
use test_utils::LocalS3Server;
use time_source::{SystemTimeSource, SystemTimeSourceStub};

use crate::{TransformTestHelper, mock_engine_provisioner};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_compact() {
    let harness = CompactTestHarness::new();

    let target = harness.create_test_root_dataset().await;
    let dataset_ref = target.get_handle().as_local_ref();

    let data_helper = harness.dataset_data_helper(&dataset_ref).await;

    // Round 1: Compaction is a no-op
    //
    // Before/after: seed <- add_push_source <- set_vocab <- set_schema <-
    // set_data_schema <- add_data(3 records)
    let data_str = indoc!(
        "
        date,city,population
        2020-01-01,A,1000
        2020-01-02,B,2000
        2020-01-03,C,3000
        "
    );

    harness
        .ingest_data(data_str.to_string(), target.clone())
        .await;

    let prev_head = target
        .as_metadata_chain()
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .unwrap();

    assert_matches!(
        harness
            .compact_dataset(target.clone(), CompactionOptions::default())
            .await,
        Ok(CompactionResult::NothingToDo)
    );

    assert_eq!(
        prev_head,
        target
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .unwrap()
    );

    // Round 2: Last blocks are compacted
    //
    // Before: seed <- add_push_source <- set_vocab <- set_schema <- set_data_schema
    // <- add_data(3 records) <- add_data(3 records)
    //
    // After: seed <- add_push_source <- set_vocab <- set_schema <- add_data(6
    // records)
    let data_str = indoc!(
        "
        date,city,population
        2020-01-04,A,4000
        2020-01-05,B,5000
        2020-01-06,C,6000
        "
    );

    harness
        .ingest_data(data_str.to_string(), target.clone())
        .await;

    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    assert_matches!(
        harness
            .compact_dataset(target.clone(), CompactionOptions::default())
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 5,
            old_num_blocks: 6
        }) if new_head != old_head,
    );

    assert!(harness.verify_dataset(target).await);

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
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

    let new_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    let last_old_block = old_blocks.first().unwrap();
    let last_new_block = new_blocks.first().unwrap();
    CompactTestHarness::assert_end_state_equivalent(&last_old_block.event, &last_new_block.event);

    use odf::metadata::EnumWithVariants;
    let new_add_data = last_new_block
        .event
        .as_variant::<odf::metadata::AddData>()
        .unwrap();
    CompactTestHarness::assert_offset_interval_eq(
        new_add_data,
        &odf::metadata::OffsetInterval { start: 0, end: 5 },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_compact_s3() {
    let s3 = LocalS3Server::new().await;
    let harness = CompactTestHarness::new_s3(&s3).await;

    let target = harness.create_test_root_dataset().await;
    let dataset_ref = target.get_handle().as_local_ref();

    // Round 1: Compaction is a no-op
    //
    // Before/after: seed <- add_push_source <- set_vocab <- set_schema <-
    // set_data_schema <- add_data(3 records)
    let data_str = indoc!(
        "
        date,city,population
        2020-01-01,A,1000
        2020-01-02,B,2000
        2020-01-03,C,3000
        "
    );

    harness
        .ingest_data(data_str.to_string(), target.clone())
        .await;

    let prev_head = target
        .as_metadata_chain()
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .unwrap();

    assert_matches!(
        harness
            .compact_dataset(target.clone(), CompactionOptions::default())
            .await,
        Ok(CompactionResult::NothingToDo)
    );

    assert_eq!(
        prev_head,
        target
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .unwrap()
    );

    // Round 2: Last blocks are compacted
    //
    // Before: seed <- add_push_source <- set_vocab <- set_schema <- set_data_schema
    // <- add_data(3 records) <- add_data(3 records)
    //
    // After: seed <- add_push_source <- set_vocab <- set_schema <- add_data(6
    // records)
    let data_str = indoc!(
        "
        date,city,population
        2020-01-04,A,4000
        2020-01-05,B,5000
        2020-01-06,C,6000
        "
    );

    harness
        .ingest_data(data_str.to_string(), target.clone())
        .await;

    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    assert_matches!(
        harness
        .compact_dataset(target.clone(), CompactionOptions::default())
        .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 5,
            old_num_blocks: 6
        }) if new_head != old_head,
    );

    assert!(harness.verify_dataset(target).await);

    let new_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    let last_old_block = old_blocks.first().unwrap();
    let last_new_block = new_blocks.first().unwrap();
    CompactTestHarness::assert_end_state_equivalent(&last_old_block.event, &last_new_block.event);

    use odf::metadata::EnumWithVariants;
    let new_add_data = last_new_block
        .event
        .as_variant::<odf::metadata::AddData>()
        .unwrap();
    CompactTestHarness::assert_offset_interval_eq(
        new_add_data,
        &odf::metadata::OffsetInterval { start: 0, end: 5 },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_compaction_watermark_only_blocks() {
    let harness = CompactTestHarness::new();

    let target = harness.create_test_root_dataset().await;
    let dataset_ref = target.get_handle().as_local_ref();

    let data_helper = harness.dataset_data_helper(&dataset_ref).await;

    // Before: ... <- add_data(3 records) <- add_data(wm1) <- add_data(3 records) <-
    // add_data(wm2, src2)
    let data_str = indoc!(
        "
        date,city,population
        2020-01-01,A,1000
        2020-01-02,B,2000
        2020-01-03,C,3000
        "
    );

    harness
        .ingest_data(data_str.to_string(), target.clone())
        .await;

    target
        .commit_add_data(
            odf::dataset::AddDataParams {
                prev_checkpoint: None,
                prev_offset: Some(2),
                new_offset_interval: None,
                new_linked_objects: None,
                new_watermark: Some(Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap()),
                new_source_state: None,
            },
            None,
            None,
            odf::dataset::CommitOpts {
                system_time: Some(harness.current_date_time),
                ..odf::dataset::CommitOpts::default()
            },
        )
        .await
        .unwrap();

    let data_str = indoc!(
        "
        date,city,population
        2020-01-04,A,4000
        2020-01-05,B,5000
        2020-01-06,C,6000
        "
    );

    harness
        .ingest_data(data_str.to_string(), target.clone())
        .await;

    target
        .commit_add_data(
            odf::dataset::AddDataParams {
                prev_checkpoint: None,
                prev_offset: Some(5),
                new_offset_interval: None,
                new_linked_objects: None,
                new_watermark: Some(Utc.with_ymd_and_hms(2050, 1, 2, 12, 0, 0).unwrap()),
                new_source_state: Some(odf::metadata::SourceState {
                    source_name: "src".to_string(),
                    kind: "odf/etag".to_string(),
                    value: "123".to_string(),
                }),
            },
            None,
            None,
            odf::dataset::CommitOpts {
                system_time: Some(harness.current_date_time),
                ..odf::dataset::CommitOpts::default()
            },
        )
        .await
        .unwrap();

    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    // After: ... <- add_data(6 records, wm2, src2)
    let res = harness
        .compact_dataset(target.clone(), CompactionOptions::default())
        .await
        .unwrap();

    let CompactionResult::Success {
        old_num_blocks,
        new_num_blocks,
        new_head,
        ..
    } = res
    else {
        panic!("Unexpected result: {res:?}")
    };

    assert_eq!(old_num_blocks, 8);
    assert_eq!(new_num_blocks, 5);

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
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

    use odf::metadata::EnumWithVariants;
    let old_last_add_data = old_blocks
        .first()
        .unwrap()
        .event
        .as_variant::<odf::metadata::AddData>()
        .unwrap();

    use odf::metadata::AsTypedBlock;
    let new_add_data = target
        .as_metadata_chain()
        .get_block(&new_head)
        .await
        .unwrap()
        .into_typed::<odf::metadata::AddData>()
        .unwrap()
        .event;

    assert_eq!(new_add_data.prev_offset, None);
    assert_eq!(
        new_add_data.new_data.as_ref().unwrap().offset_interval,
        odf::metadata::OffsetInterval { start: 0, end: 5 }
    );
    assert_eq!(new_add_data.new_checkpoint, None);
    assert_eq!(new_add_data.new_watermark, old_last_add_data.new_watermark);
    assert_eq!(
        new_add_data.new_source_state,
        old_last_add_data.new_source_state
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_compaction_limits() {
    let harness = CompactTestHarness::new();

    let target = harness.create_test_root_dataset().await;
    let dataset_ref = target.get_handle().as_local_ref();

    let data_helper = harness.dataset_data_helper(&dataset_ref).await;
    let data_str = indoc!(
        "
        date,city,population
        2020-01-01,A,1000
        2020-01-02,B,2000
        2020-01-03,C,3000
        "
    );

    harness
        .ingest_data(data_str.to_string(), target.clone())
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
        .ingest_data(data_str.to_string(), target.clone())
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
        .ingest_data(data_str.to_string(), target.clone())
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
        .ingest_data(data_str.to_string(), target.clone())
        .await;

    let data_str = indoc!(
        "
        date,city,population
        2020-01-16,A,16000
        "
    );

    harness
        .ingest_data(data_str.to_string(), target.clone())
        .await;

    // Initial state:
    // seed <- add_push_source <- set_vocab <- set_schema <- add_data(3r) <-
    // add_data(3r) <- add_data(6r) <- add_data(3r) <- add_data(1r)
    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    assert_matches!(
        harness
            .compact_dataset(
                target.clone(),
                CompactionOptions {
                    max_slice_records: Some(6),
                    ..CompactionOptions::default()
                },
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 7,
            old_num_blocks: 9
        }) if new_head != old_head,
    );
    assert!(harness.verify_dataset(target).await);

    data_helper
            .assert_last_data_eq(
                indoc!(
                    r#"
                    message arrow_schema {
                      REQUIRED INT64 offset;
                      REQUIRED INT32 op;
                      REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                      REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
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

    // Expected state:
    // seed <- add_push_source <- set_vocab <- set_schema <- add_data(6r) <-
    // add_data(6r) <- add_data(4r)
    let new_blocks: Vec<_> = harness.get_dataset_blocks(&dataset_ref).await;

    // Shoud save original amount of blocks
    // such as size is equal to max-slice-size
    let last_old_block = old_blocks.first().unwrap();
    let last_new_block = new_blocks.first().unwrap();
    CompactTestHarness::assert_end_state_equivalent(&last_old_block.event, &last_new_block.event);

    // Validate offsets
    use odf::metadata::EnumWithVariants;
    let new_data_events: Vec<_> = harness
        .get_dataset_blocks(&dataset_ref)
        .await
        .into_iter()
        .filter_map(|b| b.event.into_variant::<odf::metadata::AddData>())
        .rev()
        .collect();

    assert_eq!(new_data_events.len(), 3);

    CompactTestHarness::assert_offset_interval_eq(
        &new_data_events[0],
        &odf::metadata::OffsetInterval { start: 0, end: 5 },
    );
    CompactTestHarness::assert_offset_interval_eq(
        &new_data_events[1],
        &odf::metadata::OffsetInterval { start: 6, end: 11 },
    );
    CompactTestHarness::assert_offset_interval_eq(
        &new_data_events[2],
        &odf::metadata::OffsetInterval { start: 12, end: 15 },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_compaction_keep_all_non_data_blocks() {
    let harness = CompactTestHarness::new();

    let target = harness.create_test_root_dataset().await;
    let dataset_ref = target.get_handle().as_local_ref();

    let data_helper = harness.dataset_data_helper(&dataset_ref).await;
    let data_str = indoc!(
        "
        date,city,population
        2020-01-01,A,1000
        2020-01-02,B,2000
        2020-01-03,C,3000
        "
    );

    harness
        .ingest_data(data_str.to_string(), target.clone())
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
        .ingest_data(data_str.to_string(), target.clone())
        .await;

    let current_head = harness.get_dataset_head(&dataset_ref).await;
    harness
        .commit_set_licence_block(&dataset_ref, &current_head)
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
        .ingest_data(data_str.to_string(), target.clone())
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
        .ingest_data(data_str.to_string(), target.clone())
        .await;

    // seed <- add_push_source <- set_vocab <- set_schema <- add_data(3r) <-
    // add_data(3r) <- set_licence <- add_data(3r) <- add_data(3r)
    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    assert_matches!(
        harness
            .compact_dataset(
                target.clone(),
                CompactionOptions::default(),
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 7,
            old_num_blocks: 9
        }) if new_head != old_head,
    );
    assert!(harness.verify_dataset(target).await);

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
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

    // seed <- add_push_source <- set_vocab <- set_scheme <- add_data(6r) <-
    // set_licence <- add_data(6r)
    let new_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    // The first two and the last two dataslices were merged
    CompactTestHarness::assert_end_state_equivalent(
        &old_blocks.first().unwrap().event,
        &new_blocks.first().unwrap().event,
    );

    use odf::metadata::EnumWithVariants;
    CompactTestHarness::assert_offset_interval_eq(
        new_blocks[2]
            .event
            .as_variant::<odf::metadata::AddData>()
            .unwrap(),
        &odf::metadata::OffsetInterval { start: 0, end: 5 },
    );

    CompactTestHarness::assert_offset_interval_eq(
        new_blocks[0]
            .event
            .as_variant::<odf::metadata::AddData>()
            .unwrap(),
        &odf::metadata::OffsetInterval { start: 6, end: 11 },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(compact)]
#[tokio::test]
async fn test_dataset_compaction_derive_error() {
    let harness = CompactTestHarness::new();

    let stored = harness
        .store_dataset(
            MetadataFactory::dataset_snapshot()
                .name("derive.foo")
                .kind(odf::DatasetKind::Derivative)
                .push_event(MetadataFactory::set_data_schema().build())
                .build(),
        )
        .await;

    let target = ResolvedDataset::from_stored(
        &stored,
        &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("derive.foo")),
    );

    assert_matches!(
        harness
            .compact_dataset(target, CompactionOptions::default(),)
            .await,
        Err(CompactionError::Planning(
            CompactionPlanningError::InvalidDatasetKind(_)
        )),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_large_dataset_compact() {
    let harness = CompactTestHarness::new();

    let target = harness.create_test_root_dataset().await;
    let dataset_ref = target.get_handle().as_local_ref();

    harness.ingest_multiple_blocks(target.clone(), 100, 2).await;

    let data_helper = harness.dataset_data_helper(&dataset_ref).await;

    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    assert_eq!(old_blocks.len(), 104);

    // check the last block data and offsets
    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
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
                | 198    | 0  | 2050-01-01T12:00:00Z | 2010-01-01T03:18:00Z | A    | 198        |
                | 199    | 0  | 2050-01-01T12:00:00Z | 2010-01-01T03:19:00Z | B    | 199        |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    assert_matches!(
        harness
            .compact_dataset(
                target.clone(),
                CompactionOptions {
                    max_slice_records: Some(10),
                    max_slice_size: None,
                    ..CompactionOptions::default()
                },
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 24,
            old_num_blocks: 104
        }) if new_head != old_head,
    );
    assert!(harness.verify_dataset(target).await);

    let new_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    // check the last block data and offsets
    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
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
                | 190    | 0  | 2050-01-01T12:00:00Z | 2010-01-01T03:10:00Z | A    | 190        |
                | 191    | 0  | 2050-01-01T12:00:00Z | 2010-01-01T03:11:00Z | B    | 191        |
                | 192    | 0  | 2050-01-01T12:00:00Z | 2010-01-01T03:12:00Z | A    | 192        |
                | 193    | 0  | 2050-01-01T12:00:00Z | 2010-01-01T03:13:00Z | B    | 193        |
                | 194    | 0  | 2050-01-01T12:00:00Z | 2010-01-01T03:14:00Z | A    | 194        |
                | 195    | 0  | 2050-01-01T12:00:00Z | 2010-01-01T03:15:00Z | B    | 195        |
                | 196    | 0  | 2050-01-01T12:00:00Z | 2010-01-01T03:16:00Z | A    | 196        |
                | 197    | 0  | 2050-01-01T12:00:00Z | 2010-01-01T03:17:00Z | B    | 197        |
                | 198    | 0  | 2050-01-01T12:00:00Z | 2010-01-01T03:18:00Z | A    | 198        |
                | 199    | 0  | 2050-01-01T12:00:00Z | 2010-01-01T03:19:00Z | B    | 199        |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    CompactTestHarness::assert_end_state_equivalent(
        &old_blocks.first().unwrap().event,
        &new_blocks.first().unwrap().event,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[test]
fn test_compact_offsets_are_sequential() {
    // Ensure we run with multiple threads otherwise DataFusion's
    // `target_partitions` setting doesn't matter
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap()
        .block_on(test_compact_offsets_are_sequential_impl());
}

async fn test_compact_offsets_are_sequential_impl() {
    testing_logger::setup();
    let harness = CompactTestHarness::new();

    let target = harness.create_test_root_dataset().await;
    let dataset_ref = target.get_handle().as_local_ref();

    harness
        .ingest_multiple_blocks(target.clone(), 10, 10000)
        .await;

    let data_helper = harness.dataset_data_helper(&dataset_ref).await;

    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    assert_eq!(old_blocks.len(), 14);

    assert_matches!(
        harness
            .compact_dataset(
                target,
                CompactionOptions {
                    max_slice_records: Some(u64::MAX),
                    max_slice_size: Some(u64::MAX),
                    ..CompactionOptions::default()
                },
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 5,
            old_num_blocks: 14
        }) if new_head != old_head,
    );

    testing_logger::validate(|capture| {
        let plan = capture
            .iter()
            .rfind(|c| c.body.contains("Optimized physical plan:"))
            .unwrap()
            .body
            .trim();

        let end = plan.find("...").unwrap();
        let start = plan[0..end].rfind('[').unwrap();
        let plan_clean = plan[0..=start].to_string() + &plan[end..plan.len()];

        pretty_assertions::assert_eq!(
            indoc::indoc!(
                r#"
                Optimized physical plan:
                DataSinkExec: sink=ParquetSink(file_groups=[])
                  SortExec: expr=[offset@0 ASC NULLS LAST], preserve_partitioning=[false]
                    DataSourceExec: file_groups={1 group: [[...]]}, projection=[offset, op, system_time, date, city, population], file_type=parquet
                "#
            )
            .trim(),
            plan_clean
        );
    });

    let data_path = data_helper.get_last_data_file().await;
    odf::utils::testing::assert_parquet_offsets_are_in_order(&data_path);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_keep_metadata_only_compact() {
    let harness = CompactTestHarness::new();

    let target_root = harness.create_test_root_dataset().await;
    let target_derived = harness.create_test_derived_dataset().await;
    let derived_dataset_ref = target_derived.get_handle().as_local_ref();

    let data_str = indoc!(
        "
        date,city,population
        2020-01-01,A,1000
        2020-01-02,B,2000
        2020-01-03,C,3000
        "
    );

    harness
        .ingest_data(data_str.to_string(), target_root.clone())
        .await;

    let prev_head = target_derived
        .as_metadata_chain()
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .unwrap();

    assert_matches!(
        harness
            .compact_dataset(
                target_derived.clone(),
                CompactionOptions {
                    keep_metadata_only: true,
                    ..CompactionOptions::default()
                },
            )
            .await,
        Ok(CompactionResult::NothingToDo)
    );

    assert_eq!(
        prev_head,
        target_derived
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .unwrap()
    );

    // Round 1: Compact derived dataset
    //
    // Before: seed <- set_transform <- execute_transform
    //
    // After: seed <- set_transform
    let res = harness
        .transform_helper
        .transform_dataset(target_derived.clone())
        .await;
    assert_matches!(res, TransformResult::Updated { .. });

    assert_matches!(
        harness
            .compact_dataset(
                target_derived.clone(),
                CompactionOptions {
                    keep_metadata_only: true,
                    ..CompactionOptions::default()
                },
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 2,
            old_num_blocks: 3
        }) if new_head != old_head,
    );

    assert!(harness.verify_dataset(target_derived).await);
    assert!(
        !harness
            .check_is_data_slices_exist(&derived_dataset_ref)
            .await
    );

    // Round 2: Compact root dataset
    //
    // Before/after: seed <- add_push_source <- set_vocab <- set_schema <-
    // add_data(3 records) <- add_data(3 records)
    //
    // After: seed <- add_push_source <- set_vocab <- set_schema <-
    // set_data_schema
    let data_str = indoc!(
        "
        date,city,population
        2020-01-04,A,4000
        2020-01-05,B,5000
        2020-01-06,C,6000
        "
    );

    harness
        .ingest_data(data_str.to_string(), target_root.clone())
        .await;

    assert_matches!(
        harness
            .compact_dataset(
                target_root.clone(),
                CompactionOptions {
                    keep_metadata_only: true,
                    ..CompactionOptions::default()
                },
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 4,
            old_num_blocks: 6
        }) if new_head != old_head,
    );

    assert!(harness.verify_dataset(target_root).await);
    assert!(
        !harness
            .check_is_data_slices_exist(&derived_dataset_ref)
            .await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_hard_compaction_with_schema_migration() {
    use odf::metadata::*;

    let harness = CompactTestHarness::new();

    let target = harness.create_test_root_dataset().await;
    let dataset_ref = target.get_handle().as_local_ref();

    // 1: Ingest some data
    let data_helper = harness.dataset_data_helper(&dataset_ref).await;

    harness
        .ingest_data(
            indoc!(
                r#"
                date,city,population
                2020-01-01,A,1000
                2020-01-02,B,2000
                2020-01-03,C,3000
                "#
            )
            .to_string(),
            target.clone(),
        )
        .await;

    harness
        .ingest_data(
            indoc!(
                r#"
                date,city,population
                2020-01-04,A,4000
                2020-01-05,B,5000
                2020-01-06,C,6000
                "#
            )
            .to_string(),
            target.clone(),
        )
        .await;

    // 2: Migrate schema by adding optional `census_url` column
    target
        .commit_event(
            SetDataSchema::new(DataSchema::new(vec![
                DataField::i64("offset"),
                DataField::i32("op"),
                DataField::timestamp_millis_utc("system_time"),
                DataField::timestamp_millis_utc("date"),
                DataField::string("city").optional(), // Poor ingest inference
                DataField::i64("population").optional(), // Poor ingest inference
                DataField::string("census_url").optional(),
            ]))
            .into(),
            odf::dataset::CommitOpts {
                system_time: Some(harness.current_date_time),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // 3: Update push source to match
    target
        .commit_event(
            AddPushSource {
                source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
                read: ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        [
                            "date TIMESTAMP",
                            "city STRING",
                            "population BIGINT",
                            "census_url STRING",
                        ]
                        .iter()
                        .map(|s| (*s).to_string())
                        .collect(),
                    ),
                    ..Default::default()
                }
                .into(),
                preprocess: None,
                merge: MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                }
                .into(),
            }
            .into(),
            odf::dataset::CommitOpts {
                system_time: Some(harness.current_date_time),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // 4: Ingest more data with new column
    harness
        .ingest_data(
            indoc!(
                r#"
                date,city,population,census_url
                2020-01-07,A,7000,https://a.ca/census
                2020-01-08,B,8000,
                2020-01-09,C,9000,
                "#
            )
            .to_string(),
            target.clone(),
        )
        .await;

    harness
        .ingest_data(
            indoc!(
                r#"
                date,city,population,census_url
                2020-01-10,A,10000,https://a.ca/census
                2020-01-11,B,11000,https://b.ca/census
                2020-01-12,C,12000,https://c.ca/census
                "#
            )
            .to_string(),
            target.clone(),
        )
        .await;

    // seed <- add_push_source <- set_vocab <- set_schema <- add_data(3r) <-
    // add_data(3r) <- set_licence <- add_data(3r) <- add_data(3r)
    let old_events: Vec<MetadataEvent> = harness
        .get_dataset_blocks(&dataset_ref)
        .await
        .into_iter()
        .rev()
        .map(|b| b.event)
        .collect();

    assert_matches!(
        harness
            .compact_dataset(
                target.clone(),
                CompactionOptions::default(),
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 8,
            old_num_blocks: 10
        }) if new_head != old_head,
    );
    assert!(harness.verify_dataset(target).await);

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                  OPTIONAL BYTE_ARRAY census_url (STRING);
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+------+------------+---------------------+
                | offset | op | system_time          | date                 | city | population | census_url          |
                +--------+----+----------------------+----------------------+------+------------+---------------------+
                | 6      | 0  | 2050-01-01T12:00:00Z | 2020-01-07T00:00:00Z | A    | 7000       | https://a.ca/census |
                | 7      | 0  | 2050-01-01T12:00:00Z | 2020-01-08T00:00:00Z | B    | 8000       |                     |
                | 8      | 0  | 2050-01-01T12:00:00Z | 2020-01-09T00:00:00Z | C    | 9000       |                     |
                | 9      | 0  | 2050-01-01T12:00:00Z | 2020-01-10T00:00:00Z | A    | 10000      | https://a.ca/census |
                | 10     | 0  | 2050-01-01T12:00:00Z | 2020-01-11T00:00:00Z | B    | 11000      | https://b.ca/census |
                | 11     | 0  | 2050-01-01T12:00:00Z | 2020-01-12T00:00:00Z | C    | 12000      | https://c.ca/census |
                +--------+----+----------------------+----------------------+------+------------+---------------------+
                "#
            ),
        )
        .await;

    let new_events: Vec<MetadataEvent> = harness
        .get_dataset_blocks(&dataset_ref)
        .await
        .into_iter()
        .rev()
        .map(|b| b.event)
        .collect();

    assert_matches!(
        &old_events[..],
        [
            MetadataEvent::Seed(_),
            MetadataEvent::AddPushSource(_),
            MetadataEvent::SetVocab(_),
            MetadataEvent::SetDataSchema(_), // inferred
            MetadataEvent::AddData(_),
            MetadataEvent::AddData(_),
            MetadataEvent::SetDataSchema(_), // migration
            MetadataEvent::AddPushSource(_), // updated source
            MetadataEvent::AddData(_),
            MetadataEvent::AddData(_),
        ]
    );

    // TODO: Consider if hard compaction should've merged the two slices together
    // while applying the migration
    assert_matches!(
        &new_events[..],
        [
            MetadataEvent::Seed(_),
            MetadataEvent::AddPushSource(_),
            MetadataEvent::SetVocab(_),
            MetadataEvent::SetDataSchema(_), // inferred
            MetadataEvent::AddData(AddData {
                new_data: Some(DataSlice {
                    offset_interval: OffsetInterval { start: 0, end: 5 },
                    ..
                }),
                ..
            }),
            MetadataEvent::SetDataSchema(_), // migration
            MetadataEvent::AddPushSource(_), // updated source
            MetadataEvent::AddData(AddData {
                new_data: Some(DataSlice {
                    offset_interval: OffsetInterval { start: 6, end: 11 },
                    ..
                }),
                ..
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CompactTestHarness {
    _temp_dir: tempfile::TempDir,
    did_generator: Arc<dyn DidGenerator>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    compaction_planner: Arc<dyn CompactionPlanner>,
    compaction_executor: Arc<dyn CompactionExecutor>,
    ingest_data_use_case: Arc<dyn PushIngestDataUseCase>,
    transform_helper: TransformTestHelper,
    verification_svc: Arc<dyn VerificationService>,
    current_date_time: DateTime<Utc>,
    ctx: SessionContext,
}

impl CompactTestHarness {
    fn new() -> Self {
        Self::new_local()
    }

    fn new_local() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        let datasets_dir = temp_dir.path().join("datasets");
        std::fs::create_dir(&run_info_dir).unwrap();
        std::fs::create_dir(&datasets_dir).unwrap();
        let current_date_time = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<DidGeneratorDefault>()
            .add_value(RunInfoDir::new(run_info_dir))
            .add_value(CurrentAccountSubject::new_test())
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                datasets_dir,
            ))
            .add::<odf::dataset::DatasetLfsBuilderDefault>()
            .add::<DatasetRegistrySoloUnitBridge>()
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add_value(SystemTimeSourceStub::new_set(current_date_time))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<DataFormatRegistryImpl>()
            .add_value(EngineConfigDatafusionEmbeddedCompaction::default())
            .add::<CompactionPlannerImpl>()
            .add::<CompactionExecutorImpl>()
            .add_value(EngineConfigDatafusionEmbeddedIngest::default())
            .add::<PushIngestExecutorImpl>()
            .add::<PushIngestPlannerImpl>()
            .add::<AccountServiceImpl>()
            .add::<InMemoryAccountRepository>()
            .add::<InMemoryAccountQuotaEventStore>()
            .add::<AccountQuotaServiceImpl>()
            .add::<AccountQuotaCheckerStorageImpl>()
            .add::<InMemoryDatasetStatisticsRepository>()
            .add::<DatasetStatisticsServiceImpl>()
            .add_value(QuotaDefaultsConfig::default())
            .add::<PushIngestDataUseCaseImpl>()
            .add::<DummyOutboxImpl>()
            .add::<TransformRequestPlannerImpl>()
            .add::<TransformElaborationServiceImpl>()
            .add::<TransformExecutorImpl>()
            .add_value(
                mock_engine_provisioner::MockEngineProvisioner::new().stub_provision_engine(),
            )
            .bind::<dyn EngineProvisioner, mock_engine_provisioner::MockEngineProvisioner>()
            .add::<VerificationServiceImpl>()
            .build();

        let transform_helper = TransformTestHelper::from_catalog(&catalog);

        Self {
            _temp_dir: temp_dir,
            did_generator: catalog.get_one().unwrap(),
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
            compaction_planner: catalog.get_one().unwrap(),
            compaction_executor: catalog.get_one().unwrap(),
            ingest_data_use_case: catalog.get_one().unwrap(),
            verification_svc: catalog.get_one().unwrap(),
            transform_helper,
            current_date_time,
            ctx: SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1)),
        }
    }

    async fn new_s3(s3: &LocalS3Server) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = Arc::new(RunInfoDir::new(temp_dir.path().join("run")));
        let s3_context = S3Context::from_url(&s3.url).await;
        let current_date_time = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<DidGeneratorDefault>()
            .add_builder(run_info_dir.clone())
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(odf::dataset::DatasetStorageUnitS3::builder(
                s3_context.clone(),
            ))
            .add_builder(odf::dataset::DatasetS3BuilderDefault::builder(None))
            .add::<DatasetRegistrySoloUnitBridge>()
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add_value(SystemTimeSourceStub::new_set(current_date_time))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<EngineProvisionerNull>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add_builder(ObjectStoreBuilderS3::builder(s3_context, true))
            .add::<VerificationServiceImpl>()
            .add_value(EngineConfigDatafusionEmbeddedIngest::default())
            .add::<PushIngestExecutorImpl>()
            .add::<PushIngestPlannerImpl>()
            .add::<PushIngestDataUseCaseImpl>()
            .add::<AccountServiceImpl>()
            .add::<InMemoryAccountRepository>()
            .add::<InMemoryAccountQuotaEventStore>()
            .add::<AccountQuotaServiceImpl>()
            .add::<InMemoryDatasetStatisticsRepository>()
            .add::<DatasetStatisticsServiceImpl>()
            .add::<AccountQuotaCheckerStorageImpl>()
            .add::<DummyOutboxImpl>()
            .add::<TransformRequestPlannerImpl>()
            .add::<TransformElaborationServiceImpl>()
            .add::<TransformExecutorImpl>()
            .add::<DataFormatRegistryImpl>()
            .add_value(QuotaDefaultsConfig::default())
            .add_value(EngineConfigDatafusionEmbeddedCompaction::default())
            .add::<CompactionPlannerImpl>()
            .add::<CompactionExecutorImpl>()
            .add_value(CurrentAccountSubject::new_test())
            .build();

        let transform_helper = TransformTestHelper::from_catalog(&catalog);

        Self {
            _temp_dir: temp_dir,
            did_generator: catalog.get_one().unwrap(),
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
            compaction_planner: catalog.get_one().unwrap(),
            compaction_executor: catalog.get_one().unwrap(),
            ingest_data_use_case: catalog.get_one().unwrap(),
            transform_helper,
            verification_svc: catalog.get_one().unwrap(),
            current_date_time,
            ctx: SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1)),
        }
    }

    async fn get_dataset_head(&self, dataset_ref: &odf::DatasetRef) -> odf::Multihash {
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_ref(dataset_ref)
            .await
            .unwrap();

        resolved_dataset
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .unwrap()
    }

    async fn get_dataset_blocks(&self, dataset_ref: &odf::DatasetRef) -> Vec<odf::MetadataBlock> {
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_ref(dataset_ref)
            .await
            .unwrap();
        let head = self.get_dataset_head(dataset_ref).await;

        resolved_dataset
            .as_metadata_chain()
            .iter_blocks_interval((&head).into(), None, false)
            .map_ok(|(_, b)| b)
            .try_collect()
            .await
            .unwrap()
    }

    async fn store_dataset(
        &self,
        dataset_snapshot: odf::DatasetSnapshot,
    ) -> odf::dataset::StoreDatasetResult {
        create_test_dataset_from_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            dataset_snapshot,
            self.did_generator.generate_dataset_id().0,
            self.current_date_time,
        )
        .await
        .unwrap()
    }

    async fn create_test_root_dataset(&self) -> ResolvedDataset {
        let dataset_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

        let store_result = self
            .store_dataset(
                MetadataFactory::dataset_snapshot()
                    .name("foo")
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
            )
            .await;

        ResolvedDataset::from_stored(&store_result, &dataset_alias)
    }

    async fn create_test_derived_dataset(&self) -> ResolvedDataset {
        let dataset_alias =
            odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo-derivative"));

        let store_result = self
            .store_dataset(
                MetadataFactory::dataset_snapshot()
                    .name("foo-derivative")
                    .kind(odf::DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(["foo"])
                            .transform(
                                MetadataFactory::transform()
                                    .engine("datafusion")
                                    .query(
                                        "SELECT
                                        op,
                                        event_time,
                                        city,
                                        cast(population * 10 as int) as population_x10
                                    FROM root",
                                    )
                                    .build(),
                            )
                            .build(),
                    )
                    .build(),
            )
            .await;

        ResolvedDataset::from_stored(&store_result, &dataset_alias)
    }

    async fn dataset_data_helper(&self, dataset_ref: &odf::DatasetRef) -> DatasetDataHelper {
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_ref(dataset_ref)
            .await
            .unwrap();

        DatasetDataHelper::new_with_context((*resolved_dataset).clone(), self.ctx.clone())
    }

    async fn ingest_multiple_blocks(
        &self,
        target: ResolvedDataset,
        blocks: i64,
        records_per_block: i64,
    ) {
        use std::io::Write;

        let mut event_time = Utc.with_ymd_and_hms(2010, 1, 1, 0, 0, 0).unwrap();
        let cities = ["A", "B", "C", "D"];

        for b in 0..blocks {
            let mut data = Vec::new();
            writeln!(&mut data, "date,city,population").unwrap();

            for r in 0..records_per_block {
                writeln!(
                    &mut data,
                    "{},{},{}",
                    event_time.to_rfc3339(),
                    cities[usize::try_from(r).unwrap() % cities.len()],
                    b * records_per_block + r
                )
                .unwrap();

                event_time += chrono::Duration::minutes(1);
            }
            self.ingest_data(String::from_utf8(data).unwrap(), target.clone())
                .await;
        }
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

    async fn commit_set_licence_block(&self, dataset_ref: &odf::DatasetRef, head: &odf::Multihash) {
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_ref(dataset_ref)
            .await
            .unwrap();
        let event = odf::metadata::SetLicense {
            short_name: "sl1".to_owned(),
            name: "set_license1".to_owned(),
            spdx_id: None,
            website_url: "http://set-license.com".to_owned(),
        };

        resolved_dataset
            .commit_event(
                event.into(),
                odf::dataset::CommitOpts {
                    block_ref: &odf::BlockRef::Head,
                    system_time: Some(self.current_date_time),
                    prev_block_hash: Some(Some(head)),
                    check_object_refs: false,
                    update_block_ref: true,
                },
            )
            .await
            .unwrap();
    }

    // Ensures that old an new blocks end with same watermark, source state, and
    // checkpoint
    fn assert_end_state_equivalent(old_event: &odf::MetadataEvent, new_event: &odf::MetadataEvent) {
        use odf::metadata::EnumWithVariants;
        let old_event = old_event.as_variant::<odf::metadata::AddData>().unwrap();
        let new_event = new_event.as_variant::<odf::metadata::AddData>().unwrap();

        let Some(old_data) = old_event.new_data.as_ref() else {
            panic!("Old event didn't contain data:\n{old_event:#?}");
        };
        let Some(new_data) = new_event.new_data.as_ref() else {
            panic!("Old event didn't contain data:\n{new_event:#?}");
        };
        assert_eq!(old_data.offset_interval.end, new_data.offset_interval.end);

        assert_eq!(old_event.prev_checkpoint, new_event.prev_checkpoint);
        assert_eq!(old_event.new_watermark, new_event.new_watermark);
        assert_eq!(old_event.new_source_state, new_event.new_source_state);
    }

    // Ensures that there are no odf::metadata::AddData/Executetransform blocks
    async fn check_is_data_slices_exist(&self, dataset_ref: &odf::DatasetRef) -> bool {
        let blocks = self.get_dataset_blocks(dataset_ref).await;
        for block in &blocks {
            match block.event {
                odf::MetadataEvent::AddData(_) | odf::MetadataEvent::ExecuteTransform(_) => {
                    return true;
                }
                _ => (),
            }
        }
        false
    }

    fn assert_offset_interval_eq(
        event: &odf::metadata::AddData,
        expected: &odf::metadata::OffsetInterval,
    ) {
        let expected_prev_offset = if expected.start == 0 {
            None
        } else {
            Some(expected.start - 1)
        };
        assert_eq!(event.prev_offset, expected_prev_offset);
        let Some(data) = event.new_data.as_ref() else {
            panic!("Event didn't contain data:\n{event:#?}");
        };
        assert_eq!(data.offset_interval, *expected);
    }

    async fn verify_dataset(&self, target: ResolvedDataset) -> bool {
        let result = self
            .verification_svc
            .verify(
                VerificationRequest {
                    target,
                    block_range: (None, None),
                    options: VerificationOptions::default(),
                },
                None,
            )
            .await;

        result.outcome.is_ok()
    }

    async fn compact_dataset(
        &self,
        target: ResolvedDataset,
        compaction_options: CompactionOptions,
    ) -> Result<CompactionResult, CompactionError> {
        let compaction_plan = self
            .compaction_planner
            .plan_compaction(target.clone(), compaction_options, None)
            .await?;

        let result = self
            .compaction_executor
            .execute(target.clone(), compaction_plan, None)
            .await?;

        if let CompactionResult::Success {
            old_head, new_head, ..
        } = &result
        {
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

        Ok(result)
    }
}
