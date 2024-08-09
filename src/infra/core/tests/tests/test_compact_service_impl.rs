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
use domain::{CompactionError, CompactionOptions, CompactionResult, CompactionService};
use futures::TryStreamExt;
use indoc::{formatdoc, indoc};
use kamu::domain::*;
use kamu::testing::{DatasetDataHelper, LocalS3Server, MetadataFactory};
use kamu::utils::s3_context::S3Context;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_auth_rebac_inmem::RebacRepositoryInMem;
use kamu_auth_rebac_services::RebacServiceImpl;
use kamu_core::auth;
use opendatafabric::*;
use time_source::{SystemTimeSource, SystemTimeSourceStub};

use super::test_pull_service_impl::TestTransformService;
use crate::mock_engine_provisioner;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_compact() {
    let harness = CompactTestHarness::new();

    let created = harness.create_test_root_dataset().await;
    let dataset_ref = created.dataset_handle.as_local_ref();

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
        .ingest_data(data_str.to_string(), &dataset_ref)
        .await;

    let prev_head = created
        .dataset
        .as_metadata_chain()
        .resolve_ref(&BlockRef::Head)
        .await
        .unwrap();

    assert_matches!(
        harness
            .compaction_svc
            .compact_dataset(
                &created.dataset_handle,
                CompactionOptions::default(),
                Some(Arc::new(NullCompactionListener {}))
            )
            .await,
        Ok(CompactionResult::NothingToDo)
    );

    assert_eq!(
        prev_head,
        created
            .dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
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
        .ingest_data(data_str.to_string(), &dataset_ref)
        .await;

    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    assert_matches!(
        harness
            .compaction_svc
            .compact_dataset(
                &created.dataset_handle,
                CompactionOptions::default(),
                Some(Arc::new(NullCompactionListener {}))
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 5,
            old_num_blocks: 6
        }) if new_head != old_head,
    );

    assert!(harness.verify_dataset(&dataset_ref).await);

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

    let new_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    let last_old_block = old_blocks.first().unwrap();
    let last_new_block = new_blocks.first().unwrap();
    CompactTestHarness::assert_end_state_equivalent(&last_old_block.event, &last_new_block.event);

    let new_add_data = last_new_block.event.as_variant::<AddData>().unwrap();
    CompactTestHarness::assert_offset_interval_eq(
        new_add_data,
        &OffsetInterval { start: 0, end: 5 },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_compact_s3() {
    let s3 = LocalS3Server::new().await;
    let harness = CompactTestHarness::new_s3(&s3).await;

    let created = harness.create_test_root_dataset().await;
    let dataset_ref = created.dataset_handle.as_local_ref();

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
        .ingest_data(data_str.to_string(), &dataset_ref)
        .await;

    let prev_head = created
        .dataset
        .as_metadata_chain()
        .resolve_ref(&BlockRef::Head)
        .await
        .unwrap();

    assert_matches!(
        harness
            .compaction_svc
            .compact_dataset(
                &created.dataset_handle,
                CompactionOptions::default(),
                Some(Arc::new(NullCompactionListener {}))
            )
            .await,
        Ok(CompactionResult::NothingToDo)
    );

    assert_eq!(
        prev_head,
        created
            .dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
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
        .ingest_data(data_str.to_string(), &dataset_ref)
        .await;

    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    assert_matches!(
        harness
            .compaction_svc
            .compact_dataset(
                &created.dataset_handle,
                CompactionOptions::default(),
                Some(Arc::new(NullCompactionListener {}))
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 5,
            old_num_blocks: 6
        }) if new_head != old_head,
    );

    assert!(harness.verify_dataset(&dataset_ref).await);

    let new_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    let last_old_block = old_blocks.first().unwrap();
    let last_new_block = new_blocks.first().unwrap();
    CompactTestHarness::assert_end_state_equivalent(&last_old_block.event, &last_new_block.event);

    let new_add_data = last_new_block.event.as_variant::<AddData>().unwrap();
    CompactTestHarness::assert_offset_interval_eq(
        new_add_data,
        &OffsetInterval { start: 0, end: 5 },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_compaction_watermark_only_blocks() {
    let harness = CompactTestHarness::new();

    let created = harness.create_test_root_dataset().await;
    let dataset_ref = created.dataset_handle.as_local_ref();

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
        .ingest_data(data_str.to_string(), &dataset_ref)
        .await;

    created
        .dataset
        .commit_add_data(
            AddDataParams {
                prev_checkpoint: None,
                prev_offset: Some(2),
                new_offset_interval: None,
                new_watermark: Some(Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap()),
                new_source_state: None,
            },
            None,
            None,
            CommitOpts {
                system_time: Some(harness.current_date_time),
                ..CommitOpts::default()
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
        .ingest_data(data_str.to_string(), &dataset_ref)
        .await;

    created
        .dataset
        .commit_add_data(
            AddDataParams {
                prev_checkpoint: None,
                prev_offset: Some(5),
                new_offset_interval: None,
                new_watermark: Some(Utc.with_ymd_and_hms(2050, 1, 2, 12, 0, 0).unwrap()),
                new_source_state: Some(SourceState {
                    source_name: "src".to_string(),
                    kind: "odf/etag".to_string(),
                    value: "123".to_string(),
                }),
            },
            None,
            None,
            CommitOpts {
                system_time: Some(harness.current_date_time),
                ..CommitOpts::default()
            },
        )
        .await
        .unwrap();

    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    // After: ... <- add_data(6 records, wm2, src2)
    let res = harness
        .compaction_svc
        .compact_dataset(
            &created.dataset_handle,
            CompactionOptions::default(),
            Some(Arc::new(NullCompactionListener {})),
        )
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

    let old_last_add_data = old_blocks
        .first()
        .unwrap()
        .event
        .as_variant::<AddData>()
        .unwrap();

    let new_add_data = created
        .dataset
        .as_metadata_chain()
        .get_block(&new_head)
        .await
        .unwrap()
        .into_typed::<AddData>()
        .unwrap()
        .event;

    assert_eq!(new_add_data.prev_offset, None);
    assert_eq!(
        new_add_data.new_data.as_ref().unwrap().offset_interval,
        OffsetInterval { start: 0, end: 5 }
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

    let created = harness.create_test_root_dataset().await;
    let dataset_ref = created.dataset_handle.as_local_ref();

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
        .ingest_data(data_str.to_string(), &dataset_ref)
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
        .ingest_data(data_str.to_string(), &dataset_ref)
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
        .ingest_data(data_str.to_string(), &dataset_ref)
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
        .ingest_data(data_str.to_string(), &dataset_ref)
        .await;

    let data_str = indoc!(
        "
        date,city,population
        2020-01-16,A,16000
        "
    );

    harness
        .ingest_data(data_str.to_string(), &dataset_ref)
        .await;

    // Initial state:
    // seed <- add_push_source <- set_vocab <- set_schema <- add_data(3r) <-
    // add_data(3r) <- add_data(6r) <- add_data(3r) <- add_data(1r)
    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    assert_matches!(
        harness
            .compaction_svc
            .compact_dataset(
                &created.dataset_handle,
                CompactionOptions {
                    max_slice_records: Some(6),
                    ..CompactionOptions::default()
                },
                Some(Arc::new(NullCompactionListener {}))
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 7,
            old_num_blocks: 9
        }) if new_head != old_head,
    );
    assert!(harness.verify_dataset(&dataset_ref).await);

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
    let new_data_events: Vec<_> = harness
        .get_dataset_blocks(&dataset_ref)
        .await
        .into_iter()
        .filter_map(|b| b.event.into_variant::<AddData>())
        .rev()
        .collect();

    assert_eq!(new_data_events.len(), 3);

    CompactTestHarness::assert_offset_interval_eq(
        &new_data_events[0],
        &OffsetInterval { start: 0, end: 5 },
    );
    CompactTestHarness::assert_offset_interval_eq(
        &new_data_events[1],
        &OffsetInterval { start: 6, end: 11 },
    );
    CompactTestHarness::assert_offset_interval_eq(
        &new_data_events[2],
        &OffsetInterval { start: 12, end: 15 },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_compaction_keep_all_non_data_blocks() {
    let harness = CompactTestHarness::new();

    let created = harness.create_test_root_dataset().await;
    let dataset_ref = created.dataset_handle.as_local_ref();

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
        .ingest_data(data_str.to_string(), &dataset_ref)
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
        .ingest_data(data_str.to_string(), &dataset_ref)
        .await;

    let current_head = harness
        .get_dataset_head(&created.dataset_handle.as_local_ref())
        .await;
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
        .ingest_data(data_str.to_string(), &dataset_ref)
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
        .ingest_data(data_str.to_string(), &dataset_ref)
        .await;

    // seed <- add_push_source <- set_vocab <- set_schema <- add_data(3r) <-
    // add_data(3r) <- set_licence <- add_data(3r) <- add_data(3r)
    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    assert_matches!(
        harness
            .compaction_svc
            .compact_dataset(
                &created.dataset_handle,
                CompactionOptions::default(),
                Some(Arc::new(NullCompactionListener {}))
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 7,
            old_num_blocks: 9
        }) if new_head != old_head,
    );
    assert!(harness.verify_dataset(&dataset_ref).await);

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

    // seed <- add_push_source <- set_vocab <- set_scheme <- add_data(6r) <-
    // set_licence <- add_data(6r)
    let new_blocks = harness.get_dataset_blocks(&dataset_ref).await;

    // The first two and the last two dataslices were merged
    CompactTestHarness::assert_end_state_equivalent(
        &old_blocks.first().unwrap().event,
        &new_blocks.first().unwrap().event,
    );

    CompactTestHarness::assert_offset_interval_eq(
        new_blocks[2].event.as_variant::<AddData>().unwrap(),
        &OffsetInterval { start: 0, end: 5 },
    );

    CompactTestHarness::assert_offset_interval_eq(
        new_blocks[0].event.as_variant::<AddData>().unwrap(),
        &OffsetInterval { start: 6, end: 11 },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(compact)]
#[tokio::test]
async fn test_dataset_compaction_derive_error() {
    let harness = CompactTestHarness::new();

    let created = harness
        .create_dataset(
            MetadataFactory::dataset_snapshot()
                .name("derive.foo")
                .kind(DatasetKind::Derivative)
                .push_event(MetadataFactory::set_data_schema().build())
                .build(),
        )
        .await;

    assert_matches!(
        harness
            .compaction_svc
            .compact_dataset(
                &created.dataset_handle,
                CompactionOptions::default(),
                Some(Arc::new(NullCompactionListener {}))
            )
            .await,
        Err(CompactionError::InvalidDatasetKind(_)),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_large_dataset_compact() {
    let harness = CompactTestHarness::new();

    let created = harness.create_test_root_dataset().await;
    let dataset_ref = created.dataset_handle.as_local_ref();

    harness.ingest_multiple_blocks(&dataset_ref, 100).await;

    let data_helper = harness.dataset_data_helper(&dataset_ref).await;

    let old_blocks = harness.get_dataset_blocks(&dataset_ref).await;

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
            .compaction_svc
            .compact_dataset(
                &created.dataset_handle,
                CompactionOptions {
                    max_slice_records: Some(10),
                    max_slice_size: None,
                    ..CompactionOptions::default()
                },
                Some(Arc::new(NullCompactionListener {}))
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 24,
            old_num_blocks: 104
        }) if new_head != old_head,
    );
    assert!(harness.verify_dataset(&dataset_ref).await);

    let new_blocks = harness.get_dataset_blocks(&dataset_ref).await;

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

    CompactTestHarness::assert_end_state_equivalent(
        &old_blocks.first().unwrap().event,
        &new_blocks.first().unwrap().event,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(ingest, datafusion, compact)]
#[tokio::test]
async fn test_dataset_keep_metadata_only_compact() {
    let harness = CompactTestHarness::new();

    let created_root = harness.create_test_root_dataset().await;
    let created_derived = harness.create_test_derived_dataset().await;
    let root_dataset_ref = created_root.dataset_handle.as_local_ref();
    let derived_dataset_ref = created_derived.dataset_handle.as_local_ref();

    let data_str = indoc!(
        "
        date,city,population
        2020-01-01,A,1000
        2020-01-02,B,2000
        2020-01-03,C,3000
        "
    );

    harness
        .ingest_data(data_str.to_string(), &root_dataset_ref)
        .await;

    let prev_head = created_derived
        .dataset
        .as_metadata_chain()
        .resolve_ref(&BlockRef::Head)
        .await
        .unwrap();

    assert_matches!(
        harness
            .compaction_svc
            .compact_dataset(
                &created_derived.dataset_handle,
                CompactionOptions {
                    keep_metadata_only: true,
                    ..CompactionOptions::default()
                },
                Some(Arc::new(NullCompactionListener {}))
            )
            .await,
        Ok(CompactionResult::NothingToDo)
    );

    assert_eq!(
        prev_head,
        created_derived
            .dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
            .unwrap()
    );

    // Round 1: Compact derived dataset
    //
    // Before: seed <- set_transform <- execute_transform
    //
    // After: seed <- set_transform
    let res = harness
        .transform_svc
        .transform(&derived_dataset_ref, TransformOptions::default(), None)
        .await
        .unwrap();
    assert_matches!(res, TransformResult::Updated { .. });

    assert_matches!(
        harness
            .compaction_svc
            .compact_dataset(
                &created_derived.dataset_handle,
                CompactionOptions {
                    keep_metadata_only: true,
                    ..CompactionOptions::default()
                },
                Some(Arc::new(NullCompactionListener {}))
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 2,
            old_num_blocks: 3
        }) if new_head != old_head,
    );

    assert!(harness.verify_dataset(&derived_dataset_ref).await);
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
        .ingest_data(data_str.to_string(), &root_dataset_ref)
        .await;

    assert_matches!(
        harness
            .compaction_svc
            .compact_dataset(
                &created_root.dataset_handle,
                CompactionOptions {
                    keep_metadata_only: true,
                    ..CompactionOptions::default()
                },
                Some(Arc::new(NullCompactionListener {}))
            )
            .await,
        Ok(CompactionResult::Success {
            new_head,
            old_head,
            new_num_blocks: 4,
            old_num_blocks: 6
        }) if new_head != old_head,
    );

    assert!(harness.verify_dataset(&derived_dataset_ref).await);
    assert!(
        !harness
            .check_is_data_slices_exist(&derived_dataset_ref)
            .await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CompactTestHarness {
    _temp_dir: tempfile::TempDir,
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    compaction_svc: Arc<CompactionServiceImpl>,
    push_ingest_svc: Arc<PushIngestServiceImpl>,
    verification_svc: Arc<dyn VerificationService>,
    transform_svc: Arc<dyn TransformService>,
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
            .add_value(RunInfoDir::new(run_info_dir))
            .add_value(CurrentAccountSubject::new_test())
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add_value(SystemTimeSourceStub::new_set(current_date_time))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<DataFormatRegistryImpl>()
            .add::<CompactionServiceImpl>()
            .add::<PushIngestServiceImpl>()
            .add_value(
                mock_engine_provisioner::MockEngineProvisioner::new().stub_provision_engine(),
            )
            .bind::<dyn EngineProvisioner, mock_engine_provisioner::MockEngineProvisioner>()
            .add::<TransformServiceImpl>()
            .add::<VerificationServiceImpl>()
            .add::<RebacRepositoryInMem>()
            .add::<RebacServiceImpl>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        let dataset_repo_writer = catalog.get_one::<dyn DatasetRepositoryWriter>().unwrap();
        let compaction_svc = catalog.get_one::<CompactionServiceImpl>().unwrap();
        let push_ingest_svc = catalog.get_one::<PushIngestServiceImpl>().unwrap();
        let transform_svc = catalog.get_one::<TransformServiceImpl>().unwrap();
        let verification_svc = catalog.get_one::<dyn VerificationService>().unwrap();

        Self {
            _temp_dir: temp_dir,
            dataset_repo,
            dataset_repo_writer,
            compaction_svc,
            push_ingest_svc,
            transform_svc,
            verification_svc,
            current_date_time,
            ctx: SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1)),
        }
    }

    async fn new_s3(s3: &LocalS3Server) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = Arc::new(RunInfoDir::new(temp_dir.path().join("run")));
        let (endpoint, bucket, key_prefix) = S3Context::split_url(&s3.url);
        let s3_context = S3Context::from_items(endpoint.clone(), bucket, key_prefix).await;
        let current_date_time = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add_builder(run_info_dir.clone())
            .add_builder(
                DatasetRepositoryS3::builder()
                    .with_s3_context(s3_context.clone())
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryS3>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryS3>()
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add_value(SystemTimeSourceStub::new_set(current_date_time))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<EngineProvisionerNull>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add_value(ObjectStoreBuilderS3::new(s3_context.clone(), true))
            .bind::<dyn ObjectStoreBuilder, ObjectStoreBuilderS3>()
            .add_value(TestTransformService::new(Arc::new(Mutex::new(Vec::new()))))
            .bind::<dyn TransformService, TestTransformService>()
            .add::<VerificationServiceImpl>()
            .add::<PushIngestServiceImpl>()
            .add::<DataFormatRegistryImpl>()
            .add::<CompactionServiceImpl>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<RebacRepositoryInMem>()
            .add::<RebacServiceImpl>()
            .build();

        let ctx = new_session_context(catalog.get_one().unwrap());

        Self {
            _temp_dir: temp_dir,
            dataset_repo: catalog.get_one().unwrap(),
            dataset_repo_writer: catalog.get_one().unwrap(),
            compaction_svc: catalog.get_one().unwrap(),
            push_ingest_svc: catalog.get_one().unwrap(),
            verification_svc: catalog.get_one().unwrap(),
            transform_svc: catalog.get_one().unwrap(),
            current_date_time,
            ctx,
        }
    }

    async fn get_dataset_head(&self, dataset_ref: &DatasetRef) -> Multihash {
        let dataset = self
            .dataset_repo
            .find_dataset_by_ref(dataset_ref)
            .await
            .unwrap();

        dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
            .unwrap()
    }

    async fn get_dataset_blocks(&self, dataset_ref: &DatasetRef) -> Vec<MetadataBlock> {
        let dataset = self
            .dataset_repo
            .find_dataset_by_ref(dataset_ref)
            .await
            .unwrap();
        let head = self.get_dataset_head(dataset_ref).await;

        dataset
            .as_metadata_chain()
            .iter_blocks_interval(&head, None, false)
            .map_ok(|(_, b)| b)
            .try_collect()
            .await
            .unwrap()
    }

    async fn create_dataset(&self, dataset_snapshot: DatasetSnapshot) -> CreateDatasetResult {
        self.dataset_repo_writer
            .create_dataset_from_snapshot(dataset_snapshot)
            .await
            .unwrap()
            .create_dataset_result
    }

    async fn create_test_root_dataset(&self) -> CreateDatasetResult {
        self.create_dataset(
            MetadataFactory::dataset_snapshot()
                .name("foo")
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
    }

    async fn create_test_derived_dataset(&self) -> CreateDatasetResult {
        self.create_dataset(
            MetadataFactory::dataset_snapshot()
                .name("foo-derivative")
                .kind(DatasetKind::Derivative)
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
        .await
    }

    async fn dataset_data_helper(&self, dataset_ref: &DatasetRef) -> DatasetDataHelper {
        let dataset = self
            .dataset_repo
            .find_dataset_by_ref(dataset_ref)
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

    async fn commit_set_licence_block(&self, dataset_ref: &DatasetRef, head: &Multihash) {
        let dataset = self
            .dataset_repo
            .find_dataset_by_ref(dataset_ref)
            .await
            .unwrap();
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
    fn assert_end_state_equivalent(old_event: &MetadataEvent, new_event: &MetadataEvent) {
        let old_event = old_event.as_variant::<AddData>().unwrap();
        let new_event = new_event.as_variant::<AddData>().unwrap();

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

    // Ensures that there are no AddData/Executetransform blocks
    async fn check_is_data_slices_exist(&self, dataset_ref: &DatasetRef) -> bool {
        let blocks = self.get_dataset_blocks(dataset_ref).await;
        for block in &blocks {
            match block.event {
                MetadataEvent::AddData(_) | MetadataEvent::ExecuteTransform(_) => return true,
                _ => (),
            }
        }
        false
    }

    fn assert_offset_interval_eq(event: &AddData, expected: &OffsetInterval) {
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
