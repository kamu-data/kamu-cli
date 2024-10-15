// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::{TimeZone, Utc};
use kamu_cli_e2e_common::{
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;
use opendatafabric::{
    AddData,
    AddPushSource,
    DatasetKind,
    DatasetName,
    EnumWithVariants,
    MergeStrategy,
    MergeStrategyLedger,
    MetadataEvent,
    OffsetInterval,
    ReadStep,
    ReadStepNdJson,
    SetDataSchema,
    SetTransform,
    SetVocab,
    SqlQueryStep,
    Transform,
    TransformSql,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_log(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        ["ingest", "player-scores", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await
    .success();

    kamu.execute_with_input(
        ["ingest", "player-scores", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    )
    .await
    .success();

    {
        let mut metadata_blocks = kamu
            .list_blocks(&DatasetName::new_unchecked("player-scores"))
            .await
            .into_iter()
            .map(|br| br.block)
            .collect::<Vec<_>>();

        pretty_assertions::assert_eq!(6, metadata_blocks.len());

        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(0, block.sequence_number);

            assert_matches!(
                block.event,
                MetadataEvent::Seed(event)
                    if event.dataset_kind == DatasetKind::Root
            );
        }
        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(1, block.sequence_number);

            let actual_push_source = block.event.as_variant::<AddPushSource>().unwrap();
            let expected_push_source = AddPushSource {
                source_name: "default".to_string(),
                read: ReadStep::NdJson(ReadStepNdJson {
                    schema: Some(vec![
                        "match_time TIMESTAMP".into(),
                        "match_id BIGINT".into(),
                        "player_id STRING".into(),
                        "score BIGINT".into(),
                    ]),
                    date_format: None,
                    encoding: None,
                    timestamp_format: None,
                }),
                preprocess: None,
                merge: MergeStrategy::Ledger(MergeStrategyLedger {
                    primary_key: vec!["match_id".into(), "player_id".into()],
                }),
            };

            pretty_assertions::assert_eq!(&expected_push_source, actual_push_source);
        }
        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(2, block.sequence_number);

            let actual_set_vocab = block.event.as_variant::<SetVocab>().unwrap();
            let expected_set_vocab = SetVocab {
                offset_column: None,
                operation_type_column: None,
                system_time_column: None,
                event_time_column: Some("match_time".into()),
            };

            pretty_assertions::assert_eq!(&expected_set_vocab, actual_set_vocab);
        }
        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(3, block.sequence_number);

            let actual_set_data_schema = block.event.as_variant::<SetDataSchema>().unwrap();
            let expected_set_data_schema = SetDataSchema {
                schema: vec![
                    12, 0, 0, 0, 8, 0, 8, 0, 0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 124,
                    1, 0, 0, 60, 1, 0, 0, 244, 0, 0, 0, 180, 0, 0, 0, 108, 0, 0, 0, 56, 0, 0, 0, 4,
                    0, 0, 0, 108, 255, 255, 255, 16, 0, 0, 0, 24, 0, 0, 0, 0, 0, 1, 2, 20, 0, 0, 0,
                    160, 254, 255, 255, 64, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 5, 0, 0, 0, 115, 99,
                    111, 114, 101, 0, 0, 0, 156, 255, 255, 255, 24, 0, 0, 0, 12, 0, 0, 0, 0, 0, 1,
                    5, 16, 0, 0, 0, 0, 0, 0, 0, 4, 0, 4, 0, 4, 0, 0, 0, 9, 0, 0, 0, 112, 108, 97,
                    121, 101, 114, 95, 105, 100, 0, 0, 0, 204, 255, 255, 255, 16, 0, 0, 0, 24, 0,
                    0, 0, 0, 0, 1, 2, 20, 0, 0, 0, 0, 255, 255, 255, 64, 0, 0, 0, 0, 0, 0, 1, 0, 0,
                    0, 0, 8, 0, 0, 0, 109, 97, 116, 99, 104, 95, 105, 100, 0, 0, 0, 0, 16, 0, 20,
                    0, 16, 0, 14, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0, 0, 20, 0, 0, 0, 12, 0, 0,
                    0, 0, 0, 1, 10, 28, 0, 0, 0, 0, 0, 0, 0, 196, 255, 255, 255, 8, 0, 0, 0, 0, 0,
                    1, 0, 3, 0, 0, 0, 85, 84, 67, 0, 10, 0, 0, 0, 109, 97, 116, 99, 104, 95, 116,
                    105, 109, 101, 0, 0, 144, 255, 255, 255, 28, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 10,
                    36, 0, 0, 0, 0, 0, 0, 0, 8, 0, 12, 0, 10, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 0,
                    0, 1, 0, 3, 0, 0, 0, 85, 84, 67, 0, 11, 0, 0, 0, 115, 121, 115, 116, 101, 109,
                    95, 116, 105, 109, 101, 0, 212, 255, 255, 255, 16, 0, 0, 0, 24, 0, 0, 0, 0, 0,
                    0, 2, 20, 0, 0, 0, 196, 255, 255, 255, 32, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 2,
                    0, 0, 0, 111, 112, 0, 0, 16, 0, 20, 0, 16, 0, 0, 0, 15, 0, 4, 0, 0, 0, 8, 0,
                    16, 0, 0, 0, 24, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 2, 28, 0, 0, 0, 8, 0, 12, 0, 4,
                    0, 11, 0, 8, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 6, 0, 0, 0, 111,
                    102, 102, 115, 101, 116, 0, 0,
                ],
            };

            pretty_assertions::assert_eq!(&expected_set_data_schema, actual_set_data_schema);
        }
        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(4, block.sequence_number);

            let actual_add_data = block.event.as_variant::<AddData>().unwrap();

            pretty_assertions::assert_eq!(None, actual_add_data.prev_checkpoint);
            pretty_assertions::assert_eq!(None, actual_add_data.prev_offset);

            let actual_new_data = actual_add_data.new_data.as_ref().unwrap();

            pretty_assertions::assert_eq!(
                OffsetInterval { start: 0, end: 1 },
                actual_new_data.offset_interval
            );
            pretty_assertions::assert_eq!(1665, actual_new_data.size);

            pretty_assertions::assert_eq!(None, actual_add_data.new_checkpoint);
            pretty_assertions::assert_eq!(
                Some(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap()),
                actual_add_data.new_watermark
            );
            pretty_assertions::assert_eq!(None, actual_add_data.new_source_state);
        }
        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(5, block.sequence_number);

            let actual_add_data = block.event.as_variant::<AddData>().unwrap();

            pretty_assertions::assert_eq!(None, actual_add_data.prev_checkpoint);
            pretty_assertions::assert_eq!(Some(1), actual_add_data.prev_offset);

            let actual_new_data = actual_add_data.new_data.as_ref().unwrap();

            pretty_assertions::assert_eq!(
                OffsetInterval { start: 2, end: 3 },
                actual_new_data.offset_interval
            );
            pretty_assertions::assert_eq!(1681, actual_new_data.size);

            pretty_assertions::assert_eq!(None, actual_add_data.new_checkpoint);
            pretty_assertions::assert_eq!(
                Some(Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()),
                actual_add_data.new_watermark
            );
            pretty_assertions::assert_eq!(None, actual_add_data.new_source_state);
        }
    }

    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();

    {
        let mut metadata_blocks = kamu
            .list_blocks(&DatasetName::new_unchecked("leaderboard"))
            .await
            .into_iter()
            .map(|br| br.block)
            .collect::<Vec<_>>();

        pretty_assertions::assert_eq!(3, metadata_blocks.len());

        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(0, block.sequence_number);

            assert_matches!(
                block.event,
                MetadataEvent::Seed(event)
                    if event.dataset_kind == DatasetKind::Derivative
            );
        }
        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(1, block.sequence_number);

            let actual_set_transform = block.event.as_variant::<SetTransform>().unwrap();

            pretty_assertions::assert_eq!(1, actual_set_transform.inputs.len());
            pretty_assertions::assert_eq!(
                Some("player_scores".into()),
                actual_set_transform.inputs[0].alias
            );

            let expected_transform = Transform::Sql(TransformSql {
                engine: "risingwave".into(),
                version: None,
                query: None,
                queries: Some(vec![
                    SqlQueryStep {
                        alias: Some("leaderboard".into()),
                        query: indoc::indoc!(
                            r#"
                            create materialized view leaderboard as
                            select
                              *
                            from (
                              select
                                row_number() over (partition by 1 order by score desc) as place,
                                match_time,
                                match_id,
                                player_id,
                                score
                              from player_scores
                            )
                            where place <= 2
                            "#
                        )
                        .into(),
                    },
                    SqlQueryStep {
                        alias: None,
                        query: "select * from leaderboard".into(),
                    },
                ]),
                temporal_tables: None,
            });

            pretty_assertions::assert_eq!(expected_transform, actual_set_transform.transform);
        }
        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(2, block.sequence_number);

            let actual_set_vocab = block.event.as_variant::<SetVocab>().unwrap();
            let expected_set_vocab = SetVocab {
                offset_column: None,
                operation_type_column: None,
                system_time_column: None,
                event_time_column: Some("match_time".into()),
            };

            pretty_assertions::assert_eq!(&expected_set_vocab, actual_set_vocab);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
