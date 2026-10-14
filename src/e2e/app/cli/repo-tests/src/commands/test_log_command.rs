// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use chrono::{TimeZone, Utc};
use kamu_cli_e2e_common::{
    DATASET_DERIVATIVE_LEADERBOARD_NAME,
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use odf::metadata::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_log_command(kamu: KamuCliPuppet) {
    use odf::metadata::EnumWithVariants;

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
            .list_blocks(&DATASET_ROOT_PLAYER_NAME)
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
                odf::MetadataEvent::Seed(event)
                    if event.dataset_kind == odf::DatasetKind::Root
            );
        }
        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(1, block.sequence_number);

            let actual_push_source = block
                .event
                .as_variant::<odf::metadata::AddPushSource>()
                .unwrap();
            let expected_push_source = odf::metadata::AddPushSource {
                source_name: "default".to_string(),
                read: odf::metadata::ReadStep::NdJson(odf::metadata::ReadStepNdJson {
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::timestamp_millis_utc("match_time"),
                        odf::schema::DataField::i64("match_id"),
                        odf::schema::DataField::string("player_id"),
                        odf::schema::DataField::i64("score"),
                    ])),
                    ..Default::default()
                }),
                preprocess: None,
                merge: odf::metadata::MergeStrategy::Ledger(odf::metadata::MergeStrategyLedger {
                    primary_key: vec!["match_id".into(), "player_id".into()],
                }),
            };

            pretty_assertions::assert_eq!(&expected_push_source, actual_push_source);
        }
        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(2, block.sequence_number);

            let actual_set_vocab = block.event.as_variant::<odf::metadata::SetVocab>().unwrap();
            let expected_set_vocab = odf::metadata::SetVocab {
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

            let actual_set_data_schema = block
                .event
                .as_variant::<odf::metadata::SetDataSchema>()
                .unwrap();
            let expected_set_data_schema = odf::metadata::SetDataSchema {
                raw_arrow_schema: None,
                schema: Some(DataSchema {
                    fields: vec![
                        DataField::i64("offset"),
                        DataField::i32("op"),
                        DataField::timestamp_millis_utc("system_time"),
                        DataField::timestamp_millis_utc("match_time"),
                        DataField::i64("match_id"),
                        DataField::string("player_id"),
                        DataField::i64("score"),
                    ],
                    extra: None,
                }),
            };

            pretty_assertions::assert_eq!(&expected_set_data_schema, actual_set_data_schema);
        }
        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(4, block.sequence_number);

            let actual_add_data = block.event.as_variant::<odf::metadata::AddData>().unwrap();

            pretty_assertions::assert_eq!(None, actual_add_data.prev_checkpoint);
            pretty_assertions::assert_eq!(None, actual_add_data.prev_offset);

            let actual_new_data = actual_add_data.new_data.as_ref().unwrap();

            pretty_assertions::assert_eq!(
                odf::metadata::OffsetInterval { start: 0, end: 1 },
                actual_new_data.offset_interval
            );
            assert!(actual_new_data.size > 2000 && actual_new_data.size < 3000);

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

            let actual_add_data = block.event.as_variant::<odf::metadata::AddData>().unwrap();

            pretty_assertions::assert_eq!(None, actual_add_data.prev_checkpoint);
            pretty_assertions::assert_eq!(Some(1), actual_add_data.prev_offset);

            let actual_new_data = actual_add_data.new_data.as_ref().unwrap();

            pretty_assertions::assert_eq!(
                odf::metadata::OffsetInterval { start: 2, end: 3 },
                actual_new_data.offset_interval
            );
            assert!(actual_new_data.size > 2000 && actual_new_data.size < 3000);

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
            .list_blocks(&DATASET_DERIVATIVE_LEADERBOARD_NAME)
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
                odf::MetadataEvent::Seed(event)
                    if event.dataset_kind == odf::DatasetKind::Derivative
            );
        }
        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(1, block.sequence_number);

            let actual_set_transform = block
                .event
                .as_variant::<odf::metadata::SetTransform>()
                .unwrap();

            pretty_assertions::assert_eq!(1, actual_set_transform.inputs.len());
            pretty_assertions::assert_eq!(
                Some("player_scores".into()),
                actual_set_transform.inputs[0].alias
            );

            let expected_transform = odf::metadata::Transform::Sql(odf::metadata::TransformSql {
                engine: "datafusion".into(),
                version: None,
                query: None,
                queries: Some(vec![odf::metadata::SqlQueryStep {
                    alias: None,
                    query: indoc::indoc!(
                        r#"
                        SELECT ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY score DESC) AS place,
                               match_time,
                               match_id,
                               player_id,
                               score
                        FROM player_scores
                        LIMIT 2
                        "#
                    )
                    .into(),
                }]),
                temporal_tables: None,
            });

            pretty_assertions::assert_eq!(expected_transform, actual_set_transform.transform);
        }
        {
            let block = metadata_blocks.pop().unwrap();

            pretty_assertions::assert_eq!(2, block.sequence_number);

            let actual_set_vocab = block.event.as_variant::<odf::metadata::SetVocab>().unwrap();
            let expected_set_vocab = odf::metadata::SetVocab {
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

pub async fn test_log_command_json_output(kamu: KamuCliPuppet) {
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

    let ndjson_records = kamu
        .log_stdout(&DATASET_ROOT_PLAYER_NAME, &["-o", "ndjson"])
        .await
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();

    assert!(ndjson_records.iter().all(|r| r["kind"] == "MetadataBlock"));
    pretty_assertions::assert_eq!(
        vec![
            (5, "AddData"),
            (4, "AddData"),
            (3, "SetDataSchema"),
            (2, "SetVocab"),
            (1, "AddPushSource"),
            (0, "Seed"),
        ],
        ndjson_records
            .iter()
            .map(|r| (
                r["content"]["sequenceNumber"].as_u64().unwrap(),
                r["content"]["event"]["kind"].as_str().unwrap(),
            ))
            .collect::<Vec<_>>()
    );

    // Each record is the YAML document as JSON, with the hash from its `# Block:` comment
    let yaml_records = kamu
        .log_stdout(&DATASET_ROOT_PLAYER_NAME, &["-o", "yaml"])
        .await
        .split("---\n")
        .skip(1)
        .map(|doc| {
            let (hash_line, manifest) = doc.split_once('\n').unwrap();
            let block_hash = hash_line.strip_prefix("# Block: ").unwrap();

            let mut record: serde_json::Value = serde_yaml::from_str(manifest).unwrap();
            record["blockHash"] = serde_json::Value::from(block_hash);
            record
        })
        .collect::<Vec<_>>();

    pretty_assertions::assert_eq!(yaml_records, ndjson_records);

    let json_records: Vec<serde_json::Value> = serde_json::from_str(
        &kamu
            .log_stdout(&DATASET_ROOT_PLAYER_NAME, &["-o", "json"])
            .await,
    )
    .unwrap();

    pretty_assertions::assert_eq!(ndjson_records, json_records);

    let limited_json_records: Vec<serde_json::Value> = serde_json::from_str(
        &kamu
            .log_stdout(&DATASET_ROOT_PLAYER_NAME, &["-o", "json", "--limit", "2"])
            .await,
    )
    .unwrap();

    pretty_assertions::assert_eq!(&ndjson_records[..2], &limited_json_records[..]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
