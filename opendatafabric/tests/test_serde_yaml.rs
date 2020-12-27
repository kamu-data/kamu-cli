use ::serde::{Deserialize, Serialize};
use chrono::prelude::*;
use indoc::indoc;
use opendatafabric::serde::yaml::generated::*;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;
use std::convert::TryFrom;

#[test]
fn serde_dataset_snapshot_root() {
    let data = indoc!(
        "
        ---
        apiVersion: 1
        kind: DatasetSnapshot
        content:
          id: kamu.test
          source:
            kind: root
            fetch:
              kind: url
              url: \"ftp://kamu.dev/test.zip\"
              cache:
                kind: forever
            prepare:
              - kind: decompress
                format: zip
                subPath: data_*.csv
            read:
              kind: csv
              header: true
            preprocess:
              kind: sql
              engine: spark
              query: SELECT * FROM input
            merge:
              kind: snapshot
              primaryKey:
                - id
          vocab:
            eventTimeColumn: date"
    );

    let expected = DatasetSnapshot {
        id: DatasetIDBuf::try_from("kamu.test").unwrap(),
        source: DatasetSource::Root(DatasetSourceRoot {
            fetch: FetchStep::Url(FetchStepUrl {
                url: "ftp://kamu.dev/test.zip".to_owned(),
                event_time: None,
                cache: Some(SourceCaching::Forever),
            }),
            prepare: Some(vec![PrepStep::Decompress(PrepStepDecompress {
                format: CompressionFormat::Zip,
                sub_path: Some("data_*.csv".to_owned()),
            })]),
            read: ReadStep::Csv(ReadStepCsv {
                schema: None,
                separator: None,
                encoding: None,
                quote: None,
                escape: None,
                comment: None,
                header: Some(true),
                enforce_schema: None,
                infer_schema: None,
                ignore_leading_white_space: None,
                ignore_trailing_white_space: None,
                null_value: None,
                empty_value: None,
                nan_value: None,
                positive_inf: None,
                negative_inf: None,
                date_format: None,
                timestamp_format: None,
                multi_line: None,
            }),
            preprocess: Some(Transform::Sql(TransformSql {
                engine: "spark".to_owned(),
                version: None,
                query: Some("SELECT * FROM input".to_owned()),
                queries: None,
                temporal_tables: None,
            })),
            merge: MergeStrategy::Snapshot(MergeStrategySnapshot {
                primary_key: vec!["id".to_owned()],
                compare_columns: None,
                observation_column: None,
                obsv_added: None,
                obsv_changed: None,
                obsv_removed: None,
            }),
        }),
        vocab: Some(DatasetVocabulary {
            system_time_column: None,
            event_time_column: Some("date".to_owned()),
        }),
    };

    let actual = YamlDatasetSnapshotDeserializer
        .read_manifest(data.as_bytes())
        .unwrap();

    assert_eq!(expected, actual);

    let data2 = YamlDatasetSnapshotSerializer
        .write_manifest(&actual)
        .unwrap();

    assert_eq!(data, std::str::from_utf8(&data2).unwrap());
}

#[test]
fn serde_dataset_snapshot_derivative() {
    let data = indoc!(
        "
        ---
        apiVersion: 1
        kind: DatasetSnapshot
        content:
          id: com.naturalearthdata.admin0
          source:
            kind: derivative
            inputs:
              - com.naturalearthdata.10m.admin0
              - com.naturalearthdata.50m.admin0
            transform:
              kind: sql
              engine: spark
              query: SOME_SQL"
    );

    let expected = DatasetSnapshot {
        id: DatasetIDBuf::try_from("com.naturalearthdata.admin0").unwrap(),
        source: DatasetSource::Derivative(DatasetSourceDerivative {
            inputs: vec![
                DatasetIDBuf::try_from("com.naturalearthdata.10m.admin0").unwrap(),
                DatasetIDBuf::try_from("com.naturalearthdata.50m.admin0").unwrap(),
            ],
            transform: Transform::Sql(TransformSql {
                engine: "spark".to_owned(),
                version: None,
                query: Some("SOME_SQL".to_owned()),
                queries: None,
                temporal_tables: None,
            }),
        }),
        vocab: None,
    };

    let actual = YamlDatasetSnapshotDeserializer
        .read_manifest(data.as_bytes())
        .unwrap();

    assert_eq!(expected, actual);

    let data2 = YamlDatasetSnapshotSerializer
        .write_manifest(&actual)
        .unwrap();

    assert_eq!(data, std::str::from_utf8(&data2).unwrap());
}

#[test]
fn serde_metadata_block() {
    let data = indoc!(
        "
        ---
        apiVersion: 1
        kind: MetadataBlock
        content:
          blockHash: 0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a
          prevBlockHash: 0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b
          systemTime: \"2020-01-01T12:00:00Z\"
          outputSlice:
            hash: 0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a
            interval: \"[2020-01-01T12:00:00Z, 2020-01-01T12:00:00Z]\"
            numRecords: 10
          outputWatermark: \"2020-01-01T12:00:00Z\"
          inputSlices:
            - hash: 0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a
              interval: \"(-inf, 2020-01-01T12:00:00Z]\"
              numRecords: 10
            - hash: 0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b
              interval: ()
              numRecords: 0
          source:
            kind: derivative
            inputs:
              - input1
              - input2
            transform:
              kind: sql
              engine: spark
              query: SELECT * FROM input1 UNION ALL SELECT * FROM input2
          vocab:
            eventTimeColumn: date"
    );

    let expected = MetadataBlock {
        block_hash: Sha3_256::new([0x0a; 32]),
        prev_block_hash: Some(Sha3_256::new([0x0b; 32])),
        system_time: Utc.ymd(2020, 1, 1).and_hms(12, 0, 0),
        source: Some(DatasetSource::Derivative(DatasetSourceDerivative {
            inputs: vec![
                DatasetIDBuf::try_from("input1").unwrap(),
                DatasetIDBuf::try_from("input2").unwrap(),
            ],
            transform: Transform::Sql(TransformSql {
                engine: "spark".to_owned(),
                version: None,
                query: Some("SELECT * FROM input1 UNION ALL SELECT * FROM input2".to_owned()),
                queries: None,
                temporal_tables: None,
            }),
        })),
        vocab: Some(DatasetVocabulary {
            system_time_column: None,
            event_time_column: Some("date".to_owned()),
        }),
        output_slice: Some(DataSlice {
            hash: Sha3_256::new([0x0a; 32]),
            interval: TimeInterval::singleton(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
            num_records: 10,
        }),
        output_watermark: Some(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
        input_slices: Some(vec![
            DataSlice {
                hash: Sha3_256::new([0x0a; 32]),
                interval: TimeInterval::unbounded_closed_right(
                    Utc.ymd(2020, 1, 1).and_hms(12, 0, 0),
                ),
                num_records: 10,
            },
            DataSlice {
                hash: Sha3_256::new([0x0b; 32]),
                interval: TimeInterval::empty(),
                num_records: 0,
            },
        ]),
    };

    let actual = YamlMetadataBlockDeserializer
        .read_manifest_unchecked(data.as_bytes())
        .unwrap();

    assert_eq!(expected, actual);

    let data2 = YamlMetadataBlockSerializer
        .write_manifest_unchecked(&actual)
        .unwrap();

    assert_eq!(data, std::str::from_utf8(&data2).unwrap());
}

#[test]
fn serde_metadata_block_hashes() {
    let expected = indoc!(
        "
        ---
        apiVersion: 1
        kind: MetadataBlock
        content:
          blockHash: 2421af4d7b82d03cc3807fb7b00d82b410efb31ef36b407868ae5ae137d14186
          prevBlockHash: 0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b
          systemTime: \"2020-01-01T12:00:00.123456789Z\"
          outputSlice:
            hash: 0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a
            interval: \"[2020-01-01T12:00:00Z, 2020-01-01T12:00:00Z]\"
            numRecords: 10
          outputWatermark: \"2020-01-01T12:00:00Z\"
          inputSlices:
            - hash: 0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a
              interval: \"(-inf, 2020-01-01T12:00:00Z]\"
              numRecords: 10
            - hash: 0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b
              interval: ()
              numRecords: 0
          source:
            kind: derivative
            inputs:
              - input1
              - input2
            transform:
              kind: sql
              engine: spark
              query: SELECT * FROM input1 UNION ALL SELECT * FROM input2
          vocab:
            eventTimeColumn: date"
    );

    let block = MetadataBlock {
        block_hash: Sha3_256::new([0; 32]),
        prev_block_hash: Some(Sha3_256::new([0x0b; 32])),
        system_time: Utc.ymd(2020, 1, 1).and_hms_nano(12, 0, 0, 123456789),
        source: Some(DatasetSource::Derivative(DatasetSourceDerivative {
            inputs: vec![
                DatasetIDBuf::try_from("input1").unwrap(),
                DatasetIDBuf::try_from("input2").unwrap(),
            ],
            transform: Transform::Sql(TransformSql {
                engine: "spark".to_owned(),
                version: None,
                query: Some("SELECT * FROM input1 UNION ALL SELECT * FROM input2".to_owned()),
                queries: None,
                temporal_tables: None,
            }),
        })),
        vocab: Some(DatasetVocabulary {
            system_time_column: None,
            event_time_column: Some("date".to_owned()),
        }),
        output_slice: Some(DataSlice {
            hash: Sha3_256::new([0x0a; 32]),
            interval: TimeInterval::singleton(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
            num_records: 10,
        }),
        output_watermark: Some(Utc.ymd(2020, 1, 1).and_hms(12, 0, 0)),
        input_slices: Some(vec![
            DataSlice {
                hash: Sha3_256::new([0x0a; 32]),
                interval: TimeInterval::unbounded_closed_right(
                    Utc.ymd(2020, 1, 1).and_hms(12, 0, 0),
                ),
                num_records: 10,
            },
            DataSlice {
                hash: Sha3_256::new([0x0b; 32]),
                interval: TimeInterval::empty(),
                num_records: 0,
            },
        ]),
    };

    let (_, actual) = YamlMetadataBlockSerializer.write_manifest(&block).unwrap();
    assert_eq!(expected, std::str::from_utf8(&actual).unwrap());

    let actual_block = YamlMetadataBlockDeserializer
        .read_manifest(&actual)
        .unwrap();
    assert_eq!(
        block,
        MetadataBlock {
            block_hash: Sha3_256::zero(),
            ..actual_block
        }
    );
}

#[test]
fn serde_fetch_step_files_glob() {
    let data = indoc!(
        "
        ---
        kind: filesGlob
        path: /opt/x/*.txt
        cache:
          kind: forever
        order: byName"
    );

    #[derive(Serialize, Deserialize)]
    struct Helper(#[serde(with = "FetchStepDef")] FetchStep);
    let hlp: Helper = serde_yaml::from_str(data).unwrap();
    let actual = hlp.0;

    let expected = FetchStep::FilesGlob(FetchStepFilesGlob {
        path: "/opt/x/*.txt".to_owned(),
        event_time: None,
        cache: Some(SourceCaching::Forever),
        order: Some(SourceOrdering::ByName),
    });

    assert_eq!(expected, actual);

    assert_eq!(
        serde_yaml::to_string(&Helper(actual)).unwrap(),
        indoc!(
            "
            ---
            kind: filesGlob
            path: /opt/x/*.txt
            cache:
              kind: forever
            order: byName"
        )
    );
}

#[test]
fn serde_transform() {
    let data = indoc!(
        "
        ---
        kind: sql
        engine: flink
        temporalTables:
        - id: foo
          primaryKey:
          - id
        queries:
        - alias: bar
          query: >
            SELECT * FROM foo"
    );

    #[derive(Serialize, Deserialize)]
    struct Helper(#[serde(with = "TransformDef")] Transform);
    let hlp: Helper = serde_yaml::from_str(data).unwrap();
    let actual = hlp.0;

    let expected = Transform::Sql(TransformSql {
        engine: "flink".to_owned(),
        version: None,
        query: None,
        temporal_tables: Some(vec![TemporalTable {
            id: "foo".to_owned(),
            primary_key: vec!["id".to_owned()],
        }]),
        queries: Some(vec![SqlQueryStep {
            alias: Some("bar".to_owned()),
            query: "SELECT * FROM foo".to_owned(),
        }]),
    });

    assert_eq!(expected, actual);

    assert_eq!(
        serde_yaml::to_string(&Helper(actual)).unwrap(),
        indoc!(
            "
            ---
            kind: sql
            engine: flink
            queries:
              - alias: bar
                query: SELECT * FROM foo
            temporalTables:
              - id: foo
                primaryKey:
                  - id"
        )
    );
}
