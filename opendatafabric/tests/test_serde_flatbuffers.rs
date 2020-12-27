use opendatafabric::serde::flatbuffers::*;
use opendatafabric::*;

use chrono::prelude::*;
use std::convert::TryFrom;

///////////////////////////////////////////////////////////////////////////////

fn get_block_root() -> MetadataBlock {
    MetadataBlock {
        block_hash: Sha3_256::zero(),
        prev_block_hash: Some(Sha3_256::new([0x0b; 32])),
        system_time: Utc.ymd(2020, 1, 1).and_hms(12, 0, 0),
        input_slices: None,
        output_slice: None,
        output_watermark: None,
        source: Some(DatasetSource::Root(DatasetSourceRoot {
            fetch: FetchStep::FilesGlob(FetchStepFilesGlob {
                path: "./*.csv".to_owned(),
                event_time: Some(EventTimeSource::FromMetadata),
                cache: Some(SourceCaching::Forever),
                order: Some(SourceOrdering::ByName),
            }),
            prepare: Some(vec![PrepStep::Decompress(PrepStepDecompress {
                format: CompressionFormat::Gzip,
                sub_path: None,
            })]),
            read: ReadStep::GeoJson(ReadStepGeoJson {
                schema: Some(vec!["a: INT".to_owned(), "b: INT".to_owned()]),
            }),
            preprocess: Some(Transform::Sql(TransformSql {
                engine: "spark".to_owned(),
                version: Some("1.0.0".to_owned()),
                query: Some("SELECT * FROM input".to_owned()),
                queries: None,
                temporal_tables: None,
            })),
            merge: MergeStrategy::Ledger(MergeStrategyLedger {
                primary_key: vec!["a".to_owned()],
            }),
        })),
        vocab: Some(DatasetVocabulary {
            system_time_column: None,
            event_time_column: Some("date".to_owned()),
        }),
    }
}

fn get_block_deriv() -> MetadataBlock {
    MetadataBlock {
        block_hash: Sha3_256::zero(),
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
    }
}

///////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_metadata_block_root() {
    let expected = get_block_root();

    let buffer = FlatbuffersMetadataBlockSerializer
        .write_manifest_unchecked(&expected)
        .unwrap();
    let actual = FlatbuffersMetadataBlockDeserializer
        .read_manifest_unchecked(&buffer)
        .unwrap();
    assert_eq!(expected, actual);

    // Ensure produces same binary result
    let buffer2 = FlatbuffersMetadataBlockSerializer
        .write_manifest_unchecked(&actual)
        .unwrap();
    assert_eq!(buffer.inner(), buffer2.inner());
}

#[test]
fn serde_metadata_block_deriv() {
    let expected = get_block_deriv();

    let buffer = FlatbuffersMetadataBlockSerializer
        .write_manifest_unchecked(&expected)
        .unwrap();
    let actual = FlatbuffersMetadataBlockDeserializer
        .read_manifest_unchecked(&buffer)
        .unwrap();
    assert_eq!(expected, actual);

    // Ensure produces same binary result
    let buffer2 = FlatbuffersMetadataBlockSerializer
        .write_manifest_unchecked(&actual)
        .unwrap();
    assert_eq!(buffer.inner(), buffer2.inner());
}

#[test]
fn serializer_hashes_are_stable_root() {
    let block = get_block_root();

    let (block_hash, _) = FlatbuffersMetadataBlockSerializer
        .write_manifest(&block)
        .unwrap();

    assert_eq!(
        block_hash,
        Sha3_256::try_from("f64cac020bc6c15f6d818296e851b7ad2c3da9407220a1741b2d1818d44ddca1")
            .unwrap()
    );
}

#[test]
fn serializer_hashes_are_stable_deriv() {
    let block = get_block_deriv();

    let (block_hash, _) = FlatbuffersMetadataBlockSerializer
        .write_manifest(&block)
        .unwrap();

    assert_eq!(
        block_hash,
        Sha3_256::try_from("34e8caff978659f26c6d9e879e3d8ad9e122c20140b5df61b0183329d0d5c41a")
            .unwrap()
    );
}

#[test]
fn serializer_rejects_incorrect_hashes() {
    let invalid = MetadataBlock {
        block_hash: Sha3_256::new([0xab; 32]),
        ..get_block_root()
    };

    assert!(matches!(
        FlatbuffersMetadataBlockSerializer.write_manifest(&invalid),
        Err(opendatafabric::serde::Error::InvalidHash { .. })
    ));
}

#[test]
fn deserializer_rejects_incorrect_hashes() {
    let invalid = MetadataBlock {
        block_hash: Sha3_256::new([0xab; 32]),
        ..get_block_root()
    };

    let buf = FlatbuffersMetadataBlockSerializer
        .write_manifest_unchecked(&invalid)
        .unwrap();

    assert!(matches!(
        FlatbuffersMetadataBlockDeserializer.validate_manifest(&buf),
        Err(opendatafabric::serde::Error::InvalidHash { .. })
    ));

    assert!(matches!(
        FlatbuffersMetadataBlockDeserializer.read_manifest(&buf),
        Err(opendatafabric::serde::Error::InvalidHash { .. })
    ));
}
