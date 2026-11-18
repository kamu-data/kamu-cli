// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use indoc::indoc;
use opendatafabric_metadata::datasets::*;
use opendatafabric_metadata::legacy::*;
use opendatafabric_metadata::serde::yaml::IntoDto;
use opendatafabric_metadata::serde::{yaml as serde, *};
use opendatafabric_metadata::sources::*;
use opendatafabric_metadata::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_metadata_block() {
    let block = MetadataBlock {
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        system_time: Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap(),
        event: MetadataEvent::ExecuteTransform(ExecuteTransform {
            query_inputs: vec![
                ExecuteTransformInput {
                    dataset_id: DatasetID::new_seeded_ed25519(b"input1"),
                    prev_block_hash: Some(Multihash::from_digest_sha3_256(b"a")),
                    new_block_hash: Some(Multihash::from_digest_sha3_256(b"b")),
                    prev_offset: Some(9),
                    new_offset: Some(20),
                },
                ExecuteTransformInput {
                    dataset_id: DatasetID::new_seeded_ed25519(b"input2"),
                    prev_block_hash: Some(Multihash::from_digest_sha3_256(b"a")),
                    new_block_hash: Some(Multihash::from_digest_sha3_256(b"b")),
                    prev_offset: None,
                    new_offset: None,
                },
            ],
            prev_checkpoint: Some(Multihash::from_digest_sha3_256(b"checkpoint")),
            prev_offset: Some(9),
            new_data: Some(DataSlice {
                logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                offset_interval: OffsetInterval { start: 10, end: 19 },
                size: 10,
            }),
            new_checkpoint: None,
            new_watermark: Some(Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap()),
        }),
        sequence_number: 127,
    };

    let expected = indoc!(
        "
        kind: MetadataBlock
        version: 3
        content:
          systemTime: 2020-01-01T12:00:00Z
          prevBlockHash: f16209eb949bd8ff0bb1d827f11809aebc6bd0d5955c7f368469a913c70d196620272
          sequenceNumber: 127
          event:
            kind: ExecuteTransform
            queryInputs:
            - datasetId: \
         did:odf:fed01816ef0a9abe93aba816ef0a9abe93aba90e6065747170300c0d3d30c2cd8d7a4
              prevBlockHash: f162080084bf2fba02475726feb2cab2d8215eab14bc6bdd8bfb2c8151257032ecd8b
              newBlockHash: f1620b039179a8a4ce2c252aa6f2f25798251c19b75fc1508d9d511a191e0487d64a7
              prevOffset: 9
              newOffset: 20
            - datasetId: \
         did:odf:fed01826ef0a9abea3a3a826ef0a9abea3a3a90e6065747270300c1d3b30a2cd9d724
              prevBlockHash: f162080084bf2fba02475726feb2cab2d8215eab14bc6bdd8bfb2c8151257032ecd8b
              newBlockHash: f1620b039179a8a4ce2c252aa6f2f25798251c19b75fc1508d9d511a191e0487d64a7
            prevCheckpoint: f1620894b355f91b194f31c1116dcec200b68d5cfd90a3df19032829fa643bc711fcb
            prevOffset: 9
            newData:
              logicalHash: f162076d3bc41c9f588f7fcd0d5bf4718f8f84b1c41b20882703100b9eb9413807c01
              physicalHash: f1620cceefd7e0545bcf8b6d19f3b5750c8a3ee8350418877bc6fb12e32de28137355
              offsetInterval:
                start: 10
                end: 19
              size: 10
            newWatermark: 2020-01-01T12:00:00Z
        "
    );

    let actual = serde::YamlMetadataBlockSerializer
        .write_manifest(&block)
        .unwrap();

    assert_eq!(expected, std::str::from_utf8(&actual).unwrap());

    let de_block = serde::YamlMetadataBlockDeserializer
        .read_manifest(expected.as_bytes())
        .unwrap();

    assert_eq!(block, de_block);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_metadata_block_obsolete_version() {
    let data = indoc!(
        "
        kind: MetadataBlock
        version: 1
        content:
          systemTime: 2020-01-01T12:00:00Z
          prevBlockHash: f16209eb949bd8ff0bb1d827f11809aebc6bd0d5955c7f368469a913c70d196620272
          sequenceNumber: 127
          event:
            kind: ExecuteTransform
            queryInputs:
            - datasetId: \
         did:odf:fed01816ef0a9abe93aba816ef0a9abe93aba90e6065747170300c0d3d30c2cd8d7a4
              prevBlockHash: f162080084bf2fba02475726feb2cab2d8215eab14bc6bdd8bfb2c8151257032ecd8b
              newBlockHash: f1620b039179a8a4ce2c252aa6f2f25798251c19b75fc1508d9d511a191e0487d64a7
              prevOffset: 9
              newOffset: 20
            - datasetId: \
         did:odf:fed01826ef0a9abea3a3a826ef0a9abea3a3a90e6065747270300c1d3b30a2cd9d724
              prevBlockHash: f162080084bf2fba02475726feb2cab2d8215eab14bc6bdd8bfb2c8151257032ecd8b
              newBlockHash: f1620b039179a8a4ce2c252aa6f2f25798251c19b75fc1508d9d511a191e0487d64a7
            prevCheckpoint: f1620894b355f91b194f31c1116dcec200b68d5cfd90a3df19032829fa643bc711fcb
            prevOffset: 9
            newData:
              logicalHash: f162076d3bc41c9f588f7fcd0d5bf4718f8f84b1c41b20882703100b9eb9413807c01
              physicalHash: f1620cceefd7e0545bcf8b6d19f3b5750c8a3ee8350418877bc6fb12e32de28137355
              offsetInterval:
                start: 10
                end: 19
              size: 10
            newWatermark: 2020-01-01T12:00:00Z
        "
    );

    let expected_error = Error::UnsupportedVersion(UnsupportedVersionError {
        manifest_version: 1,
        supported_version_range: METADATA_BLOCK_SUPPORTED_VERSION_RANGE,
    });

    let actual_error = serde::YamlMetadataBlockDeserializer
        .read_manifest(data.as_bytes())
        .unwrap_err();

    assert_eq!(expected_error.to_string(), actual_error.to_string());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_fetch_step_files_glob() {
    let data = indoc!(
        "
        kind: FilesGlob
        path: /opt/x/*.txt
        cache:
          kind: Forever
        order: ByName
        "
    );

    let actual = serde_yaml::from_str::<serde::legacy::FetchStep>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    let expected = FetchStep::FilesGlob(FetchStepFilesGlob {
        path: "/opt/x/*.txt".to_owned(),
        event_time: None,
        cache: Some(SourceCaching::Forever(SourceCachingForever {})),
        order: Some(SourceOrdering::ByName),
    });

    assert_eq!(expected, actual);

    assert_eq!(
        serde_yaml::to_string(&serde::legacy::FetchStep::from(actual)).unwrap(),
        data
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_transform() {
    let data = indoc!(
        "
        kind: Sql
        engine: flink
        queries:
        - alias: bar
          query: SELECT * FROM foo
        temporalTables:
        - name: foo
          primaryKey:
          - id
        "
    );

    let actual = serde_yaml::from_str::<serde::datasets::Transform>(data)
        .unwrap()
        .into_dto()
        .unwrap();

    let expected = Transform::Sql(TransformSql {
        engine: "flink".to_owned(),
        version: None,
        query: None,
        temporal_tables: Some(vec![TemporalTable {
            name: "foo".to_owned(),
            primary_key: vec!["id".to_owned()],
        }]),
        queries: Some(vec![SqlQueryStep {
            alias: Some("bar".to_owned()),
            query: "SELECT * FROM foo".to_owned(),
        }]),
    });

    assert_eq!(expected, actual);

    assert_eq!(
        serde_yaml::to_string(&serde::datasets::Transform::from(actual)).unwrap(),
        data
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
