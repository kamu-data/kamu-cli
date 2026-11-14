// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use chrono::prelude::*;
use indoc::indoc;
use opendatafabric_metadata::data::ext::*;
use opendatafabric_metadata::data::*;
use opendatafabric_metadata::datasets::*;
use opendatafabric_metadata::serde::{yaml as serde, *};
use opendatafabric_metadata::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn serde_set_data_schema() {
    let expected_schema = DataSchema::builder()
        .extend(vec![
            DataField::string("city").encoding(ArrowBufferEncoding::View {
                offset_bit_width: Some(32),
            }),
            DataField::u64("population"),
            DataField::string("census")
                .optional()
                .extra(DataTypeExt::object_link(DataTypeExt::multihash())),
            DataField::list("links", DataType::string()),
        ])
        .extra(DatasetArchetype::Collection)
        .build()
        .unwrap();

    let event: MetadataEvent = SetDataSchema::new(expected_schema.clone()).into();

    let expected_block = MetadataBlock {
        sequence_number: 123,
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        system_time: Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap(),
        event,
    };

    let actual_data = serde::YamlMetadataBlockSerializer
        .write_manifest(&expected_block)
        .unwrap();

    let expected_data = indoc!(
        r#"
        kind: MetadataBlock
        version: 3
        content:
          systemTime: 2020-01-01T12:00:00Z
          prevBlockHash: f16209eb949bd8ff0bb1d827f11809aebc6bd0d5955c7f368469a913c70d196620272
          sequenceNumber: 123
          event:
            kind: SetDataSchema
            schema:
              fields:
              - name: city
                type:
                  kind: String
                extra:
                  arrow.apache.org/bufferEncoding:
                    kind: View
                    offsetBitWidth: 32
              - name: population
                type:
                  kind: UInt64
              - name: census
                type:
                  kind: Option
                  inner:
                    kind: String
                extra:
                  opendatafabric.org/type:
                    kind: ObjectLink
                    linkType:
                      kind: Multihash
              - name: links
                type:
                  kind: List
                  itemType:
                    kind: String
              extra:
                kamu.dev/archetype: Collection
        "#
    );

    pretty_assertions::assert_eq!(expected_data, std::str::from_utf8(&actual_data).unwrap());

    let actual_block = serde::YamlMetadataBlockDeserializer
        .read_manifest(&actual_data)
        .unwrap();

    assert_eq!(expected_block, actual_block);

    let actual_schema = actual_block
        .event
        .as_variant::<SetDataSchema>()
        .unwrap()
        .schema
        .clone()
        .unwrap();

    assert_eq!(expected_schema, actual_schema);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "arrow")]
#[test]
#[expect(
    deprecated,
    reason = "exercises the deprecated legacy format on purpose"
)]
fn serde_set_data_schema_legacy() {
    use arrow::datatypes::*;

    let expected_schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Boolean, false),
    ]);

    let event: MetadataEvent = SetDataSchema::new_legacy_raw_arrow(&expected_schema).into();

    let expected_block = MetadataBlock {
        sequence_number: 123,
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        system_time: Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap(),
        event,
    };

    let actual_data = serde::YamlMetadataBlockSerializer
        .write_manifest(&expected_block)
        .unwrap();

    let expected_data = indoc!(
        r#"
        kind: MetadataBlock
        version: 3
        content:
          systemTime: 2020-01-01T12:00:00Z
          prevBlockHash: f16209eb949bd8ff0bb1d827f11809aebc6bd0d5955c7f368469a913c70d196620272
          sequenceNumber: 123
          event:
            kind: SetDataSchema
            rawArrowSchema: DAAAAAgACAAAAAQACAAAAAQAAAACAAAAQAAAAAQAAADY////GAAAAAwAAAAAAAAGEAAAAAAAAAAEAAQABAAAAAEAAABiAAAAEAAUABAAAAAPAAQAAAAIABAAAAAYAAAAIAAAAAAAAAIcAAAACAAMAAQACwAIAAAAQAAAAAAAAAEAAAAAAQAAAGEAAAA=
        "#
    );

    assert_eq!(expected_data, std::str::from_utf8(&actual_data).unwrap());

    let actual_block = serde::YamlMetadataBlockDeserializer
        .read_manifest(&actual_data)
        .unwrap();

    assert_eq!(expected_block, actual_block);

    let actual_schema = actual_block
        .event
        .as_variant::<SetDataSchema>()
        .unwrap()
        .schema_as_arrow(&ToArrowSettings::default())
        .unwrap();

    assert_eq!(expected_schema, actual_schema);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[ignore = "Requires improving validation. See https://github.com/open-data-fabric/open-data-fabric/issues/112"]
#[test]
fn serde_set_data_schema_validation() {
    // Valid
    assert_matches!(
        serde::YamlMetadataEventDeserializer.read_manifest(indoc!(
            br#"
            kind: MetadataEvent
            version: 1
            content:
              kind: SetDataSchema
              schema:
                fields:
                - name: city
                  type:
                    kind: String
            "#
        )),
        Ok(_)
    );

    // Invalid bit width
    assert_matches!(
        serde::YamlMetadataEventDeserializer.read_manifest(indoc!(
            br#"
            kind: MetadataEvent
            version: 1
            content:
              kind: SetDataSchema
              schema:
                fields:
                - name: population
                  type:
                    kind: Int
                    bitWidth: 46
                    signed: false
            "#
        )),
        Ok(_)
    );

    // Invalid arrow encoding type
    assert_matches!(
        serde::YamlMetadataEventDeserializer.read_manifest(indoc!(
            br#"
            kind: MetadataEvent
            version: 1
            content:
              kind: SetDataSchema
              schema:
                fields:
                - name: city
                  type:
                    kind: String
                  extra:
                    arrow.apache.org/encoding: blerb
                    arrow.apache.org/offsetBitWidth: 32
            "#
        )),
        Err(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
