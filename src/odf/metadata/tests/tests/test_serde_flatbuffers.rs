// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use chrono::prelude::*;
use digest::Digest;
use opendatafabric_metadata::config::*;
use opendatafabric_metadata::data::ext::*;
use opendatafabric_metadata::data::*;
use opendatafabric_metadata::dataset::*;
use opendatafabric_metadata::engine::*;
use opendatafabric_metadata::legacy::*;
use opendatafabric_metadata::resource::*;
use opendatafabric_metadata::serde::flatbuffers::{proxies_generated as fb, *};
use opendatafabric_metadata::serde::*;
use opendatafabric_metadata::source::*;
use opendatafabric_metadata::*;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_flatbuffers_maps() {
    // String -> Struct
    let expected = Secrets {
        entries: BTreeMap::from_iter([
            (
                "password".to_string(),
                Secret {
                    value: "swordfish".into(),
                    content_encoding: None,
                },
            ),
            (
                "tls".to_string(),
                Secret {
                    value: "aabbcc".into(),
                    content_encoding: Some("base64".into()),
                },
            ),
        ]),
    };

    let mut fb = ::flatbuffers::FlatBufferBuilder::new();
    let offset = expected.serialize(&mut fb);
    fb.finish(offset, None);
    let data = fb.finished_data();

    let actual = Secrets::deserialize(::flatbuffers::root::<fb::Secrets>(data).unwrap());

    pretty_assertions::assert_eq!(expected, actual);

    // TypeRef -> AnyJson
    let expected = ResourceLabels {
        entries: BTreeMap::from_iter([
            ("string".parse().unwrap(), json!("foo")),
            ("nested".parse().unwrap(), json!({"a": "x", "b": "y"})),
            ("https://example.com/v1/X".parse().unwrap(), json!("bar")),
        ]),
    };

    let mut fb = ::flatbuffers::FlatBufferBuilder::new();
    let offset = expected.serialize(&mut fb);
    fb.finish(offset, None);
    let data = fb.finished_data();

    let actual =
        ResourceLabels::deserialize(::flatbuffers::root::<fb::ResourceLabels>(data).unwrap());

    pretty_assertions::assert_eq!(expected, actual);

    // String -> AnyJson (json-encoded-string)
    let expected = ExtraAttributes {
        entries: BTreeMap::from_iter([
            ("arrow.apache.org/offsetBitWidth".to_string(), json!(32)),
            (
                "opendatafabric.org/description".to_string(),
                json!("foobar"),
            ),
            (
                "opendatafabric.org/type".to_string(),
                json!({
                    "kind": "ObjectLink",
                    "linkType": { "kind": "Multihash" }
                }),
            ),
        ]),
    };

    let mut fb = ::flatbuffers::FlatBufferBuilder::new();
    let offset = expected.serialize(&mut fb);
    fb.finish(offset, None);
    let data = fb.finished_data();

    let actual =
        ExtraAttributes::deserialize(::flatbuffers::root::<fb::ExtraAttributes>(data).unwrap());

    pretty_assertions::assert_eq!(expected, actual);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_flatbuffers_any_json_property() {
    // String
    let expected = auth::AttributeInput {
        object: "X:x".parse().unwrap(),
        name: "my-attr".to_string(),
        value: json!("some-attr"),
    };

    let mut fb = ::flatbuffers::FlatBufferBuilder::new();
    let offset = expected.serialize(&mut fb);
    fb.finish(offset, None);
    let data = fb.finished_data();

    let actual =
        auth::AttributeInput::deserialize(::flatbuffers::root::<fb::AttributeInput>(data).unwrap());
    pretty_assertions::assert_eq!(expected, actual);

    // Int
    let expected = auth::AttributeInput {
        object: "X:x".parse().unwrap(),
        name: "my-attr".to_string(),
        value: json!(123),
    };

    let mut fb = ::flatbuffers::FlatBufferBuilder::new();
    let offset = expected.serialize(&mut fb);
    fb.finish(offset, None);
    let data = fb.finished_data();

    let actual =
        auth::AttributeInput::deserialize(::flatbuffers::root::<fb::AttributeInput>(data).unwrap());
    pretty_assertions::assert_eq!(expected, actual);

    // Nested
    let expected = auth::AttributeInput {
        object: "X:x".parse().unwrap(),
        name: "my-attr".to_string(),
        value: json!({"a": "x", "b": "y"}),
    };

    let mut fb = ::flatbuffers::FlatBufferBuilder::new();
    let offset = expected.serialize(&mut fb);
    fb.finish(offset, None);
    let data = fb.finished_data();

    let actual =
        auth::AttributeInput::deserialize(::flatbuffers::root::<fb::AttributeInput>(data).unwrap());
    pretty_assertions::assert_eq!(expected, actual);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_test_events() -> [(MetadataEvent, &'static str); 10] {
    [
        (
            MetadataEvent::AddData(AddData {
                prev_checkpoint: None,
                prev_offset: None,
                new_data: None,
                new_checkpoint: None,
                new_watermark: None,
                new_source_state: None,
                extra: None,
            }),
            "a93874702479f83ddff1e7a660ca78085def333c86501b0d895fa3aec79c7189",
        ),
        (
            MetadataEvent::AddData(AddData {
                prev_checkpoint: Some(Multihash::from_digest_sha3_256(b"prev")),
                prev_offset: Some(9),
                new_data: None,
                new_checkpoint: None,
                new_watermark: None,
                new_source_state: None,
                extra: None,
            }),
            "86aeb7dee0502b4886761de5e45b5111c0bc9e0fbfee02e3685487109305ef56",
        ),
        (
            MetadataEvent::AddData(AddData {
                prev_checkpoint: Some(Multihash::from_digest_sha3_256(b"prev")),
                prev_offset: Some(9),
                new_data: None,
                new_checkpoint: None,
                new_watermark: Some(Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap()),
                new_source_state: None,
                extra: None,
            }),
            "d1b412dab2a6af53f3e1b183834557bbf1c4689e2fc9b92fc0d04741ef607ecc",
        ),
        (
            MetadataEvent::AddData(AddData {
                prev_checkpoint: Some(Multihash::from_digest_sha3_256(b"prev")),
                prev_offset: Some(9),
                new_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"logical"),
                    physical_hash: Multihash::from_digest_sha3_256(b"physical"),
                    offset_interval: OffsetInterval { start: 10, end: 99 },
                    size: 100,
                }),
                new_checkpoint: None,
                new_watermark: Some(Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap()),
                new_source_state: None,
                extra: None,
            }),
            "1fcf10cb843746c4c44e6c288e216995701aa073426fc4bebe14f1680f8ec6d1",
        ),
        (
            MetadataEvent::SetPollingSource(SetPollingSource {
                fetch: FetchStep::FilesGlob(FetchStepFilesGlob {
                    path: "./*.csv".to_owned(),
                    event_time: Some(EventTimeSource::FromMetadata(
                        EventTimeSourceFromMetadata {},
                    )),
                    cache: Some(SourceCaching::Forever(SourceCachingForever {})),
                    order: Some(SourceOrdering::ByName),
                }),
                prepare: Some(vec![PrepStep::Decompress(PrepStepDecompress {
                    format: CompressionFormat::Gzip,
                    sub_path: None,
                })]),
                read: ReadStep::GeoJson(ReadStepGeoJson {
                    ddl_schema: Some(vec!["a: INT".to_owned(), "b: INT".to_owned()]),
                    ..Default::default()
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
            }),
            "eb61498b7362d334131a8486a5ce4cc2638d7c1aa3e4da82a19cf834a77df718",
        ),
        (
            MetadataEvent::SetPollingSource(SetPollingSource {
                fetch: FetchStep::FilesGlob(FetchStepFilesGlob {
                    path: "./*.csv".to_owned(),
                    event_time: Some(EventTimeSource::FromMetadata(
                        EventTimeSourceFromMetadata {},
                    )),
                    cache: Some(SourceCaching::Forever(SourceCachingForever {})),
                    order: Some(SourceOrdering::ByName),
                }),
                prepare: Some(vec![PrepStep::Decompress(PrepStepDecompress {
                    format: CompressionFormat::Gzip,
                    sub_path: None,
                })]),
                read: ReadStep::GeoJson(ReadStepGeoJson {
                    schema: Some(DataSchema::new(vec![
                        DataField::i32("a"),
                        DataField::i32("b"),
                    ])),
                    ..Default::default()
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
            }),
            "f96c90e9e9700819ba05d87f66b200a18a08404706ac62cde6d68bc350e73861",
        ),
        (
            MetadataEvent::AddData(AddData {
                prev_checkpoint: Some(Multihash::from_digest_sha3_256(b"checkpoint")),
                prev_offset: Some(9),
                new_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"logical"),
                    physical_hash: Multihash::from_digest_sha3_256(b"physical"),
                    offset_interval: OffsetInterval { start: 10, end: 99 },
                    size: 100,
                }),
                new_checkpoint: None,
                new_watermark: None,
                new_source_state: Some(SourceState {
                    source_name: "push-source-1".to_owned(),
                    kind: "kamu/kafka-offset".to_owned(),
                    value: "SOME_OFFSET".to_owned(),
                }),
                extra: None,
            }),
            "9326dc5daab04877a9f006ff6b1636a8fa9dcec9615b54f741a1a90091eaf694",
        ),
        (
            MetadataEvent::AddData(AddData {
                prev_checkpoint: Some(Multihash::from_digest_sha3_256(b"checkpoint")),
                prev_offset: Some(9),
                new_data: Some(DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"logical"),
                    physical_hash: Multihash::from_digest_sha3_256(b"physical"),
                    offset_interval: OffsetInterval { start: 10, end: 99 },
                    size: 100,
                }),
                new_checkpoint: None,
                new_watermark: None,
                new_source_state: Some(SourceState {
                    source_name: "push-source-1".to_owned(),
                    kind: "kamu/kafka-offset".to_owned(),
                    value: "SOME_OFFSET".to_owned(),
                }),
                extra: Some(ExtraAttributes::new().with(LinkedObjectsSummary {
                    num_objects_naive: 10,
                    size_naive: 100,
                })),
            }),
            "54f64c642cca6f67416ebe2b29e5d9414ece91f819efe97cd437fbe542c61bbb",
        ),
        (
            MetadataEvent::SetTransform(SetTransform {
                inputs: vec![
                    TransformInput {
                        dataset_ref: DatasetID::new_seeded_ed25519(b"input1").into(),
                        alias: Some("input1".to_string()),
                    },
                    TransformInput {
                        dataset_ref: DatasetName::try_from("input2").unwrap().into(),
                        alias: Some("input2".to_string()),
                    },
                ],
                transform: Transform::Sql(TransformSql {
                    engine: "spark".to_owned(),
                    version: None,
                    query: Some("SELECT * FROM input1 UNION ALL SELECT * FROM input2".to_owned()),
                    queries: None,
                    temporal_tables: None,
                }),
            }),
            "bb6aa1f9689b794c75affa709ae2477b209411d9ad092c2318ecd9ea94121bcc",
        ),
        (
            MetadataEvent::ExecuteTransform(ExecuteTransform {
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
            "21cab7396525e614beeafe21c05a38bf597a39a191cc14147b9c5ec32210cf82",
        ),
    ]
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_SEQUENCE_NUMBER: u64 = 117;

fn wrap_into_block(event: MetadataEvent) -> MetadataBlock {
    MetadataBlock {
        system_time: Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap(),
        prev_block_hash: Some(Multihash::from_digest_sha3_256(b"prev")),
        sequence_number: TEST_SEQUENCE_NUMBER,
        event,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_metadata_block() {
    for (event, _) in get_test_events() {
        let expected = wrap_into_block(event);

        let buffer = FlatbuffersMetadataBlockSerializer
            .write_manifest(&expected)
            .unwrap();

        let actual = FlatbuffersMetadataBlockDeserializer
            .read_manifest(&buffer)
            .unwrap();

        assert_eq!(expected, actual);
    }
}

#[test]
fn test_serializer_stability() {
    for (i, (event, hash_expected)) in get_test_events().into_iter().enumerate() {
        let buffer = FlatbuffersMetadataBlockSerializer
            .write_manifest(&wrap_into_block(event))
            .unwrap();

        let hash_actual = format!("{:x}", sha3::Sha3_256::digest(&buffer));

        assert_eq!(hash_actual, hash_expected, "Case {i}");
    }
}

#[cfg(feature = "arrow")]
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

    let expected_block = wrap_into_block(event);

    let buffer = FlatbuffersMetadataBlockSerializer
        .write_manifest(&expected_block)
        .unwrap();

    let actual_block = FlatbuffersMetadataBlockDeserializer
        .read_manifest(&buffer)
        .unwrap();

    assert_eq!(expected_block, actual_block);

    let hash_actual = format!("{:x}", sha3::Sha3_256::digest(&buffer));
    let hash_expected = "80865248519d55fa04598e24ba42884728efe70956e640dca4df5ec8043c69ca";

    assert_eq!(hash_actual, hash_expected);

    let actual_schema = actual_block
        .event
        .as_variant::<SetDataSchema>()
        .unwrap()
        .schema
        .clone()
        .unwrap();

    assert_eq!(expected_schema, actual_schema);
}

#[cfg(feature = "arrow")]
#[test]
#[expect(deprecated)]
fn serde_set_data_schema_legacy() {
    use arrow::datatypes::*;

    let expected_schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Boolean, false),
    ]);

    let event: MetadataEvent = SetDataSchema::new_legacy_raw_arrow(&expected_schema).into();

    let expected_block = wrap_into_block(event);

    let buffer = FlatbuffersMetadataBlockSerializer
        .write_manifest(&expected_block)
        .unwrap();

    let actual_block = FlatbuffersMetadataBlockDeserializer
        .read_manifest(&buffer)
        .unwrap();

    assert_eq!(expected_block, actual_block);

    let hash_actual = format!("{:x}", sha3::Sha3_256::digest(&buffer));
    let hash_expected = "a7e79d088ca7bac689f12d71ae64b93e7bab62a7db2fec83ea89e9bf90dd89ed";

    assert_eq!(hash_actual, hash_expected);

    let actual_schema = actual_block
        .event
        .as_variant::<SetDataSchema>()
        .unwrap()
        .schema_as_arrow(&ToArrowSettings::default())
        .unwrap();

    assert_eq!(expected_schema, actual_schema);
}

#[test]
fn serde_execute_transform_response() {
    let examples = [
        TransformResponse::Success(TransformResponseSuccess {
            new_offset_interval: Some(OffsetInterval { start: 0, end: 10 }),
            new_watermark: Some(Utc::now()),
        }),
        TransformResponse::InvalidQuery(TransformResponseInvalidQuery {
            message: "boop".to_owned(),
        }),
        TransformResponse::InternalError(TransformResponseInternalError {
            message: "boop".to_owned(),
            backtrace: Some("woop".to_owned()),
        }),
        TransformResponse::Progress(TransformResponseProgress {}),
    ];

    for expected in examples {
        let buf = FlatbuffersEngineProtocol
            .write_transform_response(&expected)
            .unwrap();
        let actual = FlatbuffersEngineProtocol
            .read_transform_response(&buf)
            .unwrap();
        assert_eq!(actual, expected);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `ResourceSelector`'s flatbuffers conversions are hand-written
// (`rust.dtoType`) and were `todo!()`. `ResourceRef` is generated, but its
// round-trip goes through `ResourceID::as_bytes`/`from_bytes` — stubbed too.
#[test]
fn test_flatbuffers_resource_selectors() {
    fn account_ref() -> opendatafabric_metadata::auth::AccountRef {
        opendatafabric_metadata::auth::AccountRef {
            id: None,
            did: None,
            name: Some("alice".parse().unwrap()),
        }
    }

    // ResourceRef — exercises `ResourceID::as_bytes` / `from_bytes`
    let expected = ResourceRef {
        account: Some(account_ref()),
        r#type: Some(TypeName::new_unchecked("SecretSet").into()),
        id: Some(ResourceID::new(uuid::Uuid::new_v4())),
        did: None,
        name: Some("my-secrets".parse().unwrap()),
    };

    let mut fb = ::flatbuffers::FlatBufferBuilder::new();
    let offset = expected.serialize(&mut fb);
    fb.finish(offset, None);
    let data = fb.finished_data();

    let actual = ResourceRef::deserialize(::flatbuffers::root::<fb::ResourceRef>(data).unwrap());
    pretty_assertions::assert_eq!(expected, actual);

    // ResourceSelector — every field populated
    let expected = ResourceSelector {
        account: Some(account_ref()),
        id: Some(ResourceID::new(uuid::Uuid::new_v4())),
        did: None,
        r#type: Some(TypeName::new_unchecked("SecretSet").into()),
        name: Some("app-%".to_string()),
        labels: Some(LabelFilter {
            entries: BTreeMap::from_iter([("environment".to_string(), json!("production"))]),
        }),
    };

    let mut fb = ::flatbuffers::FlatBufferBuilder::new();
    let offset = expected.serialize(&mut fb);
    fb.finish(offset, None);
    let data = fb.finished_data();

    let actual =
        ResourceSelector::deserialize(::flatbuffers::root::<fb::ResourceSelector>(data).unwrap());
    pretty_assertions::assert_eq!(expected, actual);

    // ResourceSelector — a type-scoped selector and nothing else, so every
    // optional field must survive as `None` rather than materializing a default
    let expected = ResourceSelector {
        account: None,
        id: None,
        did: None,
        r#type: Some(TypeName::new_unchecked("VariableSet").into()),
        name: None,
        labels: None,
    };

    let mut fb = ::flatbuffers::FlatBufferBuilder::new();
    let offset = expected.serialize(&mut fb);
    fb.finish(offset, None);
    let data = fb.finished_data();

    let actual =
        ResourceSelector::deserialize(::flatbuffers::root::<fb::ResourceSelector>(data).unwrap());
    pretty_assertions::assert_eq!(expected, actual);

    // ResourceSelector — wholly empty, the "every resource of every type" form.
    // `type` is optional since ODF adopted the type-less selector, so an absent
    // one must round-trip as `None` instead of panicking on deserialize.
    let expected = ResourceSelector::default();

    let mut fb = ::flatbuffers::FlatBufferBuilder::new();
    let offset = expected.serialize(&mut fb);
    fb.finish(offset, None);
    let data = fb.finished_data();

    let actual =
        ResourceSelector::deserialize(::flatbuffers::root::<fb::ResourceSelector>(data).unwrap());
    pretty_assertions::assert_eq!(expected, actual);
}
