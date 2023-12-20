// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use opendatafabric::*;

use super::IDFactory;

pub struct MetadataFactory;

impl MetadataFactory {
    pub fn seed(kind: DatasetKind) -> SeedBuilder {
        SeedBuilder::new(kind)
    }

    pub fn set_info() -> SetInfoBuilder {
        SetInfoBuilder::new()
    }

    pub fn transform() -> TransformSqlBuilder {
        TransformSqlBuilder::new()
    }

    pub fn set_polling_source() -> SetPollingSourceBuilder {
        SetPollingSourceBuilder::new()
    }

    pub fn add_push_source() -> AddPushSourceBuilder {
        AddPushSourceBuilder::new()
    }

    pub fn set_data_schema() -> SetDataSchemaBuilder {
        SetDataSchemaBuilder::new()
    }

    pub fn add_data() -> AddDataBuilder {
        AddDataBuilder::new()
    }

    pub fn execute_query() -> ExecuteQueryBuilder {
        ExecuteQueryBuilder::new()
    }

    pub fn set_transform<S, I>(inputs: I) -> SetTransformBuilder
    where
        I: IntoIterator<Item = S>,
        S: TryInto<DatasetName>,
        <S as TryInto<DatasetName>>::Error: std::fmt::Debug,
    {
        SetTransformBuilder::new(inputs.into_iter())
    }

    pub fn set_transform_aliases<A, I>(inputs: I) -> SetTransformBuilder
    where
        I: IntoIterator<Item = A>,
        A: TryInto<DatasetAlias>,
        <A as TryInto<DatasetAlias>>::Error: std::fmt::Debug,
    {
        SetTransformBuilder::from_aliases(inputs.into_iter())
    }

    pub fn set_transform_names_and_refs<N, R, I>(inputs: I) -> SetTransformBuilder
    where
        I: IntoIterator<Item = (N, R)>,
        N: TryInto<DatasetName>,
        <N as TryInto<DatasetName>>::Error: std::fmt::Debug,
        R: TryInto<DatasetRefAny>,
        <R as TryInto<DatasetRefAny>>::Error: std::fmt::Debug,
    {
        SetTransformBuilder::from_names_and_refs(inputs.into_iter())
    }

    pub fn metadata_block<E: Into<MetadataEvent>>(event: E) -> MetadataBlockBuilder<E> {
        MetadataBlockBuilder::new(event)
    }

    pub fn dataset_snapshot() -> DatasetSnapshotBuilder {
        DatasetSnapshotBuilder::new()
    }
}

///////////////////////////////////////////////////////////////////////////////
// Seed Builder
///////////////////////////////////////////////////////////////////////////////

pub struct SeedBuilder {
    v: Seed,
}

impl SeedBuilder {
    fn new(kind: DatasetKind) -> Self {
        Self {
            v: Seed {
                dataset_id: DatasetID::new_seeded_ed25519(b""),
                dataset_kind: kind,
            },
        }
    }

    pub fn id_random(mut self) -> Self {
        self.v.dataset_id = DatasetID::new_generated_ed25519().1;
        self
    }

    pub fn id_from<B: AsRef<[u8]>>(mut self, key: B) -> Self {
        self.v.dataset_id = DatasetID::new_seeded_ed25519(key.as_ref());
        self
    }

    pub fn build(self) -> Seed {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// SetInfo Builder
///////////////////////////////////////////////////////////////////////////////

pub struct SetInfoBuilder {
    v: SetInfo,
}

impl SetInfoBuilder {
    fn new() -> Self {
        Self {
            v: SetInfo {
                description: None,
                keywords: None,
            },
        }
    }

    pub fn description(mut self, description: &str) -> Self {
        self.v.description = Some(String::from(description));
        self
    }

    pub fn keyword(mut self, keyword: &str) -> Self {
        if self.v.keywords.is_none() {
            self.v.keywords = Some(vec![String::from(keyword)]);
        } else {
            self.v
                .keywords
                .as_mut()
                .unwrap()
                .push(String::from(keyword));
        }
        self
    }

    pub fn build(self) -> SetInfo {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// Transform Builder
///////////////////////////////////////////////////////////////////////////////

pub struct TransformSqlBuilder {
    v: TransformSql,
}

impl TransformSqlBuilder {
    fn new() -> Self {
        Self {
            v: TransformSql {
                engine: "some_engine".to_owned(),
                version: None,
                query: None,
                queries: None,
                temporal_tables: None,
            },
        }
    }

    pub fn engine(mut self, engine: &str) -> Self {
        self.v.engine = engine.to_owned();
        self
    }

    pub fn query(mut self, query: &str) -> Self {
        self.v.query = Some(query.to_owned());
        self
    }

    pub fn build(self) -> Transform {
        Transform::Sql(self.v)
    }
}

///////////////////////////////////////////////////////////////////////////////
// SetPollingSourceBuilder
///////////////////////////////////////////////////////////////////////////////

pub struct SetPollingSourceBuilder {
    v: SetPollingSource,
}

impl SetPollingSourceBuilder {
    fn new() -> Self {
        Self {
            v: SetPollingSource {
                fetch: FetchStep::Url(FetchStepUrl {
                    url: "http://nowhere.org".to_owned(),
                    event_time: None,
                    cache: None,
                    headers: None,
                }),
                prepare: None,
                read: ReadStep::GeoJson(ReadStepGeoJson { schema: None }),
                preprocess: None,
                merge: MergeStrategy::Append(MergeStrategyAppend {}),
            },
        }
    }

    pub fn fetch(mut self, fetch_step: impl Into<FetchStep>) -> Self {
        self.v = SetPollingSource {
            fetch: fetch_step.into(),
            ..self.v
        };
        self
    }

    pub fn fetch_file(self, path: &Path) -> Self {
        self.fetch(FetchStep::Url(FetchStepUrl {
            url: url::Url::from_file_path(path).unwrap().as_str().to_owned(),
            event_time: Some(EventTimeSourceFromSystemTime {}.into()),
            cache: None,
            headers: None,
        }))
    }

    pub fn read(mut self, read_step: impl Into<ReadStep>) -> Self {
        self.v = SetPollingSource {
            read: read_step.into(),
            ..self.v
        };
        self
    }

    pub fn preprocess(mut self, preprocess_step: impl Into<Transform>) -> Self {
        self.v = SetPollingSource {
            preprocess: Some(preprocess_step.into()),
            ..self.v
        };
        self
    }

    pub fn merge(mut self, merge_strategy: impl Into<MergeStrategy>) -> Self {
        self.v = SetPollingSource {
            merge: merge_strategy.into(),
            ..self.v
        };
        self
    }

    pub fn build(self) -> SetPollingSource {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// AddPushSourceBuilder
///////////////////////////////////////////////////////////////////////////////

pub struct AddPushSourceBuilder {
    v: AddPushSource,
}

impl AddPushSourceBuilder {
    fn new() -> Self {
        Self {
            v: AddPushSource {
                source_name: None,
                read: ReadStepNdJson {
                    schema: None,
                    ..Default::default()
                }
                .into(),
                preprocess: None,
                merge: MergeStrategy::Append(MergeStrategyAppend {}),
            },
        }
    }

    pub fn source_name(mut self, name: impl Into<String>) -> Self {
        self.v = AddPushSource {
            source_name: Some(name.into()),
            ..self.v
        };
        self
    }

    pub fn read(mut self, read_step: impl Into<ReadStep>) -> Self {
        self.v = AddPushSource {
            read: read_step.into(),
            ..self.v
        };
        self
    }

    pub fn some_read(self) -> Self {
        self.read(ReadStepNdJson {
            schema: Some(vec![
                "city STRING".to_string(),
                "population BIGINT".to_string(),
            ]),
            ..Default::default()
        })
    }

    pub fn preprocess(mut self, preprocess_step: impl Into<Transform>) -> Self {
        self.v = AddPushSource {
            preprocess: Some(preprocess_step.into()),
            ..self.v
        };
        self
    }

    pub fn merge(mut self, merge_strategy: impl Into<MergeStrategy>) -> Self {
        self.v = AddPushSource {
            merge: merge_strategy.into(),
            ..self.v
        };
        self
    }

    pub fn build(self) -> AddPushSource {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// SetTransformBuilder
///////////////////////////////////////////////////////////////////////////////

pub struct SetTransformBuilder {
    v: SetTransform,
}

impl SetTransformBuilder {
    fn new<S, I>(inputs: I) -> Self
    where
        I: Iterator<Item = S>,
        S: TryInto<DatasetName>,
        <S as TryInto<DatasetName>>::Error: std::fmt::Debug,
    {
        Self {
            v: SetTransform {
                inputs: inputs
                    .map(|s| TransformInput {
                        id: None,
                        name: s.try_into().unwrap(),
                        dataset_ref: None,
                    })
                    .collect(),
                transform: TransformSqlBuilder::new().build(),
            },
        }
    }

    fn from_aliases<A, I>(inputs: I) -> Self
    where
        I: Iterator<Item = A>,
        A: TryInto<DatasetAlias>,
        <A as TryInto<DatasetAlias>>::Error: std::fmt::Debug,
    {
        Self {
            v: SetTransform {
                inputs: inputs
                    .map(|a| {
                        let alias: DatasetAlias = a.try_into().unwrap();
                        TransformInput {
                            id: None,
                            name: alias.dataset_name.to_owned(),
                            dataset_ref: Some(alias.as_any_ref()),
                        }
                    })
                    .collect(),
                transform: TransformSqlBuilder::new().build(),
            },
        }
    }

    fn from_names_and_refs<N, R, I>(inputs: I) -> Self
    where
        I: Iterator<Item = (N, R)>,
        N: TryInto<DatasetName>,
        <N as TryInto<DatasetName>>::Error: std::fmt::Debug,
        R: TryInto<DatasetRefAny>,
        <R as TryInto<DatasetRefAny>>::Error: std::fmt::Debug,
    {
        Self {
            v: SetTransform {
                inputs: inputs
                    .map(|s| TransformInput {
                        id: None,
                        name: s.0.try_into().unwrap(),
                        dataset_ref: Some(s.1.try_into().unwrap()),
                    })
                    .collect(),
                transform: TransformSqlBuilder::new().build(),
            },
        }
    }

    pub fn input_ids_from_names(mut self) -> Self {
        for input in self.v.inputs.iter_mut() {
            input.id = Some(DatasetID::new_seeded_ed25519(input.name.as_bytes()));
        }
        self
    }

    pub fn set_dataset_ids(mut self, mut ids: HashMap<DatasetName, DatasetID>) -> Self {
        for input in self.v.inputs.iter_mut() {
            if let Some(input_id) = ids.remove(&input.name) {
                input.id = Some(input_id)
            }
        }
        self
    }

    pub fn transform(mut self, transform: Transform) -> Self {
        self.v.transform = transform;
        self
    }

    pub fn build(self) -> SetTransform {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// SetDataSchemaBuilder
///////////////////////////////////////////////////////////////////////////////

pub struct SetDataSchemaBuilder {
    v: SetDataSchema,
}

impl SetDataSchemaBuilder {
    pub fn new() -> Self {
        use datafusion::arrow::datatypes::*;
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("population", DataType::Int64, false),
        ]);
        Self {
            v: SetDataSchema::new(&schema),
        }
    }

    pub fn schema(mut self, schema: &datafusion::arrow::datatypes::Schema) -> Self {
        self.v = SetDataSchema::new(schema);
        self
    }

    pub fn build(self) -> SetDataSchema {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// AddDataBuilder
///////////////////////////////////////////////////////////////////////////////

pub struct AddDataBuilder {
    v: AddData,
}

impl AddDataBuilder {
    pub fn empty() -> Self {
        Self {
            v: AddData {
                input_checkpoint: None,
                output_data: None,
                output_checkpoint: None,
                output_watermark: None,
                source_state: None,
            },
        }
    }

    pub fn new() -> Self {
        Self::empty()
    }

    pub fn some_input_checkpoint(mut self) -> Self {
        if self.v.input_checkpoint.is_none() {
            self.v.input_checkpoint = Some(Multihash::from_digest_sha3_256(b"foo"));
        }
        self
    }

    pub fn input_checkpoint(mut self, checkpoint: Option<Multihash>) -> Self {
        self.v.input_checkpoint = checkpoint;
        self
    }

    pub fn some_output_data(self) -> Self {
        self.some_output_data_with_offset(0, 9)
    }

    pub fn some_output_data_with_offset(mut self, start: i64, end: i64) -> Self {
        if self.v.output_data.is_none() {
            self.v.output_data = Some(DataSlice {
                logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                interval: OffsetInterval { start, end },
                size: 0,
            });
        }
        self
    }

    pub fn some_output_checkpoint(mut self) -> Self {
        if self.v.output_checkpoint.is_none() {
            self.v.output_checkpoint = Some(Checkpoint {
                physical_hash: Multihash::from_digest_sha3_256(b"foo"),
                size: 1,
            });
        }
        self
    }

    pub fn output_checkpoint(mut self, checkpoint: Option<Checkpoint>) -> Self {
        self.v.output_checkpoint = checkpoint;
        self
    }

    pub fn some_output_watermark(mut self) -> Self {
        if self.v.output_watermark.is_none() {
            self.v.output_watermark = Some(Utc::now());
        }
        self
    }

    pub fn output_watermark(mut self, watermark: Option<DateTime<Utc>>) -> Self {
        self.v.output_watermark = watermark;
        self
    }

    pub fn some_source_state(mut self) -> Self {
        if self.v.source_state.is_none() {
            self.v.source_state = Some(SourceState {
                kind: SourceState::KIND_ETAG.to_owned(),
                source: SourceState::SOURCE_POLLING.to_owned(),
                value: "<etag>".to_owned(),
            });
        }
        self
    }

    pub fn source_state(mut self, source_state: Option<SourceState>) -> Self {
        self.v.source_state = source_state;
        self
    }

    pub fn data_physical_hash(mut self, hash: Multihash) -> Self {
        self = self.some_output_data();
        self.v.output_data.as_mut().unwrap().physical_hash = hash;
        self
    }

    pub fn interval(mut self, start: i64, end: i64) -> Self {
        self = self.some_output_data();
        self.v.output_data.as_mut().unwrap().interval = OffsetInterval { start, end };
        self
    }

    pub fn data_size(mut self, size: i64) -> Self {
        self = self.some_output_data();
        self.v.output_data.as_mut().unwrap().size = size;
        self
    }

    pub fn checkpoint_physical_hash(mut self, hash: Multihash) -> Self {
        self = self.some_output_checkpoint();
        self.v.output_checkpoint.as_mut().unwrap().physical_hash = hash;
        self
    }

    pub fn checkpoint_size(mut self, size: i64) -> Self {
        self = self.some_output_checkpoint();
        self.v.output_checkpoint.as_mut().unwrap().size = size;
        self
    }

    pub fn watermark(mut self, output_watermark: DateTime<Utc>) -> Self {
        self.v.output_watermark = Some(output_watermark);
        self
    }

    pub fn build(self) -> AddData {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// ExecuteQueryBuilder
///////////////////////////////////////////////////////////////////////////////

pub struct ExecuteQueryBuilder {
    v: ExecuteQuery,
}

impl ExecuteQueryBuilder {
    pub fn new() -> Self {
        Self {
            v: ExecuteQuery {
                input_slices: Vec::new(),
                input_checkpoint: None,
                output_data: None,
                output_checkpoint: None,
                output_watermark: None,
            },
        }
    }

    pub fn input_checkpoint(mut self, checkpoint: Option<Multihash>) -> Self {
        self.v.input_checkpoint = checkpoint;
        self
    }

    pub fn some_output_data(self) -> Self {
        self.some_output_data_with_offset(0, 9)
    }

    pub fn some_output_data_with_offset(mut self, start: i64, end: i64) -> Self {
        if self.v.output_data.is_none() {
            self.v.output_data = Some(DataSlice {
                logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                interval: OffsetInterval { start, end },
                size: 0,
            });
        }
        self
    }

    pub fn output_data(mut self, data_slice: Option<DataSlice>) -> Self {
        self.v.output_data = data_slice;
        self
    }

    pub fn some_output_checkpoint(mut self) -> Self {
        if self.v.output_checkpoint.is_none() {
            self.v.output_checkpoint = Some(Checkpoint {
                physical_hash: Multihash::from_digest_sha3_256(b"foo"),
                size: 1,
            });
        }
        self
    }

    pub fn output_checkpoint(mut self, checkpoint: Option<Checkpoint>) -> Self {
        self.v.output_checkpoint = checkpoint;
        self
    }

    pub fn some_output_watermark(mut self) -> Self {
        self.v.output_watermark = Some(Utc::now());
        self
    }

    pub fn output_watermark(mut self, watermark: Option<DateTime<Utc>>) -> Self {
        self.v.output_watermark = watermark;
        self
    }

    pub fn build(self) -> ExecuteQuery {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// MetadataBlock Builder
///////////////////////////////////////////////////////////////////////////////

pub struct MetadataBlockBuilder<E> {
    prev_block_hash: Option<Multihash>,
    system_time: DateTime<Utc>,
    sequence_number: i32,
    event: E,
}

impl<E> MetadataBlockBuilder<E>
where
    E: Into<MetadataEvent>,
{
    fn new(event: E) -> Self {
        Self {
            prev_block_hash: None,
            system_time: Utc::now(),
            sequence_number: 0,
            event,
        }
    }

    pub fn prev(mut self, prev_block_hash: &Multihash, prev_sequence_number: i32) -> Self {
        self.prev_block_hash = Some(prev_block_hash.clone());
        self.sequence_number = prev_sequence_number + 1;
        self
    }

    pub fn system_time(mut self, system_time: DateTime<Utc>) -> Self {
        self.system_time = system_time;
        self
    }

    pub fn build(self) -> MetadataBlock {
        MetadataBlock {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash,
            sequence_number: self.sequence_number,
            event: self.event.into(),
        }
    }

    pub fn build_typed(self) -> MetadataBlockTyped<E> {
        MetadataBlockTyped {
            system_time: self.system_time,
            prev_block_hash: self.prev_block_hash,
            sequence_number: self.sequence_number,
            event: self.event,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot Builder
///////////////////////////////////////////////////////////////////////////////

pub struct DatasetSnapshotBuilder {
    name: Option<DatasetName>,
    kind: DatasetKind,
    metadata: Vec<MetadataEvent>,
}

impl DatasetSnapshotBuilder {
    fn new() -> Self {
        Self {
            name: None,
            kind: DatasetKind::Root,
            metadata: Vec::new(),
        }
    }

    pub fn name<S: TryInto<DatasetName>>(mut self, s: S) -> Self
    where
        <S as TryInto<DatasetName>>::Error: std::fmt::Debug,
    {
        self.name = Some(s.try_into().unwrap());
        self
    }

    pub fn kind(mut self, kind: DatasetKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn push_event<E: Into<MetadataEvent>>(mut self, event: E) -> Self {
        self.metadata.push(event.into());
        self
    }

    pub fn build(self) -> DatasetSnapshot {
        DatasetSnapshot {
            name: self.name.unwrap_or_else(|| IDFactory::dataset_name()),
            kind: self.kind,
            metadata: self.metadata,
        }
    }
}
