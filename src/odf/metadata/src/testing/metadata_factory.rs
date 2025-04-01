// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use chrono::{DateTime, Utc};

use super::IDFactory;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataFactory;

impl MetadataFactory {
    pub fn seed(kind: DatasetKind) -> SeedBuilder {
        SeedBuilder::new(kind)
    }

    pub fn set_info() -> SetInfoBuilder {
        SetInfoBuilder::new()
    }

    pub fn set_license() -> SetLicenseBuilder {
        SetLicenseBuilder::new()
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

    #[cfg(feature = "arrow")]
    pub fn set_data_schema() -> SetDataSchemaBuilder {
        SetDataSchemaBuilder::new()
    }

    pub fn add_data() -> AddDataBuilder {
        AddDataBuilder::new()
    }

    pub fn execute_transform() -> ExecuteTransformBuilder {
        ExecuteTransformBuilder::new()
    }

    pub fn set_transform() -> SetTransformBuilder {
        SetTransformBuilder::new()
    }

    pub fn metadata_block<E: Into<MetadataEvent>>(event: E) -> MetadataBlockBuilder<E> {
        MetadataBlockBuilder::new(event)
    }

    pub fn dataset_snapshot() -> DatasetSnapshotBuilder {
        DatasetSnapshotBuilder::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Seed Builder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    pub fn id(mut self, dataset_id: DatasetID) -> Self {
        self.v.dataset_id = dataset_id;
        self
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetInfo Builder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetLicense Builder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SetLicenseBuilder {
    v: SetLicense,
}

impl SetLicenseBuilder {
    fn new() -> Self {
        Self {
            v: SetLicense {
                short_name: String::from("TEST"),
                name: String::from("TEST LICENSE"),
                spdx_id: None,
                website_url: String::from("http://example.com"),
            },
        }
    }

    pub fn short_name(mut self, short_name: &str) -> Self {
        self.v.short_name = String::from(short_name);
        self
    }

    pub fn name(mut self, name: &str) -> Self {
        self.v.name = String::from(name);
        self
    }

    pub fn spdx_id(mut self, spdx_id: &str) -> Self {
        self.v.spdx_id = Some(String::from(spdx_id));
        self
    }

    pub fn website_url(mut self, website_url: &str) -> Self {
        self.v.website_url = String::from(website_url);
        self
    }

    pub fn build(self) -> SetLicense {
        self.v
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Transform Builder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
                queries: Some(vec![SqlQueryStep {
                    alias: None,
                    query: "select * from input".to_string(),
                }]),
                temporal_tables: None,
            },
        }
    }

    pub fn engine(mut self, engine: &str) -> Self {
        engine.clone_into(&mut self.v.engine);
        self
    }

    pub fn query(mut self, query: &str) -> Self {
        self.v.queries = Some(vec![SqlQueryStep {
            alias: None,
            query: query.to_owned(),
        }]);
        self
    }

    pub fn build(self) -> Transform {
        Transform::Sql(self.v)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetPollingSourceBuilder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddPushSourceBuilder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AddPushSourceBuilder {
    v: AddPushSource,
}

impl AddPushSourceBuilder {
    fn new() -> Self {
        Self {
            v: AddPushSource {
                source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
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
            source_name: name.into(),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetTransformBuilder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SetTransformBuilder {
    v: SetTransform,
}

impl SetTransformBuilder {
    pub fn new() -> Self {
        Self {
            v: SetTransform {
                inputs: Vec::new(),
                transform: TransformSqlBuilder::new().build(),
            },
        }
    }

    pub fn inputs_from_refs<R, I>(mut self, inputs: I) -> Self
    where
        I: IntoIterator<Item = R>,
        R: TryInto<DatasetRef>,
        <R as TryInto<DatasetRef>>::Error: std::fmt::Debug,
    {
        self.v.inputs = inputs
            .into_iter()
            .map(|a| TransformInput {
                dataset_ref: a.try_into().unwrap(),
                alias: None,
            })
            .collect();
        self
    }

    pub fn inputs_from_refs_and_aliases<R, A, I>(mut self, inputs: I) -> Self
    where
        I: IntoIterator<Item = (R, A)>,
        A: Into<String>,
        R: TryInto<DatasetRef>,
        <R as TryInto<DatasetRef>>::Error: std::fmt::Debug,
    {
        self.v.inputs = inputs
            .into_iter()
            .map(|(r, a)| TransformInput {
                dataset_ref: r.try_into().unwrap(),
                alias: Some(a.into()),
            })
            .collect();
        self
    }

    pub fn inputs_from_aliases_and_seeded_ids<A, I>(mut self, inputs: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: Into<String>,
    {
        self.v.inputs = inputs
            .into_iter()
            .map(|a| {
                let alias = a.into();
                TransformInput {
                    dataset_ref: DatasetID::new_seeded_ed25519(alias.as_bytes()).into(),
                    alias: Some(alias),
                }
            })
            .collect();
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetDataSchemaBuilder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SetDataSchemaBuilder {
    v: SetDataSchema,
}

impl SetDataSchemaBuilder {
    #[cfg(feature = "arrow")]
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

    #[cfg(feature = "arrow")]
    pub fn schema(mut self, schema: &datafusion::arrow::datatypes::Schema) -> Self {
        self.v = SetDataSchema::new(schema);
        self
    }

    pub fn build(self) -> SetDataSchema {
        self.v
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddDataBuilder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AddDataBuilder {
    v: AddData,
}

impl AddDataBuilder {
    pub fn empty() -> Self {
        Self {
            v: AddData {
                prev_checkpoint: None,
                prev_offset: None,
                new_data: None,
                new_checkpoint: None,
                new_watermark: None,
                new_source_state: None,
            },
        }
    }

    pub fn new() -> Self {
        Self::empty()
    }

    pub fn some_prev_checkpoint(mut self) -> Self {
        if self.v.prev_checkpoint.is_none() {
            self.v.prev_checkpoint = Some(Multihash::from_digest_sha3_256(b"foo"));
        }
        self
    }

    pub fn prev_checkpoint(mut self, checkpoint: Option<Multihash>) -> Self {
        self.v.prev_checkpoint = checkpoint;
        self
    }

    pub fn prev_offset(mut self, prev_offset: Option<u64>) -> Self {
        self.v.prev_offset = prev_offset;
        self
    }

    pub fn some_new_data(self) -> Self {
        let start_offset = self.v.prev_offset.map_or(0, |offset| offset + 1);
        self.some_new_data_with_offset(start_offset, start_offset + 9)
    }

    pub fn some_new_data_with_offset(mut self, start: u64, end: u64) -> Self {
        if self.v.new_data.is_none() {
            self.v.new_data = Some(DataSlice {
                logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                offset_interval: OffsetInterval { start, end },
                size: 0,
            });
            if start != 0 {
                self.v.prev_offset = Some(start - 1);
            }
        }
        self
    }

    pub fn some_new_checkpoint(mut self) -> Self {
        if self.v.new_checkpoint.is_none() {
            self.v.new_checkpoint = Some(Checkpoint {
                physical_hash: Multihash::from_digest_sha3_256(b"foo"),
                size: 1,
            });
        }
        self
    }

    pub fn new_checkpoint(mut self, checkpoint: Option<Checkpoint>) -> Self {
        self.v.new_checkpoint = checkpoint;
        self
    }

    pub fn some_new_watermark(mut self) -> Self {
        if self.v.new_watermark.is_none() {
            self.v.new_watermark = Some(Utc::now());
        }
        self
    }

    pub fn new_watermark(mut self, watermark: Option<DateTime<Utc>>) -> Self {
        self.v.new_watermark = watermark;
        self
    }

    pub fn some_new_source_state(mut self) -> Self {
        if self.v.new_source_state.is_none() {
            self.v.new_source_state = Some(SourceState {
                source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
                kind: SourceState::KIND_ETAG.to_owned(),
                value: "<etag>".to_owned(),
            });
        }
        self
    }

    pub fn new_source_state(mut self, source_state: Option<SourceState>) -> Self {
        self.v.new_source_state = source_state;
        self
    }

    pub fn new_data_physical_hash(mut self, hash: Multihash) -> Self {
        self = self.some_new_data();
        self.v.new_data.as_mut().unwrap().physical_hash = hash;
        self
    }

    pub fn new_offset_interval(mut self, start: u64, end: u64) -> Self {
        self = self.some_new_data();
        self.v.new_data.as_mut().unwrap().offset_interval = OffsetInterval { start, end };
        if start != 0 {
            self.v.prev_offset = Some(start - 1);
        }
        self
    }

    pub fn new_data_size(mut self, size: u64) -> Self {
        self = self.some_new_data();
        self.v.new_data.as_mut().unwrap().size = size;
        self
    }

    pub fn new_checkpoint_physical_hash(mut self, hash: Multihash) -> Self {
        self = self.some_new_checkpoint();
        self.v.new_checkpoint.as_mut().unwrap().physical_hash = hash;
        self
    }

    pub fn new_checkpoint_size(mut self, size: u64) -> Self {
        self = self.some_new_checkpoint();
        self.v.new_checkpoint.as_mut().unwrap().size = size;
        self
    }

    pub fn build(self) -> AddData {
        self.v
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransformBuilder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ExecuteTransformBuilder {
    v: ExecuteTransform,
}

impl ExecuteTransformBuilder {
    pub fn new() -> Self {
        Self {
            v: ExecuteTransform {
                query_inputs: Vec::new(),
                prev_checkpoint: None,
                prev_offset: None,
                new_data: None,
                new_checkpoint: None,
                new_watermark: None,
            },
        }
    }

    pub fn push_query_input(mut self, input: ExecuteTransformInput) -> Self {
        self.v.query_inputs.push(input);
        self
    }

    pub fn empty_query_inputs_from_particular_ids<I>(mut self, dataset_ids: I) -> Self
    where
        I: IntoIterator<Item = DatasetID>,
    {
        self.v.query_inputs = dataset_ids
            .into_iter()
            .map(|dataset_id| ExecuteTransformInput {
                dataset_id,
                prev_block_hash: None,
                new_block_hash: None,
                prev_offset: None,
                new_offset: None,
            })
            .collect();
        self
    }

    pub fn empty_query_inputs_from_seeded_ids<I, A>(mut self, aliases: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: Into<String>,
    {
        self.v.query_inputs = aliases
            .into_iter()
            .map(|i| ExecuteTransformInput {
                dataset_id: DatasetID::new_seeded_ed25519(i.into().as_bytes()),
                prev_block_hash: None,
                new_block_hash: None,
                prev_offset: None,
                new_offset: None,
            })
            .collect();
        self
    }

    pub fn prev_checkpoint(mut self, checkpoint: Option<Multihash>) -> Self {
        self.v.prev_checkpoint = checkpoint;
        self
    }

    pub fn prev_offset(mut self, offset: Option<u64>) -> Self {
        self.v.prev_offset = offset;
        self
    }

    pub fn some_new_data(self) -> Self {
        let start_offset = self.v.prev_offset.map_or(0, |offset| offset + 1);
        self.some_new_data_with_offset(start_offset, start_offset + 9)
    }

    pub fn some_new_data_with_offset(mut self, start: u64, end: u64) -> Self {
        if self.v.new_data.is_none() {
            self.v.new_data = Some(DataSlice {
                logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                offset_interval: OffsetInterval { start, end },
                size: 0,
            });
            if start != 0 {
                self.v.prev_offset = Some(start - 1);
            }
        }
        self
    }

    pub fn new_data(mut self, data_slice: Option<DataSlice>) -> Self {
        self.v.new_data = data_slice;
        self
    }

    pub fn some_new_checkpoint(mut self) -> Self {
        if self.v.new_checkpoint.is_none() {
            self.v.new_checkpoint = Some(Checkpoint {
                physical_hash: Multihash::from_digest_sha3_256(b"foo"),
                size: 1,
            });
        }
        self
    }

    pub fn new_checkpoint(mut self, checkpoint: Option<Checkpoint>) -> Self {
        self.v.new_checkpoint = checkpoint;
        self
    }

    pub fn some_new_watermark(mut self) -> Self {
        self.v.new_watermark = Some(Utc::now());
        self
    }

    pub fn new_watermark(mut self, watermark: Option<DateTime<Utc>>) -> Self {
        self.v.new_watermark = watermark;
        self
    }

    pub fn build(self) -> ExecuteTransform {
        self.v
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataBlock Builder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataBlockBuilder<E> {
    prev_block_hash: Option<Multihash>,
    system_time: DateTime<Utc>,
    sequence_number: u64,
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

    pub fn prev(mut self, prev_block_hash: &Multihash, prev_sequence_number: u64) -> Self {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot Builder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetSnapshotBuilder {
    name: Option<DatasetAlias>,
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

    pub fn name<S: TryInto<DatasetAlias>>(mut self, s: S) -> Self
    where
        <S as TryInto<DatasetAlias>>::Error: std::fmt::Debug,
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
            name: self
                .name
                .unwrap_or_else(|| DatasetAlias::new(None, IDFactory::dataset_name())),
            kind: self.kind,
            metadata: self.metadata,
        }
    }
}
