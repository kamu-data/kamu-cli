// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::*;

use chrono::{DateTime, Utc};
use std::path::Path;

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

    pub fn add_data() -> AddDataBuilder {
        AddDataBuilder::new()
    }

    pub fn set_transform<S, I>(inputs: I) -> SetTransformBuilder
    where
        I: IntoIterator<Item = S>,
        S: TryInto<DatasetName>,
        <S as TryInto<DatasetName>>::Error: std::fmt::Debug,
    {
        SetTransformBuilder::new(inputs.into_iter())
    }

    pub fn metadata_block<E: Into<MetadataEvent>>(event: E) -> MetadataBlockBuilder {
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
                dataset_id: DatasetID::from_pub_key_ed25519(b""),
                dataset_kind: kind,
            },
        }
    }

    pub fn id_random(mut self) -> Self {
        self.v.dataset_id = DatasetID::from_new_keypair_ed25519().1;
        self
    }

    pub fn id_from<B: AsRef<[u8]>>(mut self, key: B) -> Self {
        self.v.dataset_id = DatasetID::from_pub_key_ed25519(key.as_ref());
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
                merge: MergeStrategy::Append,
            },
        }
    }

    pub fn fetch(mut self, fetch_step: FetchStep) -> Self {
        self.v = SetPollingSource {
            fetch: fetch_step,
            ..self.v
        };
        self
    }

    pub fn fetch_file(self, path: &Path) -> Self {
        self.fetch(FetchStep::Url(FetchStepUrl {
            url: url::Url::from_file_path(path).unwrap().as_str().to_owned(),
            event_time: None, // TODO: Some(EventTimeSource::FromMetadata),
            cache: None,
            headers: None,
        }))
    }

    pub fn read(mut self, read_step: ReadStep) -> Self {
        self.v = SetPollingSource {
            read: read_step,
            ..self.v
        };
        self
    }

    pub fn build(self) -> SetPollingSource {
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
                    })
                    .collect(),
                transform: TransformSqlBuilder::new().build(),
            },
        }
    }

    pub fn input_ids_from_names(mut self) -> Self {
        for input in self.v.inputs.iter_mut() {
            input.id = Some(DatasetID::from_pub_key_ed25519(input.name.as_bytes()));
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
// AddDataBuilder
///////////////////////////////////////////////////////////////////////////////

pub struct AddDataBuilder {
    v: AddData,
}

impl AddDataBuilder {
    fn new() -> Self {
        Self {
            v: AddData {
                input_checkpoint: None,
                output_data: DataSlice {
                    logical_hash: Multihash::from_digest_sha3_256(b"foo"),
                    physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                    interval: OffsetInterval { start: 0, end: 9 },
                    size: 0,
                },
                output_checkpoint: None,
                output_watermark: Some(Utc::now()),
            },
        }
    }

    pub fn data_physical_hash(mut self, hash: Multihash) -> Self {
        self.v.output_data.physical_hash = hash;
        self
    }

    pub fn data_size(mut self, size: i64) -> Self {
        self.v.output_data.size = size;
        self
    }

    pub fn checkpoint_physical_hash(mut self, hash: Multihash) -> Self {
        self.v.output_checkpoint = Some(Checkpoint {
            physical_hash: hash,
            size: 1,
        });
        self
    }

    pub fn checkpoint_size(mut self, size: i64) -> Self {
        if self.v.output_checkpoint.is_none() {
            self = self.checkpoint_physical_hash(Multihash::from_digest_sha3_256(b"foo"));
        }
        self.v.output_checkpoint.as_mut().unwrap().size = size;
        self
    }

    pub fn interval(mut self, start: i64, end: i64) -> Self {
        self.v.output_data.interval = OffsetInterval { start, end };
        self
    }

    pub fn watermark(mut self, wm: DateTime<Utc>) -> Self {
        self.v.output_watermark = Some(wm);
        self
    }

    pub fn build(self) -> AddData {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// MetadataBlock Builder
///////////////////////////////////////////////////////////////////////////////

pub struct MetadataBlockBuilder {
    prev_block_hash: Option<Multihash>,
    system_time: DateTime<Utc>,
    event: MetadataEvent,
    sequence_number: i32,
}

impl MetadataBlockBuilder {
    fn new<E: Into<MetadataEvent>>(event: E) -> Self {
        Self {
            prev_block_hash: None,
            system_time: Utc::now(),
            event: event.into(),
            sequence_number: 0,
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
            event: self.event,
            sequence_number: self.sequence_number,
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
