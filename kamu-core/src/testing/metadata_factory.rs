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
    pub fn transform() -> TransformSqlBuilder {
        TransformSqlBuilder::new()
    }

    pub fn dataset_source_root() -> DatasetSourceBuilderRoot {
        DatasetSourceBuilderRoot::new()
    }

    pub fn dataset_source_deriv<S, I>(inputs: I) -> DatasetSourceBuilderDeriv
    where
        I: IntoIterator<Item = S>,
        S: TryInto<DatasetName>,
        <S as TryInto<DatasetName>>::Error: std::fmt::Debug,
    {
        DatasetSourceBuilderDeriv::new(inputs.into_iter())
    }

    pub fn metadata_block() -> MetadataBlockBuilder {
        MetadataBlockBuilder::new()
    }

    pub fn dataset_snapshot() -> DatasetSnapshotBuilder {
        DatasetSnapshotBuilder::new()
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
// DatasetSource Builder Root
///////////////////////////////////////////////////////////////////////////////

pub struct DatasetSourceBuilderRoot {
    v: DatasetSourceRoot,
}

impl DatasetSourceBuilderRoot {
    fn new() -> Self {
        Self {
            v: DatasetSourceRoot {
                fetch: FetchStep::Url(FetchStepUrl {
                    url: "http://nowhere.org".to_owned(),
                    event_time: None,
                    cache: None,
                }),
                prepare: None,
                read: ReadStep::GeoJson(ReadStepGeoJson { schema: None }),
                preprocess: None,
                merge: MergeStrategy::Append,
            },
        }
    }

    pub fn fetch(mut self, fetch_step: FetchStep) -> Self {
        self.v = DatasetSourceRoot {
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
        }))
    }

    pub fn read(mut self, read_step: ReadStep) -> Self {
        self.v = DatasetSourceRoot {
            read: read_step,
            ..self.v
        };
        self
    }

    pub fn build(self) -> DatasetSource {
        DatasetSource::Root(self.v)
    }

    pub fn build_inner(self) -> DatasetSourceRoot {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// DatasetSource Builder Deriv
///////////////////////////////////////////////////////////////////////////////

pub struct DatasetSourceBuilderDeriv {
    v: DatasetSourceDerivative,
}

impl DatasetSourceBuilderDeriv {
    fn new<S, I>(inputs: I) -> Self
    where
        I: Iterator<Item = S>,
        S: TryInto<DatasetName>,
        <S as TryInto<DatasetName>>::Error: std::fmt::Debug,
    {
        Self {
            v: DatasetSourceDerivative {
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

    pub fn build(self) -> DatasetSource {
        DatasetSource::Derivative(self.v)
    }

    pub fn build_inner(self) -> DatasetSourceDerivative {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// MetadataBlock Builder
///////////////////////////////////////////////////////////////////////////////

pub struct MetadataBlockBuilder {
    v: MetadataBlock,
}

impl MetadataBlockBuilder {
    fn new() -> Self {
        Self {
            v: MetadataBlock {
                prev_block_hash: None,
                system_time: Utc::now(),
                output_slice: None,
                output_watermark: None,
                input_slices: None,
                source: None,
                vocab: None,
                seed: None,
            },
        }
    }

    pub fn prev(mut self, prev_block_hash: &Multihash) -> Self {
        self.v.prev_block_hash = Some(prev_block_hash.clone());
        self
    }

    pub fn system_time(mut self, system_time: DateTime<Utc>) -> Self {
        self.v.system_time = system_time;
        self
    }

    pub fn input_slice(mut self, slice: InputSlice) -> Self {
        if self.v.input_slices.is_none() {
            self.v.input_slices = Some(Vec::new());
        }
        self.v.input_slices.as_mut().unwrap().push(slice);
        self
    }

    pub fn output_slice(mut self, slice: OutputSlice) -> Self {
        self.v.output_slice = Some(slice);
        self
    }

    pub fn output_watermark(mut self, wm: DateTime<Utc>) -> Self {
        self.v.output_watermark = Some(wm);
        self
    }

    pub fn source(mut self, source: DatasetSource) -> Self {
        self.v.source = Some(source);
        self
    }

    pub fn seed_random(mut self) -> Self {
        self.v.seed = Some(DatasetID::from_new_keypair_ed25519().1);
        self
    }

    pub fn seed_from<B: AsRef<[u8]>>(mut self, key: B) -> Self {
        self.v.seed = Some(DatasetID::from_pub_key_ed25519(key.as_ref()));
        self
    }

    pub fn build(self) -> MetadataBlock {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot Builder
///////////////////////////////////////////////////////////////////////////////

pub struct DatasetSnapshotBuilder {
    v: DatasetSnapshot,
}

impl DatasetSnapshotBuilder {
    fn new() -> Self {
        Self {
            v: DatasetSnapshot {
                name: IDFactory::dataset_name(),
                source: DatasetSourceBuilderRoot::new().build(),
                vocab: None,
            },
        }
    }

    pub fn name<S: TryInto<DatasetName>>(mut self, s: S) -> Self
    where
        <S as TryInto<DatasetName>>::Error: std::fmt::Debug,
    {
        self.v.name = s.try_into().unwrap();
        self
    }

    pub fn source(mut self, source: DatasetSource) -> Self {
        self.v.source = source;
        self
    }

    pub fn build(self) -> DatasetSnapshot {
        self.v
    }
}
