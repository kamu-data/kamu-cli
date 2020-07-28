use chrono::{SubsecRound, Utc};
use kamu::domain::*;
use kamu::infra::serde::yaml::*;

use std::convert::TryFrom;

pub struct MetadataFactory;

impl MetadataFactory {
    pub fn transform() -> TransformBuilder {
        TransformBuilder::new()
    }

    pub fn dataset_source_root() -> DatasetSourceBuilderRoot {
        DatasetSourceBuilderRoot::new()
    }

    pub fn dataset_source_deriv<S, I>(inputs: I) -> DatasetSourceBuilderDeriv
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
    {
        DatasetSourceBuilderDeriv::new(inputs)
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

pub struct TransformBuilder {
    v: Transform,
}

impl TransformBuilder {
    fn new() -> Self {
        Self {
            v: Transform {
                engine: "some_engine".to_owned(),
                additional_properties: std::collections::BTreeMap::new(),
            },
        }
    }

    pub fn build(self) -> Transform {
        self.v
    }
}

///////////////////////////////////////////////////////////////////////////////
// DatasetSource Builder
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

    pub fn build(self) -> DatasetSource {
        DatasetSource::Root(self.v)
    }
}

pub struct DatasetSourceBuilderDeriv {
    v: DatasetSourceDerivative,
}

impl DatasetSourceBuilderDeriv {
    fn new<S, I>(inputs: I) -> Self
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            v: DatasetSourceDerivative {
                inputs: inputs
                    .map(|s| DatasetIDBuf::try_from(s.as_ref()).unwrap())
                    .collect(),
                transform: TransformBuilder::new().build(),
            },
        }
    }

    pub fn build(self) -> DatasetSource {
        DatasetSource::Derivative(self.v)
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
                block_hash: "".to_owned(),
                prev_block_hash: "".to_owned(),
                system_time: Utc::now().round_subsecs(3),
                output_slice: None,
                output_watermark: None,
                input_slices: None,
                source: None,
            },
        }
    }

    pub fn prev(mut self, prev_block_hash: &str) -> Self {
        self.v.prev_block_hash.clear();
        self.v.prev_block_hash.push_str(prev_block_hash);
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
                id: DatasetIDBuf::new(),
                source: DatasetSourceBuilderRoot::new().build(),
                vocab: None,
            },
        }
    }

    pub fn id<S: AsRef<str>>(mut self, s: S) -> Self {
        self.v.id = DatasetIDBuf::try_from(s.as_ref()).unwrap();
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
