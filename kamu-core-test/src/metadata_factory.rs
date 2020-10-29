use kamu::domain::*;
use kamu::infra::serde::yaml::*;

use chrono::{DateTime, SubsecRound, Utc};
use std::convert::TryFrom;
use std::path::Path;

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
        S: AsRef<str>,
    {
        Self {
            v: DatasetSourceDerivative {
                inputs: inputs
                    .map(|s| DatasetIDBuf::try_from(s.as_ref()).unwrap())
                    .collect(),
                transform: TransformSqlBuilder::new().build(),
            },
        }
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

    pub fn system_time(mut self, system_time: DateTime<Utc>) -> Self {
        self.v.system_time = system_time;
        self
    }

    pub fn output_slice(mut self, slice: DataSlice) -> Self {
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
