////////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
////////////////////////////////////////////////////////////////////////////////

use super::odf_generated as fb;
mod odf {
    pub use crate::dataset_id::*;
    pub use crate::dtos::*;
    pub use crate::sha::*;
    pub use crate::time_interval::*;
}
use ::flatbuffers::{FlatBufferBuilder, Table, UnionWIPOffset, WIPOffset};
use chrono::prelude::*;
use std::convert::{TryFrom, TryInto};
use std::str::FromStr;

pub trait FlatbuffersSerializable<'fb> {
    type OffsetT;
    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT;
}

pub trait FlatbuffersDeserializable<T> {
    fn deserialize(fb: T) -> Self;
}

trait FlatbuffersEnumSerializable<'fb, E> {
    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> (E, WIPOffset<UnionWIPOffset>);
}

trait FlatbuffersEnumDeserializable<'fb, E> {
    fn deserialize(table: Table<'fb>, t: E) -> Self
    where
        Self: Sized;
}

////////////////////////////////////////////////////////////////////////////////
// DatasetSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsource-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::DatasetSource> for odf::DatasetSource {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::DatasetSource, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::DatasetSource::Root(v) => (
                fb::DatasetSource::DatasetSourceRoot,
                v.serialize(fb).as_union_value(),
            ),
            odf::DatasetSource::Derivative(v) => (
                fb::DatasetSource::DatasetSourceDerivative,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::DatasetSource> for odf::DatasetSource {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::DatasetSource) -> Self {
        match t {
            fb::DatasetSource::DatasetSourceRoot => odf::DatasetSource::Root(
                odf::DatasetSourceRoot::deserialize(fb::DatasetSourceRoot::init_from_table(table)),
            ),
            fb::DatasetSource::DatasetSourceDerivative => {
                odf::DatasetSource::Derivative(odf::DatasetSourceDerivative::deserialize(
                    fb::DatasetSourceDerivative::init_from_table(table),
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::DatasetSourceRoot {
    type OffsetT = WIPOffset<fb::DatasetSourceRoot<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let fetch_offset = { self.fetch.serialize(fb) };
        let prepare_offset = self.prepare.as_ref().map(|v| {
            let offsets: Vec<_> = v
                .iter()
                .map(|i| {
                    let (value_type, value_offset) = i.serialize(fb);
                    let mut builder = fb::PrepStepWrapperBuilder::new(fb);
                    builder.add_value_type(value_type);
                    builder.add_value(value_offset);
                    builder.finish()
                })
                .collect();
            fb.create_vector(&offsets)
        });
        let read_offset = { self.read.serialize(fb) };
        let preprocess_offset = self.preprocess.as_ref().map(|v| v.serialize(fb));
        let merge_offset = { self.merge.serialize(fb) };
        let mut builder = fb::DatasetSourceRootBuilder::new(fb);
        builder.add_fetch_type(fetch_offset.0);
        builder.add_fetch(fetch_offset.1);
        prepare_offset.map(|off| builder.add_prepare(off));
        builder.add_read_type(read_offset.0);
        builder.add_read(read_offset.1);
        preprocess_offset.map(|(e, off)| {
            builder.add_preprocess_type(e);
            builder.add_preprocess(off)
        });
        builder.add_merge_type(merge_offset.0);
        builder.add_merge(merge_offset.1);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DatasetSourceRoot<'fb>> for odf::DatasetSourceRoot {
    fn deserialize(proxy: fb::DatasetSourceRoot<'fb>) -> Self {
        odf::DatasetSourceRoot {
            fetch: odf::FetchStep::deserialize(proxy.fetch().unwrap(), proxy.fetch_type()),
            prepare: proxy.prepare().map(|v| {
                v.iter()
                    .map(|i| odf::PrepStep::deserialize(i.value().unwrap(), i.value_type()))
                    .collect()
            }),
            read: odf::ReadStep::deserialize(proxy.read().unwrap(), proxy.read_type()),
            preprocess: proxy
                .preprocess()
                .map(|tbl| odf::Transform::deserialize(tbl, proxy.preprocess_type())),
            merge: odf::MergeStrategy::deserialize(proxy.merge().unwrap(), proxy.merge_type()),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::DatasetSourceDerivative {
    type OffsetT = WIPOffset<fb::DatasetSourceDerivative<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let inputs_offset = {
            let offsets: Vec<_> = self.inputs.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        };
        let transform_offset = { self.transform.serialize(fb) };
        let mut builder = fb::DatasetSourceDerivativeBuilder::new(fb);
        builder.add_inputs(inputs_offset);
        builder.add_transform_type(transform_offset.0);
        builder.add_transform(transform_offset.1);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DatasetSourceDerivative<'fb>>
    for odf::DatasetSourceDerivative
{
    fn deserialize(proxy: fb::DatasetSourceDerivative<'fb>) -> Self {
        odf::DatasetSourceDerivative {
            inputs: proxy
                .inputs()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::DatasetIDBuf::from_str(i).unwrap())
                        .collect()
                })
                .unwrap(),
            transform: odf::Transform::deserialize(
                proxy.transform().unwrap(),
                proxy.transform_type(),
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::DatasetVocabulary {
    type OffsetT = WIPOffset<fb::DatasetVocabulary<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let system_time_column_offset = self
            .system_time_column
            .as_ref()
            .map(|v| fb.create_string(&v));
        let event_time_column_offset = self
            .event_time_column
            .as_ref()
            .map(|v| fb.create_string(&v));
        let mut builder = fb::DatasetVocabularyBuilder::new(fb);
        system_time_column_offset.map(|off| builder.add_system_time_column(off));
        event_time_column_offset.map(|off| builder.add_event_time_column(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DatasetVocabulary<'fb>> for odf::DatasetVocabulary {
    fn deserialize(proxy: fb::DatasetVocabulary<'fb>) -> Self {
        odf::DatasetVocabulary {
            system_time_column: proxy.system_time_column().map(|v| v.to_owned()),
            event_time_column: proxy.event_time_column().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::SourceCaching> for odf::SourceCaching {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::SourceCaching, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::SourceCaching::Forever => (
                fb::SourceCaching::SourceCachingForever,
                empty_table(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::SourceCaching> for odf::SourceCaching {
    fn deserialize(_table: flatbuffers::Table<'fb>, t: fb::SourceCaching) -> Self {
        match t {
            fb::SourceCaching::SourceCachingForever => odf::SourceCaching::Forever,
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::TemporalTable {
    type OffsetT = WIPOffset<fb::TemporalTable<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let id_offset = { fb.create_string(&self.id) };
        let primary_key_offset = {
            let offsets: Vec<_> = self
                .primary_key
                .iter()
                .map(|i| fb.create_string(&i))
                .collect();
            fb.create_vector(&offsets)
        };
        let mut builder = fb::TemporalTableBuilder::new(fb);
        builder.add_id(id_offset);
        builder.add_primary_key(primary_key_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TemporalTable<'fb>> for odf::TemporalTable {
    fn deserialize(proxy: fb::TemporalTable<'fb>) -> Self {
        odf::TemporalTable {
            id: proxy.id().map(|v| v.to_owned()).unwrap(),
            primary_key: proxy
                .primary_key()
                .map(|v| v.iter().map(|i| i.to_owned()).collect())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::DatasetSnapshot {
    type OffsetT = WIPOffset<fb::DatasetSnapshot<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let id_offset = { fb.create_string(&self.id) };
        let source_offset = { self.source.serialize(fb) };
        let vocab_offset = self.vocab.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::DatasetSnapshotBuilder::new(fb);
        builder.add_id(id_offset);
        builder.add_source_type(source_offset.0);
        builder.add_source(source_offset.1);
        vocab_offset.map(|off| builder.add_vocab(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DatasetSnapshot<'fb>> for odf::DatasetSnapshot {
    fn deserialize(proxy: fb::DatasetSnapshot<'fb>) -> Self {
        odf::DatasetSnapshot {
            id: proxy
                .id()
                .map(|v| odf::DatasetIDBuf::try_from(v).unwrap())
                .unwrap(),
            source: odf::DatasetSource::deserialize(proxy.source().unwrap(), proxy.source_type()),
            vocab: proxy
                .vocab()
                .map(|v| odf::DatasetVocabulary::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::DataSlice {
    type OffsetT = WIPOffset<fb::DataSlice<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let hash_offset = { fb.create_vector(&self.hash) };
        let mut builder = fb::DataSliceBuilder::new(fb);
        builder.add_hash(hash_offset);
        builder.add_interval(&interval_to_fb(&self.interval));
        builder.add_num_records(self.num_records);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataSlice<'fb>> for odf::DataSlice {
    fn deserialize(proxy: fb::DataSlice<'fb>) -> Self {
        odf::DataSlice {
            hash: proxy
                .hash()
                .map(|v| odf::Sha3_256::new(v.try_into().unwrap()))
                .unwrap(),
            interval: fb_to_interval(proxy.interval().unwrap()),
            num_records: proxy.num_records(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SqlQueryStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sqlquerystep-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::SqlQueryStep {
    type OffsetT = WIPOffset<fb::SqlQueryStep<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let alias_offset = self.alias.as_ref().map(|v| fb.create_string(&v));
        let query_offset = { fb.create_string(&self.query) };
        let mut builder = fb::SqlQueryStepBuilder::new(fb);
        alias_offset.map(|off| builder.add_alias(off));
        builder.add_query(query_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SqlQueryStep<'fb>> for odf::SqlQueryStep {
    fn deserialize(proxy: fb::SqlQueryStep<'fb>) -> Self {
        odf::SqlQueryStep {
            alias: proxy.alias().map(|v| v.to_owned()),
            query: proxy.query().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::MergeStrategy> for odf::MergeStrategy {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::MergeStrategy, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::MergeStrategy::Append => (
                fb::MergeStrategy::MergeStrategyAppend,
                empty_table(fb).as_union_value(),
            ),
            odf::MergeStrategy::Ledger(v) => (
                fb::MergeStrategy::MergeStrategyLedger,
                v.serialize(fb).as_union_value(),
            ),
            odf::MergeStrategy::Snapshot(v) => (
                fb::MergeStrategy::MergeStrategySnapshot,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::MergeStrategy> for odf::MergeStrategy {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::MergeStrategy) -> Self {
        match t {
            fb::MergeStrategy::MergeStrategyAppend => odf::MergeStrategy::Append,
            fb::MergeStrategy::MergeStrategyLedger => {
                odf::MergeStrategy::Ledger(odf::MergeStrategyLedger::deserialize(
                    fb::MergeStrategyLedger::init_from_table(table),
                ))
            }
            fb::MergeStrategy::MergeStrategySnapshot => {
                odf::MergeStrategy::Snapshot(odf::MergeStrategySnapshot::deserialize(
                    fb::MergeStrategySnapshot::init_from_table(table),
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::MergeStrategyLedger {
    type OffsetT = WIPOffset<fb::MergeStrategyLedger<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let primary_key_offset = {
            let offsets: Vec<_> = self
                .primary_key
                .iter()
                .map(|i| fb.create_string(&i))
                .collect();
            fb.create_vector(&offsets)
        };
        let mut builder = fb::MergeStrategyLedgerBuilder::new(fb);
        builder.add_primary_key(primary_key_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::MergeStrategyLedger<'fb>> for odf::MergeStrategyLedger {
    fn deserialize(proxy: fb::MergeStrategyLedger<'fb>) -> Self {
        odf::MergeStrategyLedger {
            primary_key: proxy
                .primary_key()
                .map(|v| v.iter().map(|i| i.to_owned()).collect())
                .unwrap(),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::MergeStrategySnapshot {
    type OffsetT = WIPOffset<fb::MergeStrategySnapshot<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let primary_key_offset = {
            let offsets: Vec<_> = self
                .primary_key
                .iter()
                .map(|i| fb.create_string(&i))
                .collect();
            fb.create_vector(&offsets)
        };
        let compare_columns_offset = self.compare_columns.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let observation_column_offset = self
            .observation_column
            .as_ref()
            .map(|v| fb.create_string(&v));
        let obsv_added_offset = self.obsv_added.as_ref().map(|v| fb.create_string(&v));
        let obsv_changed_offset = self.obsv_changed.as_ref().map(|v| fb.create_string(&v));
        let obsv_removed_offset = self.obsv_removed.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::MergeStrategySnapshotBuilder::new(fb);
        builder.add_primary_key(primary_key_offset);
        compare_columns_offset.map(|off| builder.add_compare_columns(off));
        observation_column_offset.map(|off| builder.add_observation_column(off));
        obsv_added_offset.map(|off| builder.add_obsv_added(off));
        obsv_changed_offset.map(|off| builder.add_obsv_changed(off));
        obsv_removed_offset.map(|off| builder.add_obsv_removed(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::MergeStrategySnapshot<'fb>> for odf::MergeStrategySnapshot {
    fn deserialize(proxy: fb::MergeStrategySnapshot<'fb>) -> Self {
        odf::MergeStrategySnapshot {
            primary_key: proxy
                .primary_key()
                .map(|v| v.iter().map(|i| i.to_owned()).collect())
                .unwrap(),
            compare_columns: proxy
                .compare_columns()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            observation_column: proxy.observation_column().map(|v| v.to_owned()),
            obsv_added: proxy.obsv_added().map(|v| v.to_owned()),
            obsv_changed: proxy.obsv_changed().map(|v| v.to_owned()),
            obsv_removed: proxy.obsv_removed().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::MetadataBlock {
    type OffsetT = WIPOffset<fb::MetadataBlock<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let block_hash_offset = { fb.create_vector(&self.block_hash) };
        let prev_block_hash_offset = self.prev_block_hash.as_ref().map(|v| fb.create_vector(&v));
        let output_slice_offset = self.output_slice.as_ref().map(|v| v.serialize(fb));
        let input_slices_offset = self.input_slices.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        });
        let source_offset = self.source.as_ref().map(|v| v.serialize(fb));
        let vocab_offset = self.vocab.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::MetadataBlockBuilder::new(fb);
        builder.add_block_hash(block_hash_offset);
        prev_block_hash_offset.map(|off| builder.add_prev_block_hash(off));
        builder.add_system_time(&datetime_to_fb(&self.system_time));
        output_slice_offset.map(|off| builder.add_output_slice(off));
        self.output_watermark
            .as_ref()
            .map(|v| builder.add_output_watermark(&datetime_to_fb(&v)));
        input_slices_offset.map(|off| builder.add_input_slices(off));
        source_offset.map(|(e, off)| {
            builder.add_source_type(e);
            builder.add_source(off)
        });
        vocab_offset.map(|off| builder.add_vocab(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::MetadataBlock<'fb>> for odf::MetadataBlock {
    fn deserialize(proxy: fb::MetadataBlock<'fb>) -> Self {
        odf::MetadataBlock {
            block_hash: proxy
                .block_hash()
                .map(|v| odf::Sha3_256::new(v.try_into().unwrap()))
                .unwrap(),
            prev_block_hash: proxy
                .prev_block_hash()
                .map(|v| odf::Sha3_256::new(v.try_into().unwrap())),
            system_time: proxy.system_time().map(|v| fb_to_datetime(v)).unwrap(),
            output_slice: proxy.output_slice().map(|v| odf::DataSlice::deserialize(v)),
            output_watermark: proxy.output_watermark().map(|v| fb_to_datetime(v)),
            input_slices: proxy
                .input_slices()
                .map(|v| v.iter().map(|i| odf::DataSlice::deserialize(i)).collect()),
            source: proxy
                .source()
                .map(|tbl| odf::DatasetSource::deserialize(tbl, proxy.source_type())),
            vocab: proxy
                .vocab()
                .map(|v| odf::DatasetVocabulary::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::ReadStep> for odf::ReadStep {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::ReadStep, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::ReadStep::Csv(v) => (fb::ReadStep::ReadStepCsv, v.serialize(fb).as_union_value()),
            odf::ReadStep::JsonLines(v) => (
                fb::ReadStep::ReadStepJsonLines,
                v.serialize(fb).as_union_value(),
            ),
            odf::ReadStep::GeoJson(v) => (
                fb::ReadStep::ReadStepGeoJson,
                v.serialize(fb).as_union_value(),
            ),
            odf::ReadStep::EsriShapefile(v) => (
                fb::ReadStep::ReadStepEsriShapefile,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::ReadStep> for odf::ReadStep {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::ReadStep) -> Self {
        match t {
            fb::ReadStep::ReadStepCsv => odf::ReadStep::Csv(odf::ReadStepCsv::deserialize(
                fb::ReadStepCsv::init_from_table(table),
            )),
            fb::ReadStep::ReadStepJsonLines => odf::ReadStep::JsonLines(
                odf::ReadStepJsonLines::deserialize(fb::ReadStepJsonLines::init_from_table(table)),
            ),
            fb::ReadStep::ReadStepGeoJson => odf::ReadStep::GeoJson(
                odf::ReadStepGeoJson::deserialize(fb::ReadStepGeoJson::init_from_table(table)),
            ),
            fb::ReadStep::ReadStepEsriShapefile => {
                odf::ReadStep::EsriShapefile(odf::ReadStepEsriShapefile::deserialize(
                    fb::ReadStepEsriShapefile::init_from_table(table),
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::ReadStepCsv {
    type OffsetT = WIPOffset<fb::ReadStepCsv<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let schema_offset = self.schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let separator_offset = self.separator.as_ref().map(|v| fb.create_string(&v));
        let encoding_offset = self.encoding.as_ref().map(|v| fb.create_string(&v));
        let quote_offset = self.quote.as_ref().map(|v| fb.create_string(&v));
        let escape_offset = self.escape.as_ref().map(|v| fb.create_string(&v));
        let comment_offset = self.comment.as_ref().map(|v| fb.create_string(&v));
        let null_value_offset = self.null_value.as_ref().map(|v| fb.create_string(&v));
        let empty_value_offset = self.empty_value.as_ref().map(|v| fb.create_string(&v));
        let nan_value_offset = self.nan_value.as_ref().map(|v| fb.create_string(&v));
        let positive_inf_offset = self.positive_inf.as_ref().map(|v| fb.create_string(&v));
        let negative_inf_offset = self.negative_inf.as_ref().map(|v| fb.create_string(&v));
        let date_format_offset = self.date_format.as_ref().map(|v| fb.create_string(&v));
        let timestamp_format_offset = self.timestamp_format.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::ReadStepCsvBuilder::new(fb);
        schema_offset.map(|off| builder.add_schema(off));
        separator_offset.map(|off| builder.add_separator(off));
        encoding_offset.map(|off| builder.add_encoding(off));
        quote_offset.map(|off| builder.add_quote(off));
        escape_offset.map(|off| builder.add_escape(off));
        comment_offset.map(|off| builder.add_comment(off));
        self.header.map(|v| builder.add_header(v));
        self.enforce_schema.map(|v| builder.add_enforce_schema(v));
        self.infer_schema.map(|v| builder.add_infer_schema(v));
        self.ignore_leading_white_space
            .map(|v| builder.add_ignore_leading_white_space(v));
        self.ignore_trailing_white_space
            .map(|v| builder.add_ignore_trailing_white_space(v));
        null_value_offset.map(|off| builder.add_null_value(off));
        empty_value_offset.map(|off| builder.add_empty_value(off));
        nan_value_offset.map(|off| builder.add_nan_value(off));
        positive_inf_offset.map(|off| builder.add_positive_inf(off));
        negative_inf_offset.map(|off| builder.add_negative_inf(off));
        date_format_offset.map(|off| builder.add_date_format(off));
        timestamp_format_offset.map(|off| builder.add_timestamp_format(off));
        self.multi_line.map(|v| builder.add_multi_line(v));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepCsv<'fb>> for odf::ReadStepCsv {
    fn deserialize(proxy: fb::ReadStepCsv<'fb>) -> Self {
        odf::ReadStepCsv {
            schema: proxy
                .schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            separator: proxy.separator().map(|v| v.to_owned()),
            encoding: proxy.encoding().map(|v| v.to_owned()),
            quote: proxy.quote().map(|v| v.to_owned()),
            escape: proxy.escape().map(|v| v.to_owned()),
            comment: proxy.comment().map(|v| v.to_owned()),
            header: proxy.header().map(|v| v),
            enforce_schema: proxy.enforce_schema().map(|v| v),
            infer_schema: proxy.infer_schema().map(|v| v),
            ignore_leading_white_space: proxy.ignore_leading_white_space().map(|v| v),
            ignore_trailing_white_space: proxy.ignore_trailing_white_space().map(|v| v),
            null_value: proxy.null_value().map(|v| v.to_owned()),
            empty_value: proxy.empty_value().map(|v| v.to_owned()),
            nan_value: proxy.nan_value().map(|v| v.to_owned()),
            positive_inf: proxy.positive_inf().map(|v| v.to_owned()),
            negative_inf: proxy.negative_inf().map(|v| v.to_owned()),
            date_format: proxy.date_format().map(|v| v.to_owned()),
            timestamp_format: proxy.timestamp_format().map(|v| v.to_owned()),
            multi_line: proxy.multi_line().map(|v| v),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::ReadStepJsonLines {
    type OffsetT = WIPOffset<fb::ReadStepJsonLines<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let schema_offset = self.schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let date_format_offset = self.date_format.as_ref().map(|v| fb.create_string(&v));
        let encoding_offset = self.encoding.as_ref().map(|v| fb.create_string(&v));
        let timestamp_format_offset = self.timestamp_format.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::ReadStepJsonLinesBuilder::new(fb);
        schema_offset.map(|off| builder.add_schema(off));
        date_format_offset.map(|off| builder.add_date_format(off));
        encoding_offset.map(|off| builder.add_encoding(off));
        self.multi_line.map(|v| builder.add_multi_line(v));
        self.primitives_as_string
            .map(|v| builder.add_primitives_as_string(v));
        timestamp_format_offset.map(|off| builder.add_timestamp_format(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepJsonLines<'fb>> for odf::ReadStepJsonLines {
    fn deserialize(proxy: fb::ReadStepJsonLines<'fb>) -> Self {
        odf::ReadStepJsonLines {
            schema: proxy
                .schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            date_format: proxy.date_format().map(|v| v.to_owned()),
            encoding: proxy.encoding().map(|v| v.to_owned()),
            multi_line: proxy.multi_line().map(|v| v),
            primitives_as_string: proxy.primitives_as_string().map(|v| v),
            timestamp_format: proxy.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::ReadStepGeoJson {
    type OffsetT = WIPOffset<fb::ReadStepGeoJson<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let schema_offset = self.schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let mut builder = fb::ReadStepGeoJsonBuilder::new(fb);
        schema_offset.map(|off| builder.add_schema(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepGeoJson<'fb>> for odf::ReadStepGeoJson {
    fn deserialize(proxy: fb::ReadStepGeoJson<'fb>) -> Self {
        odf::ReadStepGeoJson {
            schema: proxy
                .schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::ReadStepEsriShapefile {
    type OffsetT = WIPOffset<fb::ReadStepEsriShapefile<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let schema_offset = self.schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let sub_path_offset = self.sub_path.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::ReadStepEsriShapefileBuilder::new(fb);
        schema_offset.map(|off| builder.add_schema(off));
        sub_path_offset.map(|off| builder.add_sub_path(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepEsriShapefile<'fb>> for odf::ReadStepEsriShapefile {
    fn deserialize(proxy: fb::ReadStepEsriShapefile<'fb>) -> Self {
        odf::ReadStepEsriShapefile {
            schema: proxy
                .schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            sub_path: proxy.sub_path().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::Transform> for odf::Transform {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::Transform, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::Transform::Sql(v) => (
                fb::Transform::TransformSql,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::Transform> for odf::Transform {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::Transform) -> Self {
        match t {
            fb::Transform::TransformSql => odf::Transform::Sql(odf::TransformSql::deserialize(
                fb::TransformSql::init_from_table(table),
            )),
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::TransformSql {
    type OffsetT = WIPOffset<fb::TransformSql<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let engine_offset = { fb.create_string(&self.engine) };
        let version_offset = self.version.as_ref().map(|v| fb.create_string(&v));
        let query_offset = self.query.as_ref().map(|v| fb.create_string(&v));
        let queries_offset = self.queries.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        });
        let temporal_tables_offset = self.temporal_tables.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        });
        let mut builder = fb::TransformSqlBuilder::new(fb);
        builder.add_engine(engine_offset);
        version_offset.map(|off| builder.add_version(off));
        query_offset.map(|off| builder.add_query(off));
        queries_offset.map(|off| builder.add_queries(off));
        temporal_tables_offset.map(|off| builder.add_temporal_tables(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TransformSql<'fb>> for odf::TransformSql {
    fn deserialize(proxy: fb::TransformSql<'fb>) -> Self {
        odf::TransformSql {
            engine: proxy.engine().map(|v| v.to_owned()).unwrap(),
            version: proxy.version().map(|v| v.to_owned()),
            query: proxy.query().map(|v| v.to_owned()),
            queries: proxy.queries().map(|v| {
                v.iter()
                    .map(|i| odf::SqlQueryStep::deserialize(i))
                    .collect()
            }),
            temporal_tables: proxy.temporal_tables().map(|v| {
                v.iter()
                    .map(|i| odf::TemporalTable::deserialize(i))
                    .collect()
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::FetchStep> for odf::FetchStep {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::FetchStep, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::FetchStep::Url(v) => (
                fb::FetchStep::FetchStepUrl,
                v.serialize(fb).as_union_value(),
            ),
            odf::FetchStep::FilesGlob(v) => (
                fb::FetchStep::FetchStepFilesGlob,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::FetchStep> for odf::FetchStep {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::FetchStep) -> Self {
        match t {
            fb::FetchStep::FetchStepUrl => odf::FetchStep::Url(odf::FetchStepUrl::deserialize(
                fb::FetchStepUrl::init_from_table(table),
            )),
            fb::FetchStep::FetchStepFilesGlob => {
                odf::FetchStep::FilesGlob(odf::FetchStepFilesGlob::deserialize(
                    fb::FetchStepFilesGlob::init_from_table(table),
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::FetchStepUrl {
    type OffsetT = WIPOffset<fb::FetchStepUrl<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let url_offset = { fb.create_string(&self.url) };
        let event_time_offset = self.event_time.as_ref().map(|v| v.serialize(fb));
        let cache_offset = self.cache.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::FetchStepUrlBuilder::new(fb);
        builder.add_url(url_offset);
        event_time_offset.map(|(e, off)| {
            builder.add_event_time_type(e);
            builder.add_event_time(off)
        });
        cache_offset.map(|(e, off)| {
            builder.add_cache_type(e);
            builder.add_cache(off)
        });
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FetchStepUrl<'fb>> for odf::FetchStepUrl {
    fn deserialize(proxy: fb::FetchStepUrl<'fb>) -> Self {
        odf::FetchStepUrl {
            url: proxy.url().map(|v| v.to_owned()).unwrap(),
            event_time: proxy
                .event_time()
                .map(|v| odf::EventTimeSource::deserialize(v, proxy.event_time_type())),
            cache: proxy
                .cache()
                .map(|v| odf::SourceCaching::deserialize(v, proxy.cache_type())),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::FetchStepFilesGlob {
    type OffsetT = WIPOffset<fb::FetchStepFilesGlob<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let path_offset = { fb.create_string(&self.path) };
        let event_time_offset = self.event_time.as_ref().map(|v| v.serialize(fb));
        let cache_offset = self.cache.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::FetchStepFilesGlobBuilder::new(fb);
        builder.add_path(path_offset);
        event_time_offset.map(|(e, off)| {
            builder.add_event_time_type(e);
            builder.add_event_time(off)
        });
        cache_offset.map(|(e, off)| {
            builder.add_cache_type(e);
            builder.add_cache(off)
        });
        self.order.map(|v| {
            builder.add_order(match v {
                odf::SourceOrdering::ByEventTime => fb::SourceOrdering::ByEventTime,
                odf::SourceOrdering::ByName => fb::SourceOrdering::ByName,
            })
        });
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FetchStepFilesGlob<'fb>> for odf::FetchStepFilesGlob {
    fn deserialize(proxy: fb::FetchStepFilesGlob<'fb>) -> Self {
        odf::FetchStepFilesGlob {
            path: proxy.path().map(|v| v.to_owned()).unwrap(),
            event_time: proxy
                .event_time()
                .map(|tbl| odf::EventTimeSource::deserialize(tbl, proxy.event_time_type())),
            cache: proxy
                .cache()
                .map(|tbl| odf::SourceCaching::deserialize(tbl, proxy.cache_type())),
            order: proxy.order().map(|v| match v {
                fb::SourceOrdering::ByEventTime => odf::SourceOrdering::ByEventTime,
                fb::SourceOrdering::ByName => odf::SourceOrdering::ByName,
                _ => panic!("Invalid enum value: {}", v.0),
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// PrepStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::PrepStep> for odf::PrepStep {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::PrepStep, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::PrepStep::Decompress(v) => (
                fb::PrepStep::PrepStepDecompress,
                v.serialize(fb).as_union_value(),
            ),
            odf::PrepStep::Pipe(v) => {
                (fb::PrepStep::PrepStepPipe, v.serialize(fb).as_union_value())
            }
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::PrepStep> for odf::PrepStep {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::PrepStep) -> Self {
        match t {
            fb::PrepStep::PrepStepDecompress => {
                odf::PrepStep::Decompress(odf::PrepStepDecompress::deserialize(
                    fb::PrepStepDecompress::init_from_table(table),
                ))
            }
            fb::PrepStep::PrepStepPipe => odf::PrepStep::Pipe(odf::PrepStepPipe::deserialize(
                fb::PrepStepPipe::init_from_table(table),
            )),
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::PrepStepDecompress {
    type OffsetT = WIPOffset<fb::PrepStepDecompress<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let sub_path_offset = self.sub_path.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::PrepStepDecompressBuilder::new(fb);
        builder.add_format(match self.format {
            odf::CompressionFormat::Gzip => fb::CompressionFormat::Gzip,
            odf::CompressionFormat::Zip => fb::CompressionFormat::Zip,
        });
        sub_path_offset.map(|off| builder.add_sub_path(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::PrepStepDecompress<'fb>> for odf::PrepStepDecompress {
    fn deserialize(proxy: fb::PrepStepDecompress<'fb>) -> Self {
        odf::PrepStepDecompress {
            format: match proxy.format() {
                fb::CompressionFormat::Gzip => odf::CompressionFormat::Gzip,
                fb::CompressionFormat::Zip => odf::CompressionFormat::Zip,
                _ => panic!("Invalid enum value: {}", proxy.format().0),
            },
            sub_path: proxy.sub_path().map(|v| v.to_owned()),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::PrepStepPipe {
    type OffsetT = WIPOffset<fb::PrepStepPipe<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let command_offset = {
            let offsets: Vec<_> = self.command.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        };
        let mut builder = fb::PrepStepPipeBuilder::new(fb);
        builder.add_command(command_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::PrepStepPipe<'fb>> for odf::PrepStepPipe {
    fn deserialize(proxy: fb::PrepStepPipe<'fb>) -> Self {
        odf::PrepStepPipe {
            command: proxy
                .command()
                .map(|v| v.iter().map(|i| i.to_owned()).collect())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// EventTimeSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::EventTimeSource> for odf::EventTimeSource {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::EventTimeSource, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::EventTimeSource::FromMetadata => (
                fb::EventTimeSource::EventTimeSourceFromMetadata,
                empty_table(fb).as_union_value(),
            ),
            odf::EventTimeSource::FromPath(v) => (
                fb::EventTimeSource::EventTimeSourceFromPath,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::EventTimeSource> for odf::EventTimeSource {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::EventTimeSource) -> Self {
        match t {
            fb::EventTimeSource::EventTimeSourceFromMetadata => odf::EventTimeSource::FromMetadata,
            fb::EventTimeSource::EventTimeSourceFromPath => {
                odf::EventTimeSource::FromPath(odf::EventTimeSourceFromPath::deserialize(
                    fb::EventTimeSourceFromPath::init_from_table(table),
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::EventTimeSourceFromPath {
    type OffsetT = WIPOffset<fb::EventTimeSourceFromPath<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let pattern_offset = { fb.create_string(&self.pattern) };
        let timestamp_format_offset = self.timestamp_format.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::EventTimeSourceFromPathBuilder::new(fb);
        builder.add_pattern(pattern_offset);
        timestamp_format_offset.map(|off| builder.add_timestamp_format(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::EventTimeSourceFromPath<'fb>>
    for odf::EventTimeSourceFromPath
{
    fn deserialize(proxy: fb::EventTimeSourceFromPath<'fb>) -> Self {
        odf::EventTimeSourceFromPath {
            pattern: proxy.pattern().map(|v| v.to_owned()).unwrap(),
            timestamp_format: proxy.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// Helpers
///////////////////////////////////////////////////////////////////////////////

fn datetime_to_fb(dt: &DateTime<Utc>) -> fb::Timestamp {
    fb::Timestamp::new(
        dt.year(),
        dt.ordinal() as u16,
        dt.naive_utc().num_seconds_from_midnight(),
        dt.naive_utc().nanosecond(),
    )
}

fn fb_to_datetime(dt: &fb::Timestamp) -> DateTime<Utc> {
    Utc.yo(dt.year(), dt.ordinal() as u32)
        .and_time(
            NaiveTime::from_num_seconds_from_midnight_opt(
                dt.seconds_from_midnight(),
                dt.nanoseconds(),
            )
            .unwrap(),
        )
        .unwrap()
}

fn interval_to_fb(iv: &odf::TimeInterval) -> fb::TimeInterval {
    use intervals_general::interval::Interval;
    match iv.0 {
        Interval::Closed { bound_pair: p } => fb::TimeInterval::new(
            fb::TimeIntervalType::Closed,
            &datetime_to_fb(p.left()),
            &datetime_to_fb(p.right()),
        ),
        Interval::Open { bound_pair: p } => fb::TimeInterval::new(
            fb::TimeIntervalType::Open,
            &datetime_to_fb(p.left()),
            &datetime_to_fb(p.right()),
        ),
        Interval::LeftHalfOpen { bound_pair: p } => fb::TimeInterval::new(
            fb::TimeIntervalType::LeftHalfOpen,
            &datetime_to_fb(p.left()),
            &datetime_to_fb(p.right()),
        ),
        Interval::RightHalfOpen { bound_pair: p } => fb::TimeInterval::new(
            fb::TimeIntervalType::RightHalfOpen,
            &datetime_to_fb(p.left()),
            &datetime_to_fb(p.right()),
        ),
        Interval::UnboundedClosedRight { right } => fb::TimeInterval::new(
            fb::TimeIntervalType::UnboundedClosedRight,
            &fb::Timestamp::default(),
            &datetime_to_fb(&right),
        ),
        Interval::UnboundedOpenRight { right } => fb::TimeInterval::new(
            fb::TimeIntervalType::UnboundedOpenRight,
            &fb::Timestamp::default(),
            &datetime_to_fb(&right),
        ),
        Interval::UnboundedClosedLeft { left } => fb::TimeInterval::new(
            fb::TimeIntervalType::UnboundedClosedLeft,
            &datetime_to_fb(&left),
            &fb::Timestamp::default(),
        ),
        Interval::UnboundedOpenLeft { left } => fb::TimeInterval::new(
            fb::TimeIntervalType::UnboundedOpenLeft,
            &datetime_to_fb(&left),
            &fb::Timestamp::default(),
        ),
        Interval::Singleton { at } => fb::TimeInterval::new(
            fb::TimeIntervalType::Singleton,
            &datetime_to_fb(&at),
            &fb::Timestamp::default(),
        ),
        Interval::Unbounded => fb::TimeInterval::new(
            fb::TimeIntervalType::Unbounded,
            &fb::Timestamp::default(),
            &fb::Timestamp::default(),
        ),
        Interval::Empty => fb::TimeInterval::new(
            fb::TimeIntervalType::Empty,
            &fb::Timestamp::default(),
            &fb::Timestamp::default(),
        ),
    }
}

fn fb_to_interval(iv: &fb::TimeInterval) -> odf::TimeInterval {
    match iv.type_() {
        fb::TimeIntervalType::Closed => {
            odf::TimeInterval::closed(fb_to_datetime(iv.left()), fb_to_datetime(iv.right()))
                .unwrap()
        }
        fb::TimeIntervalType::Open => {
            odf::TimeInterval::open(fb_to_datetime(iv.left()), fb_to_datetime(iv.right())).unwrap()
        }
        fb::TimeIntervalType::LeftHalfOpen => {
            odf::TimeInterval::left_half_open(fb_to_datetime(iv.left()), fb_to_datetime(iv.right()))
                .unwrap()
        }
        fb::TimeIntervalType::RightHalfOpen => odf::TimeInterval::right_half_open(
            fb_to_datetime(iv.left()),
            fb_to_datetime(iv.right()),
        )
        .unwrap(),
        fb::TimeIntervalType::UnboundedClosedRight => {
            odf::TimeInterval::unbounded_closed_right(fb_to_datetime(iv.right()))
        }
        fb::TimeIntervalType::UnboundedOpenRight => {
            odf::TimeInterval::unbounded_open_right(fb_to_datetime(iv.right()))
        }
        fb::TimeIntervalType::UnboundedClosedLeft => {
            odf::TimeInterval::unbounded_closed_left(fb_to_datetime(iv.left()))
        }
        fb::TimeIntervalType::UnboundedOpenLeft => {
            odf::TimeInterval::unbounded_open_left(fb_to_datetime(iv.left()))
        }
        fb::TimeIntervalType::Singleton => odf::TimeInterval::singleton(fb_to_datetime(iv.left())),
        fb::TimeIntervalType::Unbounded => odf::TimeInterval::unbounded(),
        fb::TimeIntervalType::Empty => odf::TimeInterval::empty(),
        _ => panic!("Invalid enum value: {}", iv.type_().0),
    }
}

fn empty_table<'fb>(
    fb: &mut FlatBufferBuilder<'fb>,
) -> WIPOffset<flatbuffers::TableFinishedWIPOffset> {
    let wip = fb.start_table();
    fb.end_table(wip)
}
