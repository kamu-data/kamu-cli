////////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
////////////////////////////////////////////////////////////////////////////////

use super::odf_generated as fb;
use crate as odf;
use ::flatbuffers::{FlatBufferBuilder, Table, UnionWIPOffset, WIPOffset};
use chrono::prelude::*;
use std::convert::{TryFrom, TryInto};

pub trait FlatbuffersSerializable<'fb> {
    type OffsetT;
    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT;
}

pub trait FlatbuffersDeserializable<T> {
    fn deserialize(fb: T) -> Self;
}

trait FlatbuffersEnumSerializable<'fb, E> {
    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> (E, Option<WIPOffset<UnionWIPOffset>>);
}

trait FlatbuffersEnumDeserializable<'fb, E> {
    fn deserialize(table: Option<Table<'fb>>, t: E) -> Option<Self>
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
    ) -> (fb::DatasetSource, Option<WIPOffset<UnionWIPOffset>>) {
        match self {
            odf::DatasetSource::Root(v) => (
                fb::DatasetSource::DatasetSourceRoot,
                Some(v.serialize(fb).as_union_value()),
            ),
            odf::DatasetSource::Derivative(v) => (
                fb::DatasetSource::DatasetSourceDerivative,
                Some(v.serialize(fb).as_union_value()),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::DatasetSource> for odf::DatasetSource {
    fn deserialize(table: Option<flatbuffers::Table<'fb>>, t: fb::DatasetSource) -> Option<Self> {
        match t {
            fb::DatasetSource::NONE => None,
            fb::DatasetSource::DatasetSourceRoot => Some(odf::DatasetSource::Root(
                odf::DatasetSourceRoot::deserialize(fb::DatasetSourceRoot::init_from_table(
                    table.unwrap(),
                )),
            )),
            fb::DatasetSource::DatasetSourceDerivative => Some(odf::DatasetSource::Derivative(
                odf::DatasetSourceDerivative::deserialize(
                    fb::DatasetSourceDerivative::init_from_table(table.unwrap()),
                ),
            )),
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
                    value_offset.map(|off| builder.add_value(off));
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
        fetch_offset.1.map(|off| builder.add_fetch(off));
        prepare_offset.map(|off| builder.add_prepare(off));
        builder.add_read_type(read_offset.0);
        read_offset.1.map(|off| builder.add_read(off));
        preprocess_offset.map(|off| {
            builder.add_preprocess_type(off.0);
            off.1.map(|v| builder.add_preprocess(v));
        });
        builder.add_merge_type(merge_offset.0);
        merge_offset.1.map(|off| builder.add_merge(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DatasetSourceRoot<'fb>> for odf::DatasetSourceRoot {
    fn deserialize(proxy: fb::DatasetSourceRoot<'fb>) -> Self {
        odf::DatasetSourceRoot {
            fetch: odf::FetchStep::deserialize(proxy.fetch(), proxy.fetch_type()).unwrap(),
            prepare: proxy.prepare().map(|v| {
                v.iter()
                    .map(|i| odf::PrepStep::deserialize(i.value(), i.value_type()).unwrap())
                    .collect()
            }),
            read: odf::ReadStep::deserialize(proxy.read(), proxy.read_type()).unwrap(),
            preprocess: odf::Transform::deserialize(proxy.preprocess(), proxy.preprocess_type()),
            merge: odf::MergeStrategy::deserialize(proxy.merge(), proxy.merge_type()).unwrap(),
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
        transform_offset.1.map(|off| builder.add_transform(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DatasetSourceDerivative<'fb>>
    for odf::DatasetSourceDerivative
{
    fn deserialize(proxy: fb::DatasetSourceDerivative<'fb>) -> Self {
        use std::str::FromStr;

        odf::DatasetSourceDerivative {
            inputs: proxy
                .inputs()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::DatasetIDBuf::from_str(i).unwrap())
                        .collect()
                })
                .unwrap(),
            transform: odf::Transform::deserialize(proxy.transform(), proxy.transform_type())
                .unwrap(),
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
    ) -> (fb::SourceCaching, Option<WIPOffset<UnionWIPOffset>>) {
        match self {
            odf::SourceCaching::Forever => (
                fb::SourceCaching::SourceCachingForever,
                Some(empty_table(fb).as_union_value()),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::SourceCaching> for odf::SourceCaching {
    fn deserialize(_table: Option<flatbuffers::Table<'fb>>, t: fb::SourceCaching) -> Option<Self> {
        match t {
            fb::SourceCaching::NONE => None,
            fb::SourceCaching::SourceCachingForever => Some(odf::SourceCaching::Forever),
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
        source_offset.1.map(|off| builder.add_source(off));
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
            source: odf::DatasetSource::deserialize(proxy.source(), proxy.source_type()).unwrap(),
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
        let interval = interval_to_fb(&self.interval);
        builder.add_interval(&interval);
        let num_records = fb::Option_int64::new(self.num_records);
        builder.add_num_records(&num_records);
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
            interval: proxy.interval().map(|v| fb_to_interval(v)).unwrap(),
            num_records: proxy.num_records().map(|v| v.value()).unwrap(),
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
    ) -> (fb::MergeStrategy, Option<WIPOffset<UnionWIPOffset>>) {
        match self {
            odf::MergeStrategy::Append => (
                fb::MergeStrategy::MergeStrategyAppend,
                Some(empty_table(fb).as_union_value()),
            ),
            odf::MergeStrategy::Ledger(v) => (
                fb::MergeStrategy::MergeStrategyLedger,
                Some(v.serialize(fb).as_union_value()),
            ),
            odf::MergeStrategy::Snapshot(v) => (
                fb::MergeStrategy::MergeStrategySnapshot,
                Some(v.serialize(fb).as_union_value()),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::MergeStrategy> for odf::MergeStrategy {
    fn deserialize(table: Option<flatbuffers::Table<'fb>>, t: fb::MergeStrategy) -> Option<Self> {
        match t {
            fb::MergeStrategy::NONE => None,
            fb::MergeStrategy::MergeStrategyAppend => Some(odf::MergeStrategy::Append),
            fb::MergeStrategy::MergeStrategyLedger => Some(odf::MergeStrategy::Ledger(
                odf::MergeStrategyLedger::deserialize(fb::MergeStrategyLedger::init_from_table(
                    table.unwrap(),
                )),
            )),
            fb::MergeStrategy::MergeStrategySnapshot => Some(odf::MergeStrategy::Snapshot(
                odf::MergeStrategySnapshot::deserialize(
                    fb::MergeStrategySnapshot::init_from_table(table.unwrap()),
                ),
            )),
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
        let system_time = datetime_to_fb(&self.system_time);
        builder.add_system_time(&system_time);
        output_slice_offset.map(|off| builder.add_output_slice(off));
        let output_watermark = self.output_watermark.map(|t| datetime_to_fb(&t));
        self.output_watermark
            .as_ref()
            .map(|_| builder.add_output_watermark(output_watermark.as_ref().unwrap()));
        input_slices_offset.map(|off| builder.add_input_slices(off));
        source_offset.map(|off| {
            builder.add_source_type(off.0);
            off.1.map(|v| builder.add_source(v));
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
            source: odf::DatasetSource::deserialize(proxy.source(), proxy.source_type()),
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
    ) -> (fb::ReadStep, Option<WIPOffset<UnionWIPOffset>>) {
        match self {
            odf::ReadStep::Csv(v) => (
                fb::ReadStep::ReadStepCsv,
                Some(v.serialize(fb).as_union_value()),
            ),
            odf::ReadStep::JsonLines(v) => (
                fb::ReadStep::ReadStepJsonLines,
                Some(v.serialize(fb).as_union_value()),
            ),
            odf::ReadStep::GeoJson(v) => (
                fb::ReadStep::ReadStepGeoJson,
                Some(v.serialize(fb).as_union_value()),
            ),
            odf::ReadStep::EsriShapefile(v) => (
                fb::ReadStep::ReadStepEsriShapefile,
                Some(v.serialize(fb).as_union_value()),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::ReadStep> for odf::ReadStep {
    fn deserialize(table: Option<flatbuffers::Table<'fb>>, t: fb::ReadStep) -> Option<Self> {
        match t {
            fb::ReadStep::NONE => None,
            fb::ReadStep::ReadStepCsv => Some(odf::ReadStep::Csv(odf::ReadStepCsv::deserialize(
                fb::ReadStepCsv::init_from_table(table.unwrap()),
            ))),
            fb::ReadStep::ReadStepJsonLines => Some(odf::ReadStep::JsonLines(
                odf::ReadStepJsonLines::deserialize(fb::ReadStepJsonLines::init_from_table(
                    table.unwrap(),
                )),
            )),
            fb::ReadStep::ReadStepGeoJson => {
                Some(odf::ReadStep::GeoJson(odf::ReadStepGeoJson::deserialize(
                    fb::ReadStepGeoJson::init_from_table(table.unwrap()),
                )))
            }
            fb::ReadStep::ReadStepEsriShapefile => Some(odf::ReadStep::EsriShapefile(
                odf::ReadStepEsriShapefile::deserialize(
                    fb::ReadStepEsriShapefile::init_from_table(table.unwrap()),
                ),
            )),
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
        let header = self.header.map(|v| fb::Option_bool::new(v));
        self.header
            .as_ref()
            .map(|_| builder.add_header(header.as_ref().unwrap()));
        let enforce_schema = self.enforce_schema.map(|v| fb::Option_bool::new(v));
        self.enforce_schema
            .as_ref()
            .map(|_| builder.add_enforce_schema(enforce_schema.as_ref().unwrap()));
        let infer_schema = self.infer_schema.map(|v| fb::Option_bool::new(v));
        self.infer_schema
            .as_ref()
            .map(|_| builder.add_infer_schema(infer_schema.as_ref().unwrap()));
        let ignore_leading_white_space = self
            .ignore_leading_white_space
            .map(|v| fb::Option_bool::new(v));
        self.ignore_leading_white_space.as_ref().map(|_| {
            builder.add_ignore_leading_white_space(ignore_leading_white_space.as_ref().unwrap())
        });
        let ignore_trailing_white_space = self
            .ignore_trailing_white_space
            .map(|v| fb::Option_bool::new(v));
        self.ignore_trailing_white_space.as_ref().map(|_| {
            builder.add_ignore_trailing_white_space(ignore_trailing_white_space.as_ref().unwrap())
        });
        null_value_offset.map(|off| builder.add_null_value(off));
        empty_value_offset.map(|off| builder.add_empty_value(off));
        nan_value_offset.map(|off| builder.add_nan_value(off));
        positive_inf_offset.map(|off| builder.add_positive_inf(off));
        negative_inf_offset.map(|off| builder.add_negative_inf(off));
        date_format_offset.map(|off| builder.add_date_format(off));
        timestamp_format_offset.map(|off| builder.add_timestamp_format(off));
        let multi_line = self.multi_line.map(|v| fb::Option_bool::new(v));
        self.multi_line
            .as_ref()
            .map(|_| builder.add_multi_line(multi_line.as_ref().unwrap()));
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
            header: proxy.header().map(|v| v.value()),
            enforce_schema: proxy.enforce_schema().map(|v| v.value()),
            infer_schema: proxy.infer_schema().map(|v| v.value()),
            ignore_leading_white_space: proxy.ignore_leading_white_space().map(|v| v.value()),
            ignore_trailing_white_space: proxy.ignore_trailing_white_space().map(|v| v.value()),
            null_value: proxy.null_value().map(|v| v.to_owned()),
            empty_value: proxy.empty_value().map(|v| v.to_owned()),
            nan_value: proxy.nan_value().map(|v| v.to_owned()),
            positive_inf: proxy.positive_inf().map(|v| v.to_owned()),
            negative_inf: proxy.negative_inf().map(|v| v.to_owned()),
            date_format: proxy.date_format().map(|v| v.to_owned()),
            timestamp_format: proxy.timestamp_format().map(|v| v.to_owned()),
            multi_line: proxy.multi_line().map(|v| v.value()),
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
        let multi_line = self.multi_line.map(|v| fb::Option_bool::new(v));
        self.multi_line
            .as_ref()
            .map(|_| builder.add_multi_line(multi_line.as_ref().unwrap()));
        let primitives_as_string = self.primitives_as_string.map(|v| fb::Option_bool::new(v));
        self.primitives_as_string
            .as_ref()
            .map(|_| builder.add_primitives_as_string(primitives_as_string.as_ref().unwrap()));
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
            multi_line: proxy.multi_line().map(|v| v.value()),
            primitives_as_string: proxy.primitives_as_string().map(|v| v.value()),
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
    ) -> (fb::Transform, Option<WIPOffset<UnionWIPOffset>>) {
        match self {
            odf::Transform::Sql(v) => (
                fb::Transform::TransformSql,
                Some(v.serialize(fb).as_union_value()),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::Transform> for odf::Transform {
    fn deserialize(table: Option<flatbuffers::Table<'fb>>, t: fb::Transform) -> Option<Self> {
        match t {
            fb::Transform::NONE => None,
            fb::Transform::TransformSql => Some(odf::Transform::Sql(
                odf::TransformSql::deserialize(fb::TransformSql::init_from_table(table.unwrap())),
            )),
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
    ) -> (fb::FetchStep, Option<WIPOffset<UnionWIPOffset>>) {
        match self {
            odf::FetchStep::Url(v) => (
                fb::FetchStep::FetchStepUrl,
                Some(v.serialize(fb).as_union_value()),
            ),
            odf::FetchStep::FilesGlob(v) => (
                fb::FetchStep::FetchStepFilesGlob,
                Some(v.serialize(fb).as_union_value()),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::FetchStep> for odf::FetchStep {
    fn deserialize(table: Option<flatbuffers::Table<'fb>>, t: fb::FetchStep) -> Option<Self> {
        match t {
            fb::FetchStep::NONE => panic!("Property is missing"),
            fb::FetchStep::FetchStepUrl => Some(odf::FetchStep::Url(
                odf::FetchStepUrl::deserialize(fb::FetchStepUrl::init_from_table(table.unwrap())),
            )),
            fb::FetchStep::FetchStepFilesGlob => Some(odf::FetchStep::FilesGlob(
                odf::FetchStepFilesGlob::deserialize(fb::FetchStepFilesGlob::init_from_table(
                    table.unwrap(),
                )),
            )),
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
        event_time_offset.map(|off| {
            builder.add_event_time_type(off.0);
            off.1.map(|v| builder.add_event_time(v));
        });
        cache_offset.map(|off| {
            builder.add_cache_type(off.0);
            off.1.map(|v| builder.add_cache(v));
        });
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FetchStepUrl<'fb>> for odf::FetchStepUrl {
    fn deserialize(proxy: fb::FetchStepUrl<'fb>) -> Self {
        odf::FetchStepUrl {
            url: proxy.url().map(|v| v.to_owned()).unwrap(),
            event_time: odf::EventTimeSource::deserialize(
                proxy.event_time(),
                proxy.event_time_type(),
            ),
            cache: odf::SourceCaching::deserialize(proxy.cache(), proxy.cache_type()),
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
        event_time_offset.map(|off| {
            builder.add_event_time_type(off.0);
            off.1.map(|v| builder.add_event_time(v));
        });
        cache_offset.map(|off| {
            builder.add_cache_type(off.0);
            off.1.map(|v| builder.add_cache(v));
        });
        let order = self.order.map(|v| {
            fb::Option_SourceOrdering::new(match v {
                odf::SourceOrdering::ByEventTime => fb::SourceOrdering::ByEventTime,
                odf::SourceOrdering::ByName => fb::SourceOrdering::ByName,
            })
        });
        self.order
            .as_ref()
            .map(|_| builder.add_order(order.as_ref().unwrap()));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FetchStepFilesGlob<'fb>> for odf::FetchStepFilesGlob {
    fn deserialize(proxy: fb::FetchStepFilesGlob<'fb>) -> Self {
        odf::FetchStepFilesGlob {
            path: proxy.path().map(|v| v.to_owned()).unwrap(),
            event_time: odf::EventTimeSource::deserialize(
                proxy.event_time(),
                proxy.event_time_type(),
            ),
            cache: odf::SourceCaching::deserialize(proxy.cache(), proxy.cache_type()),
            order: proxy.order().map(|v| match v.value() {
                fb::SourceOrdering::ByEventTime => odf::SourceOrdering::ByEventTime,
                fb::SourceOrdering::ByName => odf::SourceOrdering::ByName,
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
    ) -> (fb::PrepStep, Option<WIPOffset<UnionWIPOffset>>) {
        match self {
            odf::PrepStep::Decompress(v) => (
                fb::PrepStep::PrepStepDecompress,
                Some(v.serialize(fb).as_union_value()),
            ),
            odf::PrepStep::Pipe(v) => (
                fb::PrepStep::PrepStepPipe,
                Some(v.serialize(fb).as_union_value()),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::PrepStep> for odf::PrepStep {
    fn deserialize(table: Option<flatbuffers::Table<'fb>>, t: fb::PrepStep) -> Option<Self> {
        match t {
            fb::PrepStep::NONE => None,
            fb::PrepStep::PrepStepDecompress => Some(odf::PrepStep::Decompress(
                odf::PrepStepDecompress::deserialize(fb::PrepStepDecompress::init_from_table(
                    table.unwrap(),
                )),
            )),
            fb::PrepStep::PrepStepPipe => Some(odf::PrepStep::Pipe(
                odf::PrepStepPipe::deserialize(fb::PrepStepPipe::init_from_table(table.unwrap())),
            )),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::PrepStepDecompress {
    type OffsetT = WIPOffset<fb::PrepStepDecompress<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let sub_path_offset = self.sub_path.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::PrepStepDecompressBuilder::new(fb);
        let format = fb::Option_CompressionFormat::new(match self.format {
            odf::CompressionFormat::Gzip => fb::CompressionFormat::Gzip,
            odf::CompressionFormat::Zip => fb::CompressionFormat::Zip,
        });
        builder.add_format(&format);
        sub_path_offset.map(|off| builder.add_sub_path(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::PrepStepDecompress<'fb>> for odf::PrepStepDecompress {
    fn deserialize(proxy: fb::PrepStepDecompress<'fb>) -> Self {
        odf::PrepStepDecompress {
            format: proxy
                .format()
                .map(|v| match v.value() {
                    fb::CompressionFormat::Gzip => odf::CompressionFormat::Gzip,
                    fb::CompressionFormat::Zip => odf::CompressionFormat::Zip,
                })
                .unwrap(),
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
    ) -> (fb::EventTimeSource, Option<WIPOffset<UnionWIPOffset>>) {
        match self {
            odf::EventTimeSource::FromMetadata => (
                fb::EventTimeSource::EventTimeSourceFromMetadata,
                Some(empty_table(fb).as_union_value()),
            ),
            odf::EventTimeSource::FromPath(v) => (
                fb::EventTimeSource::EventTimeSourceFromPath,
                Some(v.serialize(fb).as_union_value()),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::EventTimeSource> for odf::EventTimeSource {
    fn deserialize(table: Option<flatbuffers::Table<'fb>>, t: fb::EventTimeSource) -> Option<Self> {
        match t {
            fb::EventTimeSource::NONE => None,
            fb::EventTimeSource::EventTimeSourceFromMetadata => {
                Some(odf::EventTimeSource::FromMetadata)
            }
            fb::EventTimeSource::EventTimeSourceFromPath => Some(odf::EventTimeSource::FromPath(
                odf::EventTimeSourceFromPath::deserialize(
                    fb::EventTimeSourceFromPath::init_from_table(table.unwrap()),
                ),
            )),
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
            &datetime_to_fb(&Utc.yo(0, 1).and_hms(0, 0, 0)),
            &datetime_to_fb(&right),
        ),
        Interval::UnboundedOpenRight { right } => fb::TimeInterval::new(
            fb::TimeIntervalType::UnboundedOpenRight,
            &datetime_to_fb(&Utc.yo(0, 1).and_hms(0, 0, 0)),
            &datetime_to_fb(&right),
        ),
        Interval::UnboundedClosedLeft { left } => fb::TimeInterval::new(
            fb::TimeIntervalType::UnboundedClosedLeft,
            &datetime_to_fb(&left),
            &datetime_to_fb(&Utc.yo(0, 1).and_hms(0, 0, 0)),
        ),
        Interval::UnboundedOpenLeft { left } => fb::TimeInterval::new(
            fb::TimeIntervalType::UnboundedOpenLeft,
            &datetime_to_fb(&left),
            &datetime_to_fb(&Utc.yo(0, 1).and_hms(0, 0, 0)),
        ),
        Interval::Singleton { at } => fb::TimeInterval::new(
            fb::TimeIntervalType::Singleton,
            &datetime_to_fb(&at),
            &datetime_to_fb(&Utc.yo(0, 1).and_hms(0, 0, 0)),
        ),
        Interval::Unbounded => fb::TimeInterval::new(
            fb::TimeIntervalType::Unbounded,
            &datetime_to_fb(&Utc.yo(0, 1).and_hms(0, 0, 0)),
            &datetime_to_fb(&Utc.yo(0, 1).and_hms(0, 0, 0)),
        ),
        Interval::Empty => fb::TimeInterval::new(
            fb::TimeIntervalType::Empty,
            &datetime_to_fb(&Utc.yo(0, 1).and_hms(0, 0, 0)),
            &datetime_to_fb(&Utc.yo(0, 1).and_hms(0, 0, 0)),
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
    }
}

fn empty_table<'fb>(
    fb: &mut FlatBufferBuilder<'fb>,
) -> WIPOffset<flatbuffers::TableFinishedWIPOffset> {
    let wip = fb.start_table();
    fb.end_table(wip)
}
