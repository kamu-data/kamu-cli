// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
////////////////////////////////////////////////////////////////////////////////

#![allow(unused_variables)]
use super::proxies_generated as fb;
mod odf {
    pub use crate::dtos::*;
    pub use crate::formats::*;
    pub use crate::identity::*;
}
use ::flatbuffers::{FlatBufferBuilder, Table, UnionWIPOffset, WIPOffset};
use chrono::prelude::*;
use std::convert::TryFrom;
use std::path::PathBuf;

pub trait FlatbuffersSerializable<'fb> {
    type OffsetT;
    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT;
}

pub trait FlatbuffersDeserializable<T> {
    fn deserialize(fb: T) -> Self;
}

pub trait FlatbuffersEnumSerializable<'fb, E> {
    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> (E, WIPOffset<UnionWIPOffset>);
}

pub trait FlatbuffersEnumDeserializable<'fb, E> {
    fn deserialize(table: Table<'fb>, t: E) -> Self
    where
        Self: Sized;
}

////////////////////////////////////////////////////////////////////////////////
// AddData
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::AddData {
    type OffsetT = WIPOffset<fb::AddData<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let input_checkpoint_offset = self
            .input_checkpoint
            .as_ref()
            .map(|v| fb.create_vector(&v.to_bytes()));
        let output_data_offset = { self.output_data.serialize(fb) };
        let output_checkpoint_offset = self.output_checkpoint.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::AddDataBuilder::new(fb);
        input_checkpoint_offset.map(|off| builder.add_input_checkpoint(off));
        builder.add_output_data(output_data_offset);
        output_checkpoint_offset.map(|off| builder.add_output_checkpoint(off));
        self.output_watermark
            .map(|v| builder.add_output_watermark(&datetime_to_fb(&v)));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::AddData<'fb>> for odf::AddData {
    fn deserialize(proxy: fb::AddData<'fb>) -> Self {
        odf::AddData {
            input_checkpoint: proxy
                .input_checkpoint()
                .map(|v| odf::Multihash::from_bytes(v).unwrap()),
            output_data: proxy
                .output_data()
                .map(|v| odf::DataSlice::deserialize(v))
                .unwrap(),
            output_checkpoint: proxy
                .output_checkpoint()
                .map(|v| odf::Checkpoint::deserialize(v)),
            output_watermark: proxy.output_watermark().map(|v| fb_to_datetime(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// AttachmentEmbedded
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentembedded-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::AttachmentEmbedded {
    type OffsetT = WIPOffset<fb::AttachmentEmbedded<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let path_offset = { fb.create_string(&self.path) };
        let content_offset = { fb.create_string(&self.content) };
        let mut builder = fb::AttachmentEmbeddedBuilder::new(fb);
        builder.add_path(path_offset);
        builder.add_content(content_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::AttachmentEmbedded<'fb>> for odf::AttachmentEmbedded {
    fn deserialize(proxy: fb::AttachmentEmbedded<'fb>) -> Self {
        odf::AttachmentEmbedded {
            path: proxy.path().map(|v| v.to_owned()).unwrap(),
            content: proxy.content().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Attachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachments-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::Attachments> for odf::Attachments {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::Attachments, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::Attachments::Embedded(v) => (
                fb::Attachments::AttachmentsEmbedded,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::Attachments> for odf::Attachments {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::Attachments) -> Self {
        match t {
            fb::Attachments::AttachmentsEmbedded => {
                odf::Attachments::Embedded(odf::AttachmentsEmbedded::deserialize(
                    fb::AttachmentsEmbedded::init_from_table(table),
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::AttachmentsEmbedded {
    type OffsetT = WIPOffset<fb::AttachmentsEmbedded<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let items_offset = {
            let offsets: Vec<_> = self.items.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        };
        let mut builder = fb::AttachmentsEmbeddedBuilder::new(fb);
        builder.add_items(items_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::AttachmentsEmbedded<'fb>> for odf::AttachmentsEmbedded {
    fn deserialize(proxy: fb::AttachmentsEmbedded<'fb>) -> Self {
        odf::AttachmentsEmbedded {
            items: proxy
                .items()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::AttachmentEmbedded::deserialize(i))
                        .collect()
                })
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// BlockInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#blockinterval-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::BlockInterval {
    type OffsetT = WIPOffset<fb::BlockInterval<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let start_offset = { fb.create_vector(&self.start.to_bytes()) };
        let end_offset = { fb.create_vector(&self.end.to_bytes()) };
        let mut builder = fb::BlockIntervalBuilder::new(fb);
        builder.add_start(start_offset);
        builder.add_end(end_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::BlockInterval<'fb>> for odf::BlockInterval {
    fn deserialize(proxy: fb::BlockInterval<'fb>) -> Self {
        odf::BlockInterval {
            start: proxy
                .start()
                .map(|v| odf::Multihash::from_bytes(v).unwrap())
                .unwrap(),
            end: proxy
                .end()
                .map(|v| odf::Multihash::from_bytes(v).unwrap())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Checkpoint
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::Checkpoint {
    type OffsetT = WIPOffset<fb::Checkpoint<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let physical_hash_offset = { fb.create_vector(&self.physical_hash.to_bytes()) };
        let mut builder = fb::CheckpointBuilder::new(fb);
        builder.add_physical_hash(physical_hash_offset);
        builder.add_size_(self.size);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Checkpoint<'fb>> for odf::Checkpoint {
    fn deserialize(proxy: fb::Checkpoint<'fb>) -> Self {
        odf::Checkpoint {
            physical_hash: proxy
                .physical_hash()
                .map(|v| odf::Multihash::from_bytes(v).unwrap())
                .unwrap(),
            size: proxy.size_(),
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
        let logical_hash_offset = { fb.create_vector(&self.logical_hash.to_bytes()) };
        let physical_hash_offset = { fb.create_vector(&self.physical_hash.to_bytes()) };
        let interval_offset = { self.interval.serialize(fb) };
        let mut builder = fb::DataSliceBuilder::new(fb);
        builder.add_logical_hash(logical_hash_offset);
        builder.add_physical_hash(physical_hash_offset);
        builder.add_interval(interval_offset);
        builder.add_size_(self.size);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataSlice<'fb>> for odf::DataSlice {
    fn deserialize(proxy: fb::DataSlice<'fb>) -> Self {
        odf::DataSlice {
            logical_hash: proxy
                .logical_hash()
                .map(|v| odf::Multihash::from_bytes(v).unwrap())
                .unwrap(),
            physical_hash: proxy
                .physical_hash()
                .map(|v| odf::Multihash::from_bytes(v).unwrap())
                .unwrap(),
            interval: proxy
                .interval()
                .map(|v| odf::OffsetInterval::deserialize(v))
                .unwrap(),
            size: proxy.size_(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DatasetKind
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetkind-schema
////////////////////////////////////////////////////////////////////////////////

impl From<odf::DatasetKind> for fb::DatasetKind {
    fn from(v: odf::DatasetKind) -> Self {
        match v {
            odf::DatasetKind::Root => fb::DatasetKind::Root,
            odf::DatasetKind::Derivative => fb::DatasetKind::Derivative,
        }
    }
}

impl Into<odf::DatasetKind> for fb::DatasetKind {
    fn into(self) -> odf::DatasetKind {
        match self {
            fb::DatasetKind::Root => odf::DatasetKind::Root,
            fb::DatasetKind::Derivative => odf::DatasetKind::Derivative,
            _ => panic!("Invalid enum value: {}", self.0),
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
        let offset_column_offset = self.offset_column.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::DatasetVocabularyBuilder::new(fb);
        system_time_column_offset.map(|off| builder.add_system_time_column(off));
        event_time_column_offset.map(|off| builder.add_event_time_column(off));
        offset_column_offset.map(|off| builder.add_offset_column(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DatasetVocabulary<'fb>> for odf::DatasetVocabulary {
    fn deserialize(proxy: fb::DatasetVocabulary<'fb>) -> Self {
        odf::DatasetVocabulary {
            system_time_column: proxy.system_time_column().map(|v| v.to_owned()),
            event_time_column: proxy.event_time_column().map(|v| v.to_owned()),
            offset_column: proxy.offset_column().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// EnvVar
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#envvar-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::EnvVar {
    type OffsetT = WIPOffset<fb::EnvVar<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let name_offset = { fb.create_string(&self.name) };
        let value_offset = self.value.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::EnvVarBuilder::new(fb);
        builder.add_name(name_offset);
        value_offset.map(|off| builder.add_value(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::EnvVar<'fb>> for odf::EnvVar {
    fn deserialize(proxy: fb::EnvVar<'fb>) -> Self {
        odf::EnvVar {
            name: proxy.name().map(|v| v.to_owned()).unwrap(),
            value: proxy.value().map(|v| v.to_owned()),
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

////////////////////////////////////////////////////////////////////////////////
// ExecuteQuery
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequery-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::ExecuteQuery {
    type OffsetT = WIPOffset<fb::ExecuteQuery<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let input_slices_offset = {
            let offsets: Vec<_> = self.input_slices.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        };
        let input_checkpoint_offset = self
            .input_checkpoint
            .as_ref()
            .map(|v| fb.create_vector(&v.to_bytes()));
        let output_data_offset = self.output_data.as_ref().map(|v| v.serialize(fb));
        let output_checkpoint_offset = self.output_checkpoint.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::ExecuteQueryBuilder::new(fb);
        builder.add_input_slices(input_slices_offset);
        input_checkpoint_offset.map(|off| builder.add_input_checkpoint(off));
        output_data_offset.map(|off| builder.add_output_data(off));
        output_checkpoint_offset.map(|off| builder.add_output_checkpoint(off));
        self.output_watermark
            .map(|v| builder.add_output_watermark(&datetime_to_fb(&v)));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ExecuteQuery<'fb>> for odf::ExecuteQuery {
    fn deserialize(proxy: fb::ExecuteQuery<'fb>) -> Self {
        odf::ExecuteQuery {
            input_slices: proxy
                .input_slices()
                .map(|v| v.iter().map(|i| odf::InputSlice::deserialize(i)).collect())
                .unwrap(),
            input_checkpoint: proxy
                .input_checkpoint()
                .map(|v| odf::Multihash::from_bytes(v).unwrap()),
            output_data: proxy.output_data().map(|v| odf::DataSlice::deserialize(v)),
            output_checkpoint: proxy
                .output_checkpoint()
                .map(|v| odf::Checkpoint::deserialize(v)),
            output_watermark: proxy.output_watermark().map(|v| fb_to_datetime(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryinput-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::ExecuteQueryInput {
    type OffsetT = WIPOffset<fb::ExecuteQueryInput<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let dataset_id_offset = { fb.create_vector(&self.dataset_id.to_bytes()) };
        let dataset_name_offset = { fb.create_string(&self.dataset_name) };
        let vocab_offset = { self.vocab.serialize(fb) };
        let data_interval_offset = self.data_interval.as_ref().map(|v| v.serialize(fb));
        let data_paths_offset = {
            let offsets: Vec<_> = self
                .data_paths
                .iter()
                .map(|i| fb.create_string(i.to_str().unwrap()))
                .collect();
            fb.create_vector(&offsets)
        };
        let schema_file_offset = { fb.create_string(self.schema_file.to_str().unwrap()) };
        let explicit_watermarks_offset = {
            let offsets: Vec<_> = self
                .explicit_watermarks
                .iter()
                .map(|i| i.serialize(fb))
                .collect();
            fb.create_vector(&offsets)
        };
        let mut builder = fb::ExecuteQueryInputBuilder::new(fb);
        builder.add_dataset_id(dataset_id_offset);
        builder.add_dataset_name(dataset_name_offset);
        builder.add_vocab(vocab_offset);
        data_interval_offset.map(|off| builder.add_data_interval(off));
        builder.add_data_paths(data_paths_offset);
        builder.add_schema_file(schema_file_offset);
        builder.add_explicit_watermarks(explicit_watermarks_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ExecuteQueryInput<'fb>> for odf::ExecuteQueryInput {
    fn deserialize(proxy: fb::ExecuteQueryInput<'fb>) -> Self {
        odf::ExecuteQueryInput {
            dataset_id: proxy
                .dataset_id()
                .map(|v| odf::DatasetID::from_bytes(v).unwrap())
                .unwrap(),
            dataset_name: proxy
                .dataset_name()
                .map(|v| odf::DatasetName::try_from(v).unwrap())
                .unwrap(),
            vocab: proxy
                .vocab()
                .map(|v| odf::DatasetVocabulary::deserialize(v))
                .unwrap(),
            data_interval: proxy
                .data_interval()
                .map(|v| odf::OffsetInterval::deserialize(v)),
            data_paths: proxy
                .data_paths()
                .map(|v| v.iter().map(|i| PathBuf::from(i)).collect())
                .unwrap(),
            schema_file: proxy.schema_file().map(|v| PathBuf::from(v)).unwrap(),
            explicit_watermarks: proxy
                .explicit_watermarks()
                .map(|v| v.iter().map(|i| odf::Watermark::deserialize(i)).collect())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryrequest-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::ExecuteQueryRequest {
    type OffsetT = WIPOffset<fb::ExecuteQueryRequest<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let dataset_id_offset = { fb.create_vector(&self.dataset_id.to_bytes()) };
        let dataset_name_offset = { fb.create_string(&self.dataset_name) };
        let vocab_offset = { self.vocab.serialize(fb) };
        let transform_offset = { self.transform.serialize(fb) };
        let inputs_offset = {
            let offsets: Vec<_> = self.inputs.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        };
        let prev_checkpoint_path_offset = self
            .prev_checkpoint_path
            .as_ref()
            .map(|v| fb.create_string(v.to_str().unwrap()));
        let new_checkpoint_path_offset =
            { fb.create_string(self.new_checkpoint_path.to_str().unwrap()) };
        let out_data_path_offset = { fb.create_string(self.out_data_path.to_str().unwrap()) };
        let mut builder = fb::ExecuteQueryRequestBuilder::new(fb);
        builder.add_dataset_id(dataset_id_offset);
        builder.add_dataset_name(dataset_name_offset);
        builder.add_system_time(&datetime_to_fb(&self.system_time));
        builder.add_offset(self.offset);
        builder.add_vocab(vocab_offset);
        builder.add_transform_type(transform_offset.0);
        builder.add_transform(transform_offset.1);
        builder.add_inputs(inputs_offset);
        prev_checkpoint_path_offset.map(|off| builder.add_prev_checkpoint_path(off));
        builder.add_new_checkpoint_path(new_checkpoint_path_offset);
        builder.add_out_data_path(out_data_path_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ExecuteQueryRequest<'fb>> for odf::ExecuteQueryRequest {
    fn deserialize(proxy: fb::ExecuteQueryRequest<'fb>) -> Self {
        odf::ExecuteQueryRequest {
            dataset_id: proxy
                .dataset_id()
                .map(|v| odf::DatasetID::from_bytes(v).unwrap())
                .unwrap(),
            dataset_name: proxy
                .dataset_name()
                .map(|v| odf::DatasetName::try_from(v).unwrap())
                .unwrap(),
            system_time: proxy.system_time().map(|v| fb_to_datetime(v)).unwrap(),
            offset: proxy.offset(),
            vocab: proxy
                .vocab()
                .map(|v| odf::DatasetVocabulary::deserialize(v))
                .unwrap(),
            transform: proxy
                .transform()
                .map(|v| odf::Transform::deserialize(v, proxy.transform_type()))
                .unwrap(),
            inputs: proxy
                .inputs()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::ExecuteQueryInput::deserialize(i))
                        .collect()
                })
                .unwrap(),
            prev_checkpoint_path: proxy.prev_checkpoint_path().map(|v| PathBuf::from(v)),
            new_checkpoint_path: proxy
                .new_checkpoint_path()
                .map(|v| PathBuf::from(v))
                .unwrap(),
            out_data_path: proxy.out_data_path().map(|v| PathBuf::from(v)).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryresponse-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::ExecuteQueryResponse> for odf::ExecuteQueryResponse {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::ExecuteQueryResponse, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::ExecuteQueryResponse::Progress => (
                fb::ExecuteQueryResponse::ExecuteQueryResponseProgress,
                empty_table(fb).as_union_value(),
            ),
            odf::ExecuteQueryResponse::Success(v) => (
                fb::ExecuteQueryResponse::ExecuteQueryResponseSuccess,
                v.serialize(fb).as_union_value(),
            ),
            odf::ExecuteQueryResponse::InvalidQuery(v) => (
                fb::ExecuteQueryResponse::ExecuteQueryResponseInvalidQuery,
                v.serialize(fb).as_union_value(),
            ),
            odf::ExecuteQueryResponse::InternalError(v) => (
                fb::ExecuteQueryResponse::ExecuteQueryResponseInternalError,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::ExecuteQueryResponse>
    for odf::ExecuteQueryResponse
{
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::ExecuteQueryResponse) -> Self {
        match t {
            fb::ExecuteQueryResponse::ExecuteQueryResponseProgress => {
                odf::ExecuteQueryResponse::Progress
            }
            fb::ExecuteQueryResponse::ExecuteQueryResponseSuccess => {
                odf::ExecuteQueryResponse::Success(odf::ExecuteQueryResponseSuccess::deserialize(
                    fb::ExecuteQueryResponseSuccess::init_from_table(table),
                ))
            }
            fb::ExecuteQueryResponse::ExecuteQueryResponseInvalidQuery => {
                odf::ExecuteQueryResponse::InvalidQuery(
                    odf::ExecuteQueryResponseInvalidQuery::deserialize(
                        fb::ExecuteQueryResponseInvalidQuery::init_from_table(table),
                    ),
                )
            }
            fb::ExecuteQueryResponse::ExecuteQueryResponseInternalError => {
                odf::ExecuteQueryResponse::InternalError(
                    odf::ExecuteQueryResponseInternalError::deserialize(
                        fb::ExecuteQueryResponseInternalError::init_from_table(table),
                    ),
                )
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::ExecuteQueryResponseSuccess {
    type OffsetT = WIPOffset<fb::ExecuteQueryResponseSuccess<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let data_interval_offset = self.data_interval.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::ExecuteQueryResponseSuccessBuilder::new(fb);
        data_interval_offset.map(|off| builder.add_data_interval(off));
        self.output_watermark
            .map(|v| builder.add_output_watermark(&datetime_to_fb(&v)));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ExecuteQueryResponseSuccess<'fb>>
    for odf::ExecuteQueryResponseSuccess
{
    fn deserialize(proxy: fb::ExecuteQueryResponseSuccess<'fb>) -> Self {
        odf::ExecuteQueryResponseSuccess {
            data_interval: proxy
                .data_interval()
                .map(|v| odf::OffsetInterval::deserialize(v)),
            output_watermark: proxy.output_watermark().map(|v| fb_to_datetime(v)),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::ExecuteQueryResponseInvalidQuery {
    type OffsetT = WIPOffset<fb::ExecuteQueryResponseInvalidQuery<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let message_offset = { fb.create_string(&self.message) };
        let mut builder = fb::ExecuteQueryResponseInvalidQueryBuilder::new(fb);
        builder.add_message(message_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ExecuteQueryResponseInvalidQuery<'fb>>
    for odf::ExecuteQueryResponseInvalidQuery
{
    fn deserialize(proxy: fb::ExecuteQueryResponseInvalidQuery<'fb>) -> Self {
        odf::ExecuteQueryResponseInvalidQuery {
            message: proxy.message().map(|v| v.to_owned()).unwrap(),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::ExecuteQueryResponseInternalError {
    type OffsetT = WIPOffset<fb::ExecuteQueryResponseInternalError<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let message_offset = { fb.create_string(&self.message) };
        let backtrace_offset = self.backtrace.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::ExecuteQueryResponseInternalErrorBuilder::new(fb);
        builder.add_message(message_offset);
        backtrace_offset.map(|off| builder.add_backtrace(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ExecuteQueryResponseInternalError<'fb>>
    for odf::ExecuteQueryResponseInternalError
{
    fn deserialize(proxy: fb::ExecuteQueryResponseInternalError<'fb>) -> Self {
        odf::ExecuteQueryResponseInternalError {
            message: proxy.message().map(|v| v.to_owned()).unwrap(),
            backtrace: proxy.backtrace().map(|v| v.to_owned()),
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
            odf::FetchStep::Container(v) => (
                fb::FetchStep::FetchStepContainer,
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
            fb::FetchStep::FetchStepContainer => {
                odf::FetchStep::Container(odf::FetchStepContainer::deserialize(
                    fb::FetchStepContainer::init_from_table(table),
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
        let headers_offset = self.headers.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        });
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
        headers_offset.map(|off| builder.add_headers(off));
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
            headers: proxy.headers().map(|v| {
                v.iter()
                    .map(|i| odf::RequestHeader::deserialize(i))
                    .collect()
            }),
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
        self.order.map(|v| builder.add_order(v.into()));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FetchStepFilesGlob<'fb>> for odf::FetchStepFilesGlob {
    fn deserialize(proxy: fb::FetchStepFilesGlob<'fb>) -> Self {
        odf::FetchStepFilesGlob {
            path: proxy.path().map(|v| v.to_owned()).unwrap(),
            event_time: proxy
                .event_time()
                .map(|v| odf::EventTimeSource::deserialize(v, proxy.event_time_type())),
            cache: proxy
                .cache()
                .map(|v| odf::SourceCaching::deserialize(v, proxy.cache_type())),
            order: proxy.order().map(|v| v.into()),
        }
    }
}

impl<'fb> FlatbuffersSerializable<'fb> for odf::FetchStepContainer {
    type OffsetT = WIPOffset<fb::FetchStepContainer<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let image_offset = { fb.create_string(&self.image) };
        let command_offset = self.command.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let args_offset = self.args.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let env_offset = self.env.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        });
        let mut builder = fb::FetchStepContainerBuilder::new(fb);
        builder.add_image(image_offset);
        command_offset.map(|off| builder.add_command(off));
        args_offset.map(|off| builder.add_args(off));
        env_offset.map(|off| builder.add_env(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FetchStepContainer<'fb>> for odf::FetchStepContainer {
    fn deserialize(proxy: fb::FetchStepContainer<'fb>) -> Self {
        odf::FetchStepContainer {
            image: proxy.image().map(|v| v.to_owned()).unwrap(),
            command: proxy
                .command()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            args: proxy
                .args()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            env: proxy
                .env()
                .map(|v| v.iter().map(|i| odf::EnvVar::deserialize(i)).collect()),
        }
    }
}

impl From<odf::SourceOrdering> for fb::SourceOrdering {
    fn from(v: odf::SourceOrdering) -> Self {
        match v {
            odf::SourceOrdering::ByEventTime => fb::SourceOrdering::ByEventTime,
            odf::SourceOrdering::ByName => fb::SourceOrdering::ByName,
        }
    }
}

impl Into<odf::SourceOrdering> for fb::SourceOrdering {
    fn into(self) -> odf::SourceOrdering {
        match self {
            fb::SourceOrdering::ByEventTime => odf::SourceOrdering::ByEventTime,
            fb::SourceOrdering::ByName => odf::SourceOrdering::ByName,
            _ => panic!("Invalid enum value: {}", self.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// InputSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#inputslice-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::InputSlice {
    type OffsetT = WIPOffset<fb::InputSlice<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let dataset_id_offset = { fb.create_vector(&self.dataset_id.to_bytes()) };
        let block_interval_offset = self.block_interval.as_ref().map(|v| v.serialize(fb));
        let data_interval_offset = self.data_interval.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::InputSliceBuilder::new(fb);
        builder.add_dataset_id(dataset_id_offset);
        block_interval_offset.map(|off| builder.add_block_interval(off));
        data_interval_offset.map(|off| builder.add_data_interval(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::InputSlice<'fb>> for odf::InputSlice {
    fn deserialize(proxy: fb::InputSlice<'fb>) -> Self {
        odf::InputSlice {
            dataset_id: proxy
                .dataset_id()
                .map(|v| odf::DatasetID::from_bytes(v).unwrap())
                .unwrap(),
            block_interval: proxy
                .block_interval()
                .map(|v| odf::BlockInterval::deserialize(v)),
            data_interval: proxy
                .data_interval()
                .map(|v| odf::OffsetInterval::deserialize(v)),
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
        let prev_block_hash_offset = self
            .prev_block_hash
            .as_ref()
            .map(|v| fb.create_vector(&v.to_bytes()));
        let event_offset = { self.event.serialize(fb) };
        let mut builder = fb::MetadataBlockBuilder::new(fb);
        builder.add_system_time(&datetime_to_fb(&self.system_time));
        prev_block_hash_offset.map(|off| builder.add_prev_block_hash(off));
        builder.add_sequence_number(self.sequence_number);
        builder.add_event_type(event_offset.0);
        builder.add_event(event_offset.1);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::MetadataBlock<'fb>> for odf::MetadataBlock {
    fn deserialize(proxy: fb::MetadataBlock<'fb>) -> Self {
        odf::MetadataBlock {
            system_time: proxy.system_time().map(|v| fb_to_datetime(v)).unwrap(),
            prev_block_hash: proxy
                .prev_block_hash()
                .map(|v| odf::Multihash::from_bytes(v).unwrap()),
            sequence_number: proxy.sequence_number(),
            event: proxy
                .event()
                .map(|v| odf::MetadataEvent::deserialize(v, proxy.event_type()))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::MetadataEvent> for odf::MetadataEvent {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::MetadataEvent, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::MetadataEvent::AddData(v) => {
                (fb::MetadataEvent::AddData, v.serialize(fb).as_union_value())
            }
            odf::MetadataEvent::ExecuteQuery(v) => (
                fb::MetadataEvent::ExecuteQuery,
                v.serialize(fb).as_union_value(),
            ),
            odf::MetadataEvent::Seed(v) => {
                (fb::MetadataEvent::Seed, v.serialize(fb).as_union_value())
            }
            odf::MetadataEvent::SetPollingSource(v) => (
                fb::MetadataEvent::SetPollingSource,
                v.serialize(fb).as_union_value(),
            ),
            odf::MetadataEvent::SetTransform(v) => (
                fb::MetadataEvent::SetTransform,
                v.serialize(fb).as_union_value(),
            ),
            odf::MetadataEvent::SetVocab(v) => (
                fb::MetadataEvent::SetVocab,
                v.serialize(fb).as_union_value(),
            ),
            odf::MetadataEvent::SetWatermark(v) => (
                fb::MetadataEvent::SetWatermark,
                v.serialize(fb).as_union_value(),
            ),
            odf::MetadataEvent::SetAttachments(v) => (
                fb::MetadataEvent::SetAttachments,
                v.serialize(fb).as_union_value(),
            ),
            odf::MetadataEvent::SetInfo(v) => {
                (fb::MetadataEvent::SetInfo, v.serialize(fb).as_union_value())
            }
            odf::MetadataEvent::SetLicense(v) => (
                fb::MetadataEvent::SetLicense,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::MetadataEvent> for odf::MetadataEvent {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::MetadataEvent) -> Self {
        match t {
            fb::MetadataEvent::AddData => odf::MetadataEvent::AddData(odf::AddData::deserialize(
                fb::AddData::init_from_table(table),
            )),
            fb::MetadataEvent::ExecuteQuery => odf::MetadataEvent::ExecuteQuery(
                odf::ExecuteQuery::deserialize(fb::ExecuteQuery::init_from_table(table)),
            ),
            fb::MetadataEvent::Seed => {
                odf::MetadataEvent::Seed(odf::Seed::deserialize(fb::Seed::init_from_table(table)))
            }
            fb::MetadataEvent::SetPollingSource => odf::MetadataEvent::SetPollingSource(
                odf::SetPollingSource::deserialize(fb::SetPollingSource::init_from_table(table)),
            ),
            fb::MetadataEvent::SetTransform => odf::MetadataEvent::SetTransform(
                odf::SetTransform::deserialize(fb::SetTransform::init_from_table(table)),
            ),
            fb::MetadataEvent::SetVocab => odf::MetadataEvent::SetVocab(
                odf::SetVocab::deserialize(fb::SetVocab::init_from_table(table)),
            ),
            fb::MetadataEvent::SetWatermark => odf::MetadataEvent::SetWatermark(
                odf::SetWatermark::deserialize(fb::SetWatermark::init_from_table(table)),
            ),
            fb::MetadataEvent::SetAttachments => odf::MetadataEvent::SetAttachments(
                odf::SetAttachments::deserialize(fb::SetAttachments::init_from_table(table)),
            ),
            fb::MetadataEvent::SetInfo => odf::MetadataEvent::SetInfo(odf::SetInfo::deserialize(
                fb::SetInfo::init_from_table(table),
            )),
            fb::MetadataEvent::SetLicense => odf::MetadataEvent::SetLicense(
                odf::SetLicense::deserialize(fb::SetLicense::init_from_table(table)),
            ),
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::OffsetInterval {
    type OffsetT = WIPOffset<fb::OffsetInterval<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::OffsetIntervalBuilder::new(fb);
        builder.add_start(self.start);
        builder.add_end(self.end);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::OffsetInterval<'fb>> for odf::OffsetInterval {
    fn deserialize(proxy: fb::OffsetInterval<'fb>) -> Self {
        odf::OffsetInterval {
            start: proxy.start(),
            end: proxy.end(),
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
        builder.add_format(self.format.into());
        sub_path_offset.map(|off| builder.add_sub_path(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::PrepStepDecompress<'fb>> for odf::PrepStepDecompress {
    fn deserialize(proxy: fb::PrepStepDecompress<'fb>) -> Self {
        odf::PrepStepDecompress {
            format: proxy.format().into(),
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

impl From<odf::CompressionFormat> for fb::CompressionFormat {
    fn from(v: odf::CompressionFormat) -> Self {
        match v {
            odf::CompressionFormat::Gzip => fb::CompressionFormat::Gzip,
            odf::CompressionFormat::Zip => fb::CompressionFormat::Zip,
        }
    }
}

impl Into<odf::CompressionFormat> for fb::CompressionFormat {
    fn into(self) -> odf::CompressionFormat {
        match self {
            fb::CompressionFormat::Gzip => odf::CompressionFormat::Gzip,
            fb::CompressionFormat::Zip => odf::CompressionFormat::Zip,
            _ => panic!("Invalid enum value: {}", self.0),
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
            odf::ReadStep::Parquet(v) => (
                fb::ReadStep::ReadStepParquet,
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
            fb::ReadStep::ReadStepParquet => odf::ReadStep::Parquet(
                odf::ReadStepParquet::deserialize(fb::ReadStepParquet::init_from_table(table)),
            ),
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

impl<'fb> FlatbuffersSerializable<'fb> for odf::ReadStepParquet {
    type OffsetT = WIPOffset<fb::ReadStepParquet<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let schema_offset = self.schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let mut builder = fb::ReadStepParquetBuilder::new(fb);
        schema_offset.map(|off| builder.add_schema(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepParquet<'fb>> for odf::ReadStepParquet {
    fn deserialize(proxy: fb::ReadStepParquet<'fb>) -> Self {
        odf::ReadStepParquet {
            schema: proxy
                .schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// RequestHeader
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#requestheader-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::RequestHeader {
    type OffsetT = WIPOffset<fb::RequestHeader<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let name_offset = { fb.create_string(&self.name) };
        let value_offset = { fb.create_string(&self.value) };
        let mut builder = fb::RequestHeaderBuilder::new(fb);
        builder.add_name(name_offset);
        builder.add_value(value_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::RequestHeader<'fb>> for odf::RequestHeader {
    fn deserialize(proxy: fb::RequestHeader<'fb>) -> Self {
        odf::RequestHeader {
            name: proxy.name().map(|v| v.to_owned()).unwrap(),
            value: proxy.value().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Seed
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::Seed {
    type OffsetT = WIPOffset<fb::Seed<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let dataset_id_offset = { fb.create_vector(&self.dataset_id.to_bytes()) };
        let mut builder = fb::SeedBuilder::new(fb);
        builder.add_dataset_id(dataset_id_offset);
        builder.add_dataset_kind(self.dataset_kind.into());
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Seed<'fb>> for odf::Seed {
    fn deserialize(proxy: fb::Seed<'fb>) -> Self {
        odf::Seed {
            dataset_id: proxy
                .dataset_id()
                .map(|v| odf::DatasetID::from_bytes(v).unwrap())
                .unwrap(),
            dataset_kind: proxy.dataset_kind().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetAttachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setattachments-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::SetAttachments {
    type OffsetT = WIPOffset<fb::SetAttachments<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let attachments_offset = { self.attachments.serialize(fb) };
        let mut builder = fb::SetAttachmentsBuilder::new(fb);
        builder.add_attachments_type(attachments_offset.0);
        builder.add_attachments(attachments_offset.1);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SetAttachments<'fb>> for odf::SetAttachments {
    fn deserialize(proxy: fb::SetAttachments<'fb>) -> Self {
        odf::SetAttachments {
            attachments: proxy
                .attachments()
                .map(|v| odf::Attachments::deserialize(v, proxy.attachments_type()))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetInfo
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setinfo-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::SetInfo {
    type OffsetT = WIPOffset<fb::SetInfo<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let description_offset = self.description.as_ref().map(|v| fb.create_string(&v));
        let keywords_offset = self.keywords.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let mut builder = fb::SetInfoBuilder::new(fb);
        description_offset.map(|off| builder.add_description(off));
        keywords_offset.map(|off| builder.add_keywords(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SetInfo<'fb>> for odf::SetInfo {
    fn deserialize(proxy: fb::SetInfo<'fb>) -> Self {
        odf::SetInfo {
            description: proxy.description().map(|v| v.to_owned()),
            keywords: proxy
                .keywords()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetLicense
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setlicense-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::SetLicense {
    type OffsetT = WIPOffset<fb::SetLicense<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let short_name_offset = { fb.create_string(&self.short_name) };
        let name_offset = { fb.create_string(&self.name) };
        let spdx_id_offset = self.spdx_id.as_ref().map(|v| fb.create_string(&v));
        let website_url_offset = { fb.create_string(&self.website_url) };
        let mut builder = fb::SetLicenseBuilder::new(fb);
        builder.add_short_name(short_name_offset);
        builder.add_name(name_offset);
        spdx_id_offset.map(|off| builder.add_spdx_id(off));
        builder.add_website_url(website_url_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SetLicense<'fb>> for odf::SetLicense {
    fn deserialize(proxy: fb::SetLicense<'fb>) -> Self {
        odf::SetLicense {
            short_name: proxy.short_name().map(|v| v.to_owned()).unwrap(),
            name: proxy.name().map(|v| v.to_owned()).unwrap(),
            spdx_id: proxy.spdx_id().map(|v| v.to_owned()),
            website_url: proxy.website_url().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetPollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::SetPollingSource {
    type OffsetT = WIPOffset<fb::SetPollingSource<'fb>>;

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
        let mut builder = fb::SetPollingSourceBuilder::new(fb);
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

impl<'fb> FlatbuffersDeserializable<fb::SetPollingSource<'fb>> for odf::SetPollingSource {
    fn deserialize(proxy: fb::SetPollingSource<'fb>) -> Self {
        odf::SetPollingSource {
            fetch: proxy
                .fetch()
                .map(|v| odf::FetchStep::deserialize(v, proxy.fetch_type()))
                .unwrap(),
            prepare: proxy.prepare().map(|v| {
                v.iter()
                    .map(|i| odf::PrepStep::deserialize(i.value().unwrap(), i.value_type()))
                    .collect()
            }),
            read: proxy
                .read()
                .map(|v| odf::ReadStep::deserialize(v, proxy.read_type()))
                .unwrap(),
            preprocess: proxy
                .preprocess()
                .map(|v| odf::Transform::deserialize(v, proxy.preprocess_type())),
            merge: proxy
                .merge()
                .map(|v| odf::MergeStrategy::deserialize(v, proxy.merge_type()))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::SetTransform {
    type OffsetT = WIPOffset<fb::SetTransform<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let inputs_offset = {
            let offsets: Vec<_> = self.inputs.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        };
        let transform_offset = { self.transform.serialize(fb) };
        let mut builder = fb::SetTransformBuilder::new(fb);
        builder.add_inputs(inputs_offset);
        builder.add_transform_type(transform_offset.0);
        builder.add_transform(transform_offset.1);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SetTransform<'fb>> for odf::SetTransform {
    fn deserialize(proxy: fb::SetTransform<'fb>) -> Self {
        odf::SetTransform {
            inputs: proxy
                .inputs()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::TransformInput::deserialize(i))
                        .collect()
                })
                .unwrap(),
            transform: proxy
                .transform()
                .map(|v| odf::Transform::deserialize(v, proxy.transform_type()))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetVocab
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::SetVocab {
    type OffsetT = WIPOffset<fb::SetVocab<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let system_time_column_offset = self
            .system_time_column
            .as_ref()
            .map(|v| fb.create_string(&v));
        let event_time_column_offset = self
            .event_time_column
            .as_ref()
            .map(|v| fb.create_string(&v));
        let offset_column_offset = self.offset_column.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::SetVocabBuilder::new(fb);
        system_time_column_offset.map(|off| builder.add_system_time_column(off));
        event_time_column_offset.map(|off| builder.add_event_time_column(off));
        offset_column_offset.map(|off| builder.add_offset_column(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SetVocab<'fb>> for odf::SetVocab {
    fn deserialize(proxy: fb::SetVocab<'fb>) -> Self {
        odf::SetVocab {
            system_time_column: proxy.system_time_column().map(|v| v.to_owned()),
            event_time_column: proxy.event_time_column().map(|v| v.to_owned()),
            offset_column: proxy.offset_column().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetWatermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setwatermark-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::SetWatermark {
    type OffsetT = WIPOffset<fb::SetWatermark<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::SetWatermarkBuilder::new(fb);
        builder.add_output_watermark(&datetime_to_fb(&self.output_watermark));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SetWatermark<'fb>> for odf::SetWatermark {
    fn deserialize(proxy: fb::SetWatermark<'fb>) -> Self {
        odf::SetWatermark {
            output_watermark: proxy.output_watermark().map(|v| fb_to_datetime(v)).unwrap(),
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
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::SourceCaching) -> Self {
        match t {
            fb::SourceCaching::SourceCachingForever => odf::SourceCaching::Forever,
            _ => panic!("Invalid enum value: {}", t.0),
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
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::TemporalTable {
    type OffsetT = WIPOffset<fb::TemporalTable<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let name_offset = { fb.create_string(&self.name) };
        let primary_key_offset = {
            let offsets: Vec<_> = self
                .primary_key
                .iter()
                .map(|i| fb.create_string(&i))
                .collect();
            fb.create_vector(&offsets)
        };
        let mut builder = fb::TemporalTableBuilder::new(fb);
        builder.add_name(name_offset);
        builder.add_primary_key(primary_key_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TemporalTable<'fb>> for odf::TemporalTable {
    fn deserialize(proxy: fb::TemporalTable<'fb>) -> Self {
        odf::TemporalTable {
            name: proxy.name().map(|v| v.to_owned()).unwrap(),
            primary_key: proxy
                .primary_key()
                .map(|v| v.iter().map(|i| i.to_owned()).collect())
                .unwrap(),
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
// TransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::TransformInput {
    type OffsetT = WIPOffset<fb::TransformInput<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let id_offset = self.id.as_ref().map(|v| fb.create_vector(&v.to_bytes()));
        let name_offset = { fb.create_string(&self.name) };
        let mut builder = fb::TransformInputBuilder::new(fb);
        id_offset.map(|off| builder.add_id(off));
        builder.add_name(name_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TransformInput<'fb>> for odf::TransformInput {
    fn deserialize(proxy: fb::TransformInput<'fb>) -> Self {
        odf::TransformInput {
            id: proxy.id().map(|v| odf::DatasetID::from_bytes(v).unwrap()),
            name: proxy
                .name()
                .map(|v| odf::DatasetName::try_from(v).unwrap())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Watermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::Watermark {
    type OffsetT = WIPOffset<fb::Watermark<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::WatermarkBuilder::new(fb);
        builder.add_system_time(&datetime_to_fb(&self.system_time));
        builder.add_event_time(&datetime_to_fb(&self.event_time));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Watermark<'fb>> for odf::Watermark {
    fn deserialize(proxy: fb::Watermark<'fb>) -> Self {
        odf::Watermark {
            system_time: proxy.system_time().map(|v| fb_to_datetime(v)).unwrap(),
            event_time: proxy.event_time().map(|v| fb_to_datetime(v)).unwrap(),
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

fn empty_table<'fb>(
    fb: &mut FlatBufferBuilder<'fb>,
) -> WIPOffset<flatbuffers::TableFinishedWIPOffset> {
    let wip = fb.start_table();
    fb.end_table(wip)
}
