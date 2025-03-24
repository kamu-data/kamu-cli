// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(clippy::all)]
#![allow(clippy::pedantic)]

use super::proxies_generated as fb;
mod odf {
    pub use crate::dtos::*;
    pub use crate::formats::*;
    pub use crate::identity::*;
}
use std::convert::TryFrom;
use std::path::PathBuf;

use ::flatbuffers::{FlatBufferBuilder, Table, UnionWIPOffset, WIPOffset};
use chrono::prelude::*;

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddData
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::AddData {
    type OffsetT = WIPOffset<fb::AddData<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let prev_checkpoint_offset = self
            .prev_checkpoint
            .as_ref()
            .map(|v| fb.create_vector(&v.as_bytes().as_slice()));
        let new_data_offset = self.new_data.as_ref().map(|v| v.serialize(fb));
        let new_checkpoint_offset = self.new_checkpoint.as_ref().map(|v| v.serialize(fb));
        let new_source_state_offset = self.new_source_state.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::AddDataBuilder::new(fb);
        prev_checkpoint_offset.map(|off| builder.add_prev_checkpoint(off));
        self.prev_offset.map(|v| builder.add_prev_offset(v));
        new_data_offset.map(|off| builder.add_new_data(off));
        new_checkpoint_offset.map(|off| builder.add_new_checkpoint(off));
        self.new_watermark
            .map(|v| builder.add_new_watermark(&datetime_to_fb(&v)));
        new_source_state_offset.map(|off| builder.add_new_source_state(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::AddData<'fb>> for odf::AddData {
    fn deserialize(proxy: fb::AddData<'fb>) -> Self {
        odf::AddData {
            prev_checkpoint: proxy
                .prev_checkpoint()
                .map(|v| odf::Multihash::from_bytes(v.bytes()).unwrap()),
            prev_offset: proxy.prev_offset().map(|v| v),
            new_data: proxy.new_data().map(|v| odf::DataSlice::deserialize(v)),
            new_checkpoint: proxy
                .new_checkpoint()
                .map(|v| odf::Checkpoint::deserialize(v)),
            new_watermark: proxy.new_watermark().map(|v| fb_to_datetime(v)),
            new_source_state: proxy
                .new_source_state()
                .map(|v| odf::SourceState::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddPushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#addpushsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::AddPushSource {
    type OffsetT = WIPOffset<fb::AddPushSource<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let source_name_offset = { fb.create_string(&self.source_name) };
        let read_offset = { self.read.serialize(fb) };
        let preprocess_offset = self.preprocess.as_ref().map(|v| v.serialize(fb));
        let merge_offset = { self.merge.serialize(fb) };
        let mut builder = fb::AddPushSourceBuilder::new(fb);
        builder.add_source_name(source_name_offset);
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

impl<'fb> FlatbuffersDeserializable<fb::AddPushSource<'fb>> for odf::AddPushSource {
    fn deserialize(proxy: fb::AddPushSource<'fb>) -> Self {
        odf::AddPushSource {
            source_name: proxy.source_name().map(|v| v.to_owned()).unwrap(),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AttachmentEmbedded
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentembedded-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Attachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachments-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
                odf::Attachments::Embedded(odf::AttachmentsEmbedded::deserialize(unsafe {
                    fb::AttachmentsEmbedded::init_from_table(table)
                }))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AttachmentsEmbedded
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentsembedded-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Checkpoint
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::Checkpoint {
    type OffsetT = WIPOffset<fb::Checkpoint<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let physical_hash_offset = { fb.create_vector(&self.physical_hash.as_bytes().as_slice()) };
        let mut builder = fb::CheckpointBuilder::new(fb);
        builder.add_physical_hash(physical_hash_offset);
        builder.add_size(self.size);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Checkpoint<'fb>> for odf::Checkpoint {
    fn deserialize(proxy: fb::Checkpoint<'fb>) -> Self {
        odf::Checkpoint {
            physical_hash: proxy
                .physical_hash()
                .map(|v| odf::Multihash::from_bytes(v.bytes()).unwrap())
                .unwrap(),
            size: proxy.size(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CompressionFormat
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#compressionformat-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::DataSlice {
    type OffsetT = WIPOffset<fb::DataSlice<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let logical_hash_offset = { fb.create_vector(&self.logical_hash.as_bytes().as_slice()) };
        let physical_hash_offset = { fb.create_vector(&self.physical_hash.as_bytes().as_slice()) };
        let offset_interval_offset = { self.offset_interval.serialize(fb) };
        let mut builder = fb::DataSliceBuilder::new(fb);
        builder.add_logical_hash(logical_hash_offset);
        builder.add_physical_hash(physical_hash_offset);
        builder.add_offset_interval(offset_interval_offset);
        builder.add_size(self.size);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataSlice<'fb>> for odf::DataSlice {
    fn deserialize(proxy: fb::DataSlice<'fb>) -> Self {
        odf::DataSlice {
            logical_hash: proxy
                .logical_hash()
                .map(|v| odf::Multihash::from_bytes(v.bytes()).unwrap())
                .unwrap(),
            physical_hash: proxy
                .physical_hash()
                .map(|v| odf::Multihash::from_bytes(v.bytes()).unwrap())
                .unwrap(),
            offset_interval: proxy
                .offset_interval()
                .map(|v| odf::OffsetInterval::deserialize(v))
                .unwrap(),
            size: proxy.size(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetKind
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetkind-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::DatasetVocabulary {
    type OffsetT = WIPOffset<fb::DatasetVocabulary<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let offset_column_offset = { fb.create_string(&self.offset_column) };
        let operation_type_column_offset = { fb.create_string(&self.operation_type_column) };
        let system_time_column_offset = { fb.create_string(&self.system_time_column) };
        let event_time_column_offset = { fb.create_string(&self.event_time_column) };
        let mut builder = fb::DatasetVocabularyBuilder::new(fb);
        builder.add_offset_column(offset_column_offset);
        builder.add_operation_type_column(operation_type_column_offset);
        builder.add_system_time_column(system_time_column_offset);
        builder.add_event_time_column(event_time_column_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DatasetVocabulary<'fb>> for odf::DatasetVocabulary {
    fn deserialize(proxy: fb::DatasetVocabulary<'fb>) -> Self {
        odf::DatasetVocabulary {
            offset_column: proxy.offset_column().map(|v| v.to_owned()).unwrap(),
            operation_type_column: proxy.operation_type_column().map(|v| v.to_owned()).unwrap(),
            system_time_column: proxy.system_time_column().map(|v| v.to_owned()).unwrap(),
            event_time_column: proxy.event_time_column().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DisablePollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepollingsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::DisablePollingSource {
    type OffsetT = WIPOffset<fb::DisablePollingSource<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DisablePollingSourceBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DisablePollingSource<'fb>> for odf::DisablePollingSource {
    fn deserialize(proxy: fb::DisablePollingSource<'fb>) -> Self {
        odf::DisablePollingSource {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DisablePushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepushsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::DisablePushSource {
    type OffsetT = WIPOffset<fb::DisablePushSource<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let source_name_offset = { fb.create_string(&self.source_name) };
        let mut builder = fb::DisablePushSourceBuilder::new(fb);
        builder.add_source_name(source_name_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DisablePushSource<'fb>> for odf::DisablePushSource {
    fn deserialize(proxy: fb::DisablePushSource<'fb>) -> Self {
        odf::DisablePushSource {
            source_name: proxy.source_name().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EnvVar
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#envvar-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::EventTimeSource> for odf::EventTimeSource {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::EventTimeSource, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::EventTimeSource::FromMetadata(v) => (
                fb::EventTimeSource::EventTimeSourceFromMetadata,
                v.serialize(fb).as_union_value(),
            ),
            odf::EventTimeSource::FromPath(v) => (
                fb::EventTimeSource::EventTimeSourceFromPath,
                v.serialize(fb).as_union_value(),
            ),
            odf::EventTimeSource::FromSystemTime(v) => (
                fb::EventTimeSource::EventTimeSourceFromSystemTime,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::EventTimeSource> for odf::EventTimeSource {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::EventTimeSource) -> Self {
        match t {
            fb::EventTimeSource::EventTimeSourceFromMetadata => {
                odf::EventTimeSource::FromMetadata(odf::EventTimeSourceFromMetadata::deserialize(
                    unsafe { fb::EventTimeSourceFromMetadata::init_from_table(table) },
                ))
            }
            fb::EventTimeSource::EventTimeSourceFromPath => {
                odf::EventTimeSource::FromPath(odf::EventTimeSourceFromPath::deserialize(unsafe {
                    fb::EventTimeSourceFromPath::init_from_table(table)
                }))
            }
            fb::EventTimeSource::EventTimeSourceFromSystemTime => {
                odf::EventTimeSource::FromSystemTime(
                    odf::EventTimeSourceFromSystemTime::deserialize(unsafe {
                        fb::EventTimeSourceFromSystemTime::init_from_table(table)
                    }),
                )
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSourceFromMetadata
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesourcefrommetadata-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::EventTimeSourceFromMetadata {
    type OffsetT = WIPOffset<fb::EventTimeSourceFromMetadata<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::EventTimeSourceFromMetadataBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::EventTimeSourceFromMetadata<'fb>>
    for odf::EventTimeSourceFromMetadata
{
    fn deserialize(proxy: fb::EventTimeSourceFromMetadata<'fb>) -> Self {
        odf::EventTimeSourceFromMetadata {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSourceFromPath
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesourcefrompath-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSourceFromSystemTime
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesourcefromsystemtime-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::EventTimeSourceFromSystemTime {
    type OffsetT = WIPOffset<fb::EventTimeSourceFromSystemTime<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::EventTimeSourceFromSystemTimeBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::EventTimeSourceFromSystemTime<'fb>>
    for odf::EventTimeSourceFromSystemTime
{
    fn deserialize(proxy: fb::EventTimeSourceFromSystemTime<'fb>) -> Self {
        odf::EventTimeSourceFromSystemTime {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::ExecuteTransform {
    type OffsetT = WIPOffset<fb::ExecuteTransform<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let query_inputs_offset = {
            let offsets: Vec<_> = self.query_inputs.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        };
        let prev_checkpoint_offset = self
            .prev_checkpoint
            .as_ref()
            .map(|v| fb.create_vector(&v.as_bytes().as_slice()));
        let new_data_offset = self.new_data.as_ref().map(|v| v.serialize(fb));
        let new_checkpoint_offset = self.new_checkpoint.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::ExecuteTransformBuilder::new(fb);
        builder.add_query_inputs(query_inputs_offset);
        prev_checkpoint_offset.map(|off| builder.add_prev_checkpoint(off));
        self.prev_offset.map(|v| builder.add_prev_offset(v));
        new_data_offset.map(|off| builder.add_new_data(off));
        new_checkpoint_offset.map(|off| builder.add_new_checkpoint(off));
        self.new_watermark
            .map(|v| builder.add_new_watermark(&datetime_to_fb(&v)));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ExecuteTransform<'fb>> for odf::ExecuteTransform {
    fn deserialize(proxy: fb::ExecuteTransform<'fb>) -> Self {
        odf::ExecuteTransform {
            query_inputs: proxy
                .query_inputs()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::ExecuteTransformInput::deserialize(i))
                        .collect()
                })
                .unwrap(),
            prev_checkpoint: proxy
                .prev_checkpoint()
                .map(|v| odf::Multihash::from_bytes(v.bytes()).unwrap()),
            prev_offset: proxy.prev_offset().map(|v| v),
            new_data: proxy.new_data().map(|v| odf::DataSlice::deserialize(v)),
            new_checkpoint: proxy
                .new_checkpoint()
                .map(|v| odf::Checkpoint::deserialize(v)),
            new_watermark: proxy.new_watermark().map(|v| fb_to_datetime(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransforminput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::ExecuteTransformInput {
    type OffsetT = WIPOffset<fb::ExecuteTransformInput<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let dataset_id_offset = { fb.create_vector(&self.dataset_id.as_bytes().as_slice()) };
        let prev_block_hash_offset = self
            .prev_block_hash
            .as_ref()
            .map(|v| fb.create_vector(&v.as_bytes().as_slice()));
        let new_block_hash_offset = self
            .new_block_hash
            .as_ref()
            .map(|v| fb.create_vector(&v.as_bytes().as_slice()));
        let mut builder = fb::ExecuteTransformInputBuilder::new(fb);
        builder.add_dataset_id(dataset_id_offset);
        prev_block_hash_offset.map(|off| builder.add_prev_block_hash(off));
        new_block_hash_offset.map(|off| builder.add_new_block_hash(off));
        self.prev_offset.map(|v| builder.add_prev_offset(v));
        self.new_offset.map(|v| builder.add_new_offset(v));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ExecuteTransformInput<'fb>> for odf::ExecuteTransformInput {
    fn deserialize(proxy: fb::ExecuteTransformInput<'fb>) -> Self {
        odf::ExecuteTransformInput {
            dataset_id: proxy
                .dataset_id()
                .map(|v| odf::DatasetID::from_bytes(v.bytes()).unwrap())
                .unwrap(),
            prev_block_hash: proxy
                .prev_block_hash()
                .map(|v| odf::Multihash::from_bytes(v.bytes()).unwrap()),
            new_block_hash: proxy
                .new_block_hash()
                .map(|v| odf::Multihash::from_bytes(v.bytes()).unwrap()),
            prev_offset: proxy.prev_offset().map(|v| v),
            new_offset: proxy.new_offset().map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
            odf::FetchStep::Mqtt(v) => (
                fb::FetchStep::FetchStepMqtt,
                v.serialize(fb).as_union_value(),
            ),
            odf::FetchStep::EthereumLogs(v) => (
                fb::FetchStep::FetchStepEthereumLogs,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::FetchStep> for odf::FetchStep {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::FetchStep) -> Self {
        match t {
            fb::FetchStep::FetchStepUrl => {
                odf::FetchStep::Url(odf::FetchStepUrl::deserialize(unsafe {
                    fb::FetchStepUrl::init_from_table(table)
                }))
            }
            fb::FetchStep::FetchStepFilesGlob => {
                odf::FetchStep::FilesGlob(odf::FetchStepFilesGlob::deserialize(unsafe {
                    fb::FetchStepFilesGlob::init_from_table(table)
                }))
            }
            fb::FetchStep::FetchStepContainer => {
                odf::FetchStep::Container(odf::FetchStepContainer::deserialize(unsafe {
                    fb::FetchStepContainer::init_from_table(table)
                }))
            }
            fb::FetchStep::FetchStepMqtt => {
                odf::FetchStep::Mqtt(odf::FetchStepMqtt::deserialize(unsafe {
                    fb::FetchStepMqtt::init_from_table(table)
                }))
            }
            fb::FetchStep::FetchStepEthereumLogs => {
                odf::FetchStep::EthereumLogs(odf::FetchStepEthereumLogs::deserialize(unsafe {
                    fb::FetchStepEthereumLogs::init_from_table(table)
                }))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepContainer
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepcontainer-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepEthereumLogs
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepethereumlogs-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::FetchStepEthereumLogs {
    type OffsetT = WIPOffset<fb::FetchStepEthereumLogs<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let node_url_offset = self.node_url.as_ref().map(|v| fb.create_string(&v));
        let filter_offset = self.filter.as_ref().map(|v| fb.create_string(&v));
        let signature_offset = self.signature.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::FetchStepEthereumLogsBuilder::new(fb);
        self.chain_id.map(|v| builder.add_chain_id(v));
        node_url_offset.map(|off| builder.add_node_url(off));
        filter_offset.map(|off| builder.add_filter(off));
        signature_offset.map(|off| builder.add_signature(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FetchStepEthereumLogs<'fb>> for odf::FetchStepEthereumLogs {
    fn deserialize(proxy: fb::FetchStepEthereumLogs<'fb>) -> Self {
        odf::FetchStepEthereumLogs {
            chain_id: proxy.chain_id().map(|v| v),
            node_url: proxy.node_url().map(|v| v.to_owned()),
            filter: proxy.filter().map(|v| v.to_owned()),
            signature: proxy.signature().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepFilesGlob
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepfilesglob-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepMqtt
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepmqtt-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::FetchStepMqtt {
    type OffsetT = WIPOffset<fb::FetchStepMqtt<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let host_offset = { fb.create_string(&self.host) };
        let username_offset = self.username.as_ref().map(|v| fb.create_string(&v));
        let password_offset = self.password.as_ref().map(|v| fb.create_string(&v));
        let topics_offset = {
            let offsets: Vec<_> = self.topics.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        };
        let mut builder = fb::FetchStepMqttBuilder::new(fb);
        builder.add_host(host_offset);
        builder.add_port(self.port);
        username_offset.map(|off| builder.add_username(off));
        password_offset.map(|off| builder.add_password(off));
        builder.add_topics(topics_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FetchStepMqtt<'fb>> for odf::FetchStepMqtt {
    fn deserialize(proxy: fb::FetchStepMqtt<'fb>) -> Self {
        odf::FetchStepMqtt {
            host: proxy.host().map(|v| v.to_owned()).unwrap(),
            port: proxy.port(),
            username: proxy.username().map(|v| v.to_owned()),
            password: proxy.password().map(|v| v.to_owned()),
            topics: proxy
                .topics()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::MqttTopicSubscription::deserialize(i))
                        .collect()
                })
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepUrl
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepurl-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::MergeStrategy> for odf::MergeStrategy {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::MergeStrategy, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::MergeStrategy::Append(v) => (
                fb::MergeStrategy::MergeStrategyAppend,
                v.serialize(fb).as_union_value(),
            ),
            odf::MergeStrategy::Ledger(v) => (
                fb::MergeStrategy::MergeStrategyLedger,
                v.serialize(fb).as_union_value(),
            ),
            odf::MergeStrategy::Snapshot(v) => (
                fb::MergeStrategy::MergeStrategySnapshot,
                v.serialize(fb).as_union_value(),
            ),
            odf::MergeStrategy::ChangelogStream(v) => (
                fb::MergeStrategy::MergeStrategyChangelogStream,
                v.serialize(fb).as_union_value(),
            ),
            odf::MergeStrategy::UpsertStream(v) => (
                fb::MergeStrategy::MergeStrategyUpsertStream,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::MergeStrategy> for odf::MergeStrategy {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::MergeStrategy) -> Self {
        match t {
            fb::MergeStrategy::MergeStrategyAppend => {
                odf::MergeStrategy::Append(odf::MergeStrategyAppend::deserialize(unsafe {
                    fb::MergeStrategyAppend::init_from_table(table)
                }))
            }
            fb::MergeStrategy::MergeStrategyLedger => {
                odf::MergeStrategy::Ledger(odf::MergeStrategyLedger::deserialize(unsafe {
                    fb::MergeStrategyLedger::init_from_table(table)
                }))
            }
            fb::MergeStrategy::MergeStrategySnapshot => {
                odf::MergeStrategy::Snapshot(odf::MergeStrategySnapshot::deserialize(unsafe {
                    fb::MergeStrategySnapshot::init_from_table(table)
                }))
            }
            fb::MergeStrategy::MergeStrategyChangelogStream => {
                odf::MergeStrategy::ChangelogStream(odf::MergeStrategyChangelogStream::deserialize(
                    unsafe { fb::MergeStrategyChangelogStream::init_from_table(table) },
                ))
            }
            fb::MergeStrategy::MergeStrategyUpsertStream => {
                odf::MergeStrategy::UpsertStream(odf::MergeStrategyUpsertStream::deserialize(
                    unsafe { fb::MergeStrategyUpsertStream::init_from_table(table) },
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategyAppend
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategyappend-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::MergeStrategyAppend {
    type OffsetT = WIPOffset<fb::MergeStrategyAppend<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::MergeStrategyAppendBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::MergeStrategyAppend<'fb>> for odf::MergeStrategyAppend {
    fn deserialize(proxy: fb::MergeStrategyAppend<'fb>) -> Self {
        odf::MergeStrategyAppend {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategyChangelogStream
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategychangelogstream-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::MergeStrategyChangelogStream {
    type OffsetT = WIPOffset<fb::MergeStrategyChangelogStream<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let primary_key_offset = {
            let offsets: Vec<_> = self
                .primary_key
                .iter()
                .map(|i| fb.create_string(&i))
                .collect();
            fb.create_vector(&offsets)
        };
        let mut builder = fb::MergeStrategyChangelogStreamBuilder::new(fb);
        builder.add_primary_key(primary_key_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::MergeStrategyChangelogStream<'fb>>
    for odf::MergeStrategyChangelogStream
{
    fn deserialize(proxy: fb::MergeStrategyChangelogStream<'fb>) -> Self {
        odf::MergeStrategyChangelogStream {
            primary_key: proxy
                .primary_key()
                .map(|v| v.iter().map(|i| i.to_owned()).collect())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategyLedger
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategyledger-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategySnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategysnapshot-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        let mut builder = fb::MergeStrategySnapshotBuilder::new(fb);
        builder.add_primary_key(primary_key_offset);
        compare_columns_offset.map(|off| builder.add_compare_columns(off));
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
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategyUpsertStream
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategyupsertstream-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::MergeStrategyUpsertStream {
    type OffsetT = WIPOffset<fb::MergeStrategyUpsertStream<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let primary_key_offset = {
            let offsets: Vec<_> = self
                .primary_key
                .iter()
                .map(|i| fb.create_string(&i))
                .collect();
            fb.create_vector(&offsets)
        };
        let mut builder = fb::MergeStrategyUpsertStreamBuilder::new(fb);
        builder.add_primary_key(primary_key_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::MergeStrategyUpsertStream<'fb>>
    for odf::MergeStrategyUpsertStream
{
    fn deserialize(proxy: fb::MergeStrategyUpsertStream<'fb>) -> Self {
        odf::MergeStrategyUpsertStream {
            primary_key: proxy
                .primary_key()
                .map(|v| v.iter().map(|i| i.to_owned()).collect())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::MetadataBlock {
    type OffsetT = WIPOffset<fb::MetadataBlock<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let prev_block_hash_offset = self
            .prev_block_hash
            .as_ref()
            .map(|v| fb.create_vector(&v.as_bytes().as_slice()));
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
                .map(|v| odf::Multihash::from_bytes(v.bytes()).unwrap()),
            sequence_number: proxy.sequence_number(),
            event: proxy
                .event()
                .map(|v| odf::MetadataEvent::deserialize(v, proxy.event_type()))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::MetadataEvent> for odf::MetadataEvent {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::MetadataEvent, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::MetadataEvent::AddData(v) => {
                (fb::MetadataEvent::AddData, v.serialize(fb).as_union_value())
            }
            odf::MetadataEvent::ExecuteTransform(v) => (
                fb::MetadataEvent::ExecuteTransform,
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
            odf::MetadataEvent::SetDataSchema(v) => (
                fb::MetadataEvent::SetDataSchema,
                v.serialize(fb).as_union_value(),
            ),
            odf::MetadataEvent::AddPushSource(v) => (
                fb::MetadataEvent::AddPushSource,
                v.serialize(fb).as_union_value(),
            ),
            odf::MetadataEvent::DisablePushSource(v) => (
                fb::MetadataEvent::DisablePushSource,
                v.serialize(fb).as_union_value(),
            ),
            odf::MetadataEvent::DisablePollingSource(v) => (
                fb::MetadataEvent::DisablePollingSource,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::MetadataEvent> for odf::MetadataEvent {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::MetadataEvent) -> Self {
        match t {
            fb::MetadataEvent::AddData => {
                odf::MetadataEvent::AddData(odf::AddData::deserialize(unsafe {
                    fb::AddData::init_from_table(table)
                }))
            }
            fb::MetadataEvent::ExecuteTransform => {
                odf::MetadataEvent::ExecuteTransform(odf::ExecuteTransform::deserialize(unsafe {
                    fb::ExecuteTransform::init_from_table(table)
                }))
            }
            fb::MetadataEvent::Seed => odf::MetadataEvent::Seed(odf::Seed::deserialize(unsafe {
                fb::Seed::init_from_table(table)
            })),
            fb::MetadataEvent::SetPollingSource => {
                odf::MetadataEvent::SetPollingSource(odf::SetPollingSource::deserialize(unsafe {
                    fb::SetPollingSource::init_from_table(table)
                }))
            }
            fb::MetadataEvent::SetTransform => {
                odf::MetadataEvent::SetTransform(odf::SetTransform::deserialize(unsafe {
                    fb::SetTransform::init_from_table(table)
                }))
            }
            fb::MetadataEvent::SetVocab => {
                odf::MetadataEvent::SetVocab(odf::SetVocab::deserialize(unsafe {
                    fb::SetVocab::init_from_table(table)
                }))
            }
            fb::MetadataEvent::SetAttachments => {
                odf::MetadataEvent::SetAttachments(odf::SetAttachments::deserialize(unsafe {
                    fb::SetAttachments::init_from_table(table)
                }))
            }
            fb::MetadataEvent::SetInfo => {
                odf::MetadataEvent::SetInfo(odf::SetInfo::deserialize(unsafe {
                    fb::SetInfo::init_from_table(table)
                }))
            }
            fb::MetadataEvent::SetLicense => {
                odf::MetadataEvent::SetLicense(odf::SetLicense::deserialize(unsafe {
                    fb::SetLicense::init_from_table(table)
                }))
            }
            fb::MetadataEvent::SetDataSchema => {
                odf::MetadataEvent::SetDataSchema(odf::SetDataSchema::deserialize(unsafe {
                    fb::SetDataSchema::init_from_table(table)
                }))
            }
            fb::MetadataEvent::AddPushSource => {
                odf::MetadataEvent::AddPushSource(odf::AddPushSource::deserialize(unsafe {
                    fb::AddPushSource::init_from_table(table)
                }))
            }
            fb::MetadataEvent::DisablePushSource => {
                odf::MetadataEvent::DisablePushSource(odf::DisablePushSource::deserialize(unsafe {
                    fb::DisablePushSource::init_from_table(table)
                }))
            }
            fb::MetadataEvent::DisablePollingSource => {
                odf::MetadataEvent::DisablePollingSource(odf::DisablePollingSource::deserialize(
                    unsafe { fb::DisablePollingSource::init_from_table(table) },
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MqttQos
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqttqos-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::MqttQos> for fb::MqttQos {
    fn from(v: odf::MqttQos) -> Self {
        match v {
            odf::MqttQos::AtMostOnce => fb::MqttQos::AtMostOnce,
            odf::MqttQos::AtLeastOnce => fb::MqttQos::AtLeastOnce,
            odf::MqttQos::ExactlyOnce => fb::MqttQos::ExactlyOnce,
        }
    }
}

impl Into<odf::MqttQos> for fb::MqttQos {
    fn into(self) -> odf::MqttQos {
        match self {
            fb::MqttQos::AtMostOnce => odf::MqttQos::AtMostOnce,
            fb::MqttQos::AtLeastOnce => odf::MqttQos::AtLeastOnce,
            fb::MqttQos::ExactlyOnce => odf::MqttQos::ExactlyOnce,
            _ => panic!("Invalid enum value: {}", self.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MqttTopicSubscription
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqtttopicsubscription-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::MqttTopicSubscription {
    type OffsetT = WIPOffset<fb::MqttTopicSubscription<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let path_offset = { fb.create_string(&self.path) };
        let mut builder = fb::MqttTopicSubscriptionBuilder::new(fb);
        builder.add_path(path_offset);
        self.qos.map(|v| builder.add_qos(v.into()));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::MqttTopicSubscription<'fb>> for odf::MqttTopicSubscription {
    fn deserialize(proxy: fb::MqttTopicSubscription<'fb>) -> Self {
        odf::MqttTopicSubscription {
            path: proxy.path().map(|v| v.to_owned()).unwrap(),
            qos: proxy.qos().map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PrepStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
                odf::PrepStep::Decompress(odf::PrepStepDecompress::deserialize(unsafe {
                    fb::PrepStepDecompress::init_from_table(table)
                }))
            }
            fb::PrepStep::PrepStepPipe => {
                odf::PrepStep::Pipe(odf::PrepStepPipe::deserialize(unsafe {
                    fb::PrepStepPipe::init_from_table(table)
                }))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PrepStepDecompress
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstepdecompress-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PrepStepPipe
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepsteppipe-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryrequest-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::RawQueryRequest {
    type OffsetT = WIPOffset<fb::RawQueryRequest<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let input_data_paths_offset = {
            let offsets: Vec<_> = self
                .input_data_paths
                .iter()
                .map(|i| fb.create_string(i.to_str().unwrap()))
                .collect();
            fb.create_vector(&offsets)
        };
        let transform_offset = { self.transform.serialize(fb) };
        let output_data_path_offset = { fb.create_string(self.output_data_path.to_str().unwrap()) };
        let mut builder = fb::RawQueryRequestBuilder::new(fb);
        builder.add_input_data_paths(input_data_paths_offset);
        builder.add_transform_type(transform_offset.0);
        builder.add_transform(transform_offset.1);
        builder.add_output_data_path(output_data_path_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::RawQueryRequest<'fb>> for odf::RawQueryRequest {
    fn deserialize(proxy: fb::RawQueryRequest<'fb>) -> Self {
        odf::RawQueryRequest {
            input_data_paths: proxy
                .input_data_paths()
                .map(|v| v.iter().map(|i| PathBuf::from(i)).collect())
                .unwrap(),
            transform: proxy
                .transform()
                .map(|v| odf::Transform::deserialize(v, proxy.transform_type()))
                .unwrap(),
            output_data_path: proxy.output_data_path().map(|v| PathBuf::from(v)).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponse-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::RawQueryResponse> for odf::RawQueryResponse {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::RawQueryResponse, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::RawQueryResponse::Progress(v) => (
                fb::RawQueryResponse::RawQueryResponseProgress,
                v.serialize(fb).as_union_value(),
            ),
            odf::RawQueryResponse::Success(v) => (
                fb::RawQueryResponse::RawQueryResponseSuccess,
                v.serialize(fb).as_union_value(),
            ),
            odf::RawQueryResponse::InvalidQuery(v) => (
                fb::RawQueryResponse::RawQueryResponseInvalidQuery,
                v.serialize(fb).as_union_value(),
            ),
            odf::RawQueryResponse::InternalError(v) => (
                fb::RawQueryResponse::RawQueryResponseInternalError,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::RawQueryResponse> for odf::RawQueryResponse {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::RawQueryResponse) -> Self {
        match t {
            fb::RawQueryResponse::RawQueryResponseProgress => {
                odf::RawQueryResponse::Progress(odf::RawQueryResponseProgress::deserialize(
                    unsafe { fb::RawQueryResponseProgress::init_from_table(table) },
                ))
            }
            fb::RawQueryResponse::RawQueryResponseSuccess => {
                odf::RawQueryResponse::Success(odf::RawQueryResponseSuccess::deserialize(unsafe {
                    fb::RawQueryResponseSuccess::init_from_table(table)
                }))
            }
            fb::RawQueryResponse::RawQueryResponseInvalidQuery => {
                odf::RawQueryResponse::InvalidQuery(odf::RawQueryResponseInvalidQuery::deserialize(
                    unsafe { fb::RawQueryResponseInvalidQuery::init_from_table(table) },
                ))
            }
            fb::RawQueryResponse::RawQueryResponseInternalError => {
                odf::RawQueryResponse::InternalError(
                    odf::RawQueryResponseInternalError::deserialize(unsafe {
                        fb::RawQueryResponseInternalError::init_from_table(table)
                    }),
                )
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponseInternalError
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponseinternalerror-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::RawQueryResponseInternalError {
    type OffsetT = WIPOffset<fb::RawQueryResponseInternalError<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let message_offset = { fb.create_string(&self.message) };
        let backtrace_offset = self.backtrace.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::RawQueryResponseInternalErrorBuilder::new(fb);
        builder.add_message(message_offset);
        backtrace_offset.map(|off| builder.add_backtrace(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::RawQueryResponseInternalError<'fb>>
    for odf::RawQueryResponseInternalError
{
    fn deserialize(proxy: fb::RawQueryResponseInternalError<'fb>) -> Self {
        odf::RawQueryResponseInternalError {
            message: proxy.message().map(|v| v.to_owned()).unwrap(),
            backtrace: proxy.backtrace().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponseInvalidQuery
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponseinvalidquery-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::RawQueryResponseInvalidQuery {
    type OffsetT = WIPOffset<fb::RawQueryResponseInvalidQuery<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let message_offset = { fb.create_string(&self.message) };
        let mut builder = fb::RawQueryResponseInvalidQueryBuilder::new(fb);
        builder.add_message(message_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::RawQueryResponseInvalidQuery<'fb>>
    for odf::RawQueryResponseInvalidQuery
{
    fn deserialize(proxy: fb::RawQueryResponseInvalidQuery<'fb>) -> Self {
        odf::RawQueryResponseInvalidQuery {
            message: proxy.message().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponseProgress
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponseprogress-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::RawQueryResponseProgress {
    type OffsetT = WIPOffset<fb::RawQueryResponseProgress<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::RawQueryResponseProgressBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::RawQueryResponseProgress<'fb>>
    for odf::RawQueryResponseProgress
{
    fn deserialize(proxy: fb::RawQueryResponseProgress<'fb>) -> Self {
        odf::RawQueryResponseProgress {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponseSuccess
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponsesuccess-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::RawQueryResponseSuccess {
    type OffsetT = WIPOffset<fb::RawQueryResponseSuccess<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::RawQueryResponseSuccessBuilder::new(fb);
        builder.add_num_records(self.num_records);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::RawQueryResponseSuccess<'fb>>
    for odf::RawQueryResponseSuccess
{
    fn deserialize(proxy: fb::RawQueryResponseSuccess<'fb>) -> Self {
        odf::RawQueryResponseSuccess {
            num_records: proxy.num_records(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::ReadStep> for odf::ReadStep {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::ReadStep, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::ReadStep::Csv(v) => (fb::ReadStep::ReadStepCsv, v.serialize(fb).as_union_value()),
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
            odf::ReadStep::Json(v) => {
                (fb::ReadStep::ReadStepJson, v.serialize(fb).as_union_value())
            }
            odf::ReadStep::NdJson(v) => (
                fb::ReadStep::ReadStepNdJson,
                v.serialize(fb).as_union_value(),
            ),
            odf::ReadStep::NdGeoJson(v) => (
                fb::ReadStep::ReadStepNdGeoJson,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::ReadStep> for odf::ReadStep {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::ReadStep) -> Self {
        match t {
            fb::ReadStep::ReadStepCsv => {
                odf::ReadStep::Csv(odf::ReadStepCsv::deserialize(unsafe {
                    fb::ReadStepCsv::init_from_table(table)
                }))
            }
            fb::ReadStep::ReadStepGeoJson => {
                odf::ReadStep::GeoJson(odf::ReadStepGeoJson::deserialize(unsafe {
                    fb::ReadStepGeoJson::init_from_table(table)
                }))
            }
            fb::ReadStep::ReadStepEsriShapefile => {
                odf::ReadStep::EsriShapefile(odf::ReadStepEsriShapefile::deserialize(unsafe {
                    fb::ReadStepEsriShapefile::init_from_table(table)
                }))
            }
            fb::ReadStep::ReadStepParquet => {
                odf::ReadStep::Parquet(odf::ReadStepParquet::deserialize(unsafe {
                    fb::ReadStepParquet::init_from_table(table)
                }))
            }
            fb::ReadStep::ReadStepJson => {
                odf::ReadStep::Json(odf::ReadStepJson::deserialize(unsafe {
                    fb::ReadStepJson::init_from_table(table)
                }))
            }
            fb::ReadStep::ReadStepNdJson => {
                odf::ReadStep::NdJson(odf::ReadStepNdJson::deserialize(unsafe {
                    fb::ReadStepNdJson::init_from_table(table)
                }))
            }
            fb::ReadStep::ReadStepNdGeoJson => {
                odf::ReadStep::NdGeoJson(odf::ReadStepNdGeoJson::deserialize(unsafe {
                    fb::ReadStepNdGeoJson::init_from_table(table)
                }))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepCsv
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepcsv-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        let null_value_offset = self.null_value.as_ref().map(|v| fb.create_string(&v));
        let date_format_offset = self.date_format.as_ref().map(|v| fb.create_string(&v));
        let timestamp_format_offset = self.timestamp_format.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::ReadStepCsvBuilder::new(fb);
        schema_offset.map(|off| builder.add_schema(off));
        separator_offset.map(|off| builder.add_separator(off));
        encoding_offset.map(|off| builder.add_encoding(off));
        quote_offset.map(|off| builder.add_quote(off));
        escape_offset.map(|off| builder.add_escape(off));
        self.header.map(|v| builder.add_header(v));
        self.infer_schema.map(|v| builder.add_infer_schema(v));
        null_value_offset.map(|off| builder.add_null_value(off));
        date_format_offset.map(|off| builder.add_date_format(off));
        timestamp_format_offset.map(|off| builder.add_timestamp_format(off));
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
            header: proxy.header().map(|v| v),
            infer_schema: proxy.infer_schema().map(|v| v),
            null_value: proxy.null_value().map(|v| v.to_owned()),
            date_format: proxy.date_format().map(|v| v.to_owned()),
            timestamp_format: proxy.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepEsriShapefile
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepesrishapefile-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepGeoJson
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepgeojson-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepJson
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepjson-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::ReadStepJson {
    type OffsetT = WIPOffset<fb::ReadStepJson<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let sub_path_offset = self.sub_path.as_ref().map(|v| fb.create_string(&v));
        let schema_offset = self.schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let date_format_offset = self.date_format.as_ref().map(|v| fb.create_string(&v));
        let encoding_offset = self.encoding.as_ref().map(|v| fb.create_string(&v));
        let timestamp_format_offset = self.timestamp_format.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::ReadStepJsonBuilder::new(fb);
        sub_path_offset.map(|off| builder.add_sub_path(off));
        schema_offset.map(|off| builder.add_schema(off));
        date_format_offset.map(|off| builder.add_date_format(off));
        encoding_offset.map(|off| builder.add_encoding(off));
        timestamp_format_offset.map(|off| builder.add_timestamp_format(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepJson<'fb>> for odf::ReadStepJson {
    fn deserialize(proxy: fb::ReadStepJson<'fb>) -> Self {
        odf::ReadStepJson {
            sub_path: proxy.sub_path().map(|v| v.to_owned()),
            schema: proxy
                .schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            date_format: proxy.date_format().map(|v| v.to_owned()),
            encoding: proxy.encoding().map(|v| v.to_owned()),
            timestamp_format: proxy.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepNdGeoJson
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepndgeojson-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::ReadStepNdGeoJson {
    type OffsetT = WIPOffset<fb::ReadStepNdGeoJson<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let schema_offset = self.schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let mut builder = fb::ReadStepNdGeoJsonBuilder::new(fb);
        schema_offset.map(|off| builder.add_schema(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepNdGeoJson<'fb>> for odf::ReadStepNdGeoJson {
    fn deserialize(proxy: fb::ReadStepNdGeoJson<'fb>) -> Self {
        odf::ReadStepNdGeoJson {
            schema: proxy
                .schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepNdJson
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepndjson-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::ReadStepNdJson {
    type OffsetT = WIPOffset<fb::ReadStepNdJson<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let schema_offset = self.schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let date_format_offset = self.date_format.as_ref().map(|v| fb.create_string(&v));
        let encoding_offset = self.encoding.as_ref().map(|v| fb.create_string(&v));
        let timestamp_format_offset = self.timestamp_format.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::ReadStepNdJsonBuilder::new(fb);
        schema_offset.map(|off| builder.add_schema(off));
        date_format_offset.map(|off| builder.add_date_format(off));
        encoding_offset.map(|off| builder.add_encoding(off));
        timestamp_format_offset.map(|off| builder.add_timestamp_format(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepNdJson<'fb>> for odf::ReadStepNdJson {
    fn deserialize(proxy: fb::ReadStepNdJson<'fb>) -> Self {
        odf::ReadStepNdJson {
            schema: proxy
                .schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            date_format: proxy.date_format().map(|v| v.to_owned()),
            encoding: proxy.encoding().map(|v| v.to_owned()),
            timestamp_format: proxy.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepParquet
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepparquet-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RequestHeader
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#requestheader-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Seed
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::Seed {
    type OffsetT = WIPOffset<fb::Seed<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let dataset_id_offset = { fb.create_vector(&self.dataset_id.as_bytes().as_slice()) };
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
                .map(|v| odf::DatasetID::from_bytes(v.bytes()).unwrap())
                .unwrap(),
            dataset_kind: proxy.dataset_kind().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetAttachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setattachments-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetDataSchema
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setdataschema-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::SetDataSchema {
    type OffsetT = WIPOffset<fb::SetDataSchema<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let schema_offset = { fb.create_vector(&self.schema[..]) };
        let mut builder = fb::SetDataSchemaBuilder::new(fb);
        builder.add_schema(schema_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SetDataSchema<'fb>> for odf::SetDataSchema {
    fn deserialize(proxy: fb::SetDataSchema<'fb>) -> Self {
        odf::SetDataSchema {
            schema: proxy.schema().map(|v| v.bytes().to_vec()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetInfo
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setinfo-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetLicense
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setlicense-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetPollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetVocab
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::SetVocab {
    type OffsetT = WIPOffset<fb::SetVocab<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let offset_column_offset = self.offset_column.as_ref().map(|v| fb.create_string(&v));
        let operation_type_column_offset = self
            .operation_type_column
            .as_ref()
            .map(|v| fb.create_string(&v));
        let system_time_column_offset = self
            .system_time_column
            .as_ref()
            .map(|v| fb.create_string(&v));
        let event_time_column_offset = self
            .event_time_column
            .as_ref()
            .map(|v| fb.create_string(&v));
        let mut builder = fb::SetVocabBuilder::new(fb);
        offset_column_offset.map(|off| builder.add_offset_column(off));
        operation_type_column_offset.map(|off| builder.add_operation_type_column(off));
        system_time_column_offset.map(|off| builder.add_system_time_column(off));
        event_time_column_offset.map(|off| builder.add_event_time_column(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SetVocab<'fb>> for odf::SetVocab {
    fn deserialize(proxy: fb::SetVocab<'fb>) -> Self {
        odf::SetVocab {
            offset_column: proxy.offset_column().map(|v| v.to_owned()),
            operation_type_column: proxy.operation_type_column().map(|v| v.to_owned()),
            system_time_column: proxy.system_time_column().map(|v| v.to_owned()),
            event_time_column: proxy.event_time_column().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::SourceCaching> for odf::SourceCaching {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::SourceCaching, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::SourceCaching::Forever(v) => (
                fb::SourceCaching::SourceCachingForever,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::SourceCaching> for odf::SourceCaching {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::SourceCaching) -> Self {
        match t {
            fb::SourceCaching::SourceCachingForever => {
                odf::SourceCaching::Forever(odf::SourceCachingForever::deserialize(unsafe {
                    fb::SourceCachingForever::init_from_table(table)
                }))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceCachingForever
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecachingforever-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::SourceCachingForever {
    type OffsetT = WIPOffset<fb::SourceCachingForever<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::SourceCachingForeverBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SourceCachingForever<'fb>> for odf::SourceCachingForever {
    fn deserialize(proxy: fb::SourceCachingForever<'fb>) -> Self {
        odf::SourceCachingForever {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceOrdering
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourceordering-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceState
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcestate-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::SourceState {
    type OffsetT = WIPOffset<fb::SourceState<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let source_name_offset = { fb.create_string(&self.source_name) };
        let kind_offset = { fb.create_string(&self.kind) };
        let value_offset = { fb.create_string(&self.value) };
        let mut builder = fb::SourceStateBuilder::new(fb);
        builder.add_source_name(source_name_offset);
        builder.add_kind(kind_offset);
        builder.add_value(value_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SourceState<'fb>> for odf::SourceState {
    fn deserialize(proxy: fb::SourceState<'fb>) -> Self {
        odf::SourceState {
            source_name: proxy.source_name().map(|v| v.to_owned()).unwrap(),
            kind: proxy.kind().map(|v| v.to_owned()).unwrap(),
            value: proxy.value().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SqlQueryStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sqlquerystep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
            fb::Transform::TransformSql => {
                odf::Transform::Sql(odf::TransformSql::deserialize(unsafe {
                    fb::TransformSql::init_from_table(table)
                }))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformSql
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformsql-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::TransformInput {
    type OffsetT = WIPOffset<fb::TransformInput<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let dataset_ref_offset = { fb.create_string(&self.dataset_ref.to_string()) };
        let alias_offset = self.alias.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::TransformInputBuilder::new(fb);
        builder.add_dataset_ref(dataset_ref_offset);
        alias_offset.map(|off| builder.add_alias(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TransformInput<'fb>> for odf::TransformInput {
    fn deserialize(proxy: fb::TransformInput<'fb>) -> Self {
        odf::TransformInput {
            dataset_ref: proxy
                .dataset_ref()
                .map(|v| odf::DatasetRef::try_from(v).unwrap())
                .unwrap(),
            alias: proxy.alias().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequest-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::TransformRequest {
    type OffsetT = WIPOffset<fb::TransformRequest<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let dataset_id_offset = { fb.create_vector(&self.dataset_id.as_bytes().as_slice()) };
        let dataset_alias_offset = { fb.create_string(&self.dataset_alias.to_string()) };
        let vocab_offset = { self.vocab.serialize(fb) };
        let transform_offset = { self.transform.serialize(fb) };
        let query_inputs_offset = {
            let offsets: Vec<_> = self.query_inputs.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        };
        let prev_checkpoint_path_offset = self
            .prev_checkpoint_path
            .as_ref()
            .map(|v| fb.create_string(v.to_str().unwrap()));
        let new_checkpoint_path_offset =
            { fb.create_string(self.new_checkpoint_path.to_str().unwrap()) };
        let new_data_path_offset = { fb.create_string(self.new_data_path.to_str().unwrap()) };
        let mut builder = fb::TransformRequestBuilder::new(fb);
        builder.add_dataset_id(dataset_id_offset);
        builder.add_dataset_alias(dataset_alias_offset);
        builder.add_system_time(&datetime_to_fb(&self.system_time));
        builder.add_vocab(vocab_offset);
        builder.add_transform_type(transform_offset.0);
        builder.add_transform(transform_offset.1);
        builder.add_query_inputs(query_inputs_offset);
        builder.add_next_offset(self.next_offset);
        prev_checkpoint_path_offset.map(|off| builder.add_prev_checkpoint_path(off));
        builder.add_new_checkpoint_path(new_checkpoint_path_offset);
        builder.add_new_data_path(new_data_path_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TransformRequest<'fb>> for odf::TransformRequest {
    fn deserialize(proxy: fb::TransformRequest<'fb>) -> Self {
        odf::TransformRequest {
            dataset_id: proxy
                .dataset_id()
                .map(|v| odf::DatasetID::from_bytes(v.bytes()).unwrap())
                .unwrap(),
            dataset_alias: proxy
                .dataset_alias()
                .map(|v| odf::DatasetAlias::try_from(v).unwrap())
                .unwrap(),
            system_time: proxy.system_time().map(|v| fb_to_datetime(v)).unwrap(),
            vocab: proxy
                .vocab()
                .map(|v| odf::DatasetVocabulary::deserialize(v))
                .unwrap(),
            transform: proxy
                .transform()
                .map(|v| odf::Transform::deserialize(v, proxy.transform_type()))
                .unwrap(),
            query_inputs: proxy
                .query_inputs()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::TransformRequestInput::deserialize(i))
                        .collect()
                })
                .unwrap(),
            next_offset: proxy.next_offset(),
            prev_checkpoint_path: proxy.prev_checkpoint_path().map(|v| PathBuf::from(v)),
            new_checkpoint_path: proxy
                .new_checkpoint_path()
                .map(|v| PathBuf::from(v))
                .unwrap(),
            new_data_path: proxy.new_data_path().map(|v| PathBuf::from(v)).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformRequestInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequestinput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::TransformRequestInput {
    type OffsetT = WIPOffset<fb::TransformRequestInput<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let dataset_id_offset = { fb.create_vector(&self.dataset_id.as_bytes().as_slice()) };
        let dataset_alias_offset = { fb.create_string(&self.dataset_alias.to_string()) };
        let query_alias_offset = { fb.create_string(&self.query_alias) };
        let vocab_offset = { self.vocab.serialize(fb) };
        let offset_interval_offset = self.offset_interval.as_ref().map(|v| v.serialize(fb));
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
        let mut builder = fb::TransformRequestInputBuilder::new(fb);
        builder.add_dataset_id(dataset_id_offset);
        builder.add_dataset_alias(dataset_alias_offset);
        builder.add_query_alias(query_alias_offset);
        builder.add_vocab(vocab_offset);
        offset_interval_offset.map(|off| builder.add_offset_interval(off));
        builder.add_data_paths(data_paths_offset);
        builder.add_schema_file(schema_file_offset);
        builder.add_explicit_watermarks(explicit_watermarks_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TransformRequestInput<'fb>> for odf::TransformRequestInput {
    fn deserialize(proxy: fb::TransformRequestInput<'fb>) -> Self {
        odf::TransformRequestInput {
            dataset_id: proxy
                .dataset_id()
                .map(|v| odf::DatasetID::from_bytes(v.bytes()).unwrap())
                .unwrap(),
            dataset_alias: proxy
                .dataset_alias()
                .map(|v| odf::DatasetAlias::try_from(v).unwrap())
                .unwrap(),
            query_alias: proxy.query_alias().map(|v| v.to_owned()).unwrap(),
            vocab: proxy
                .vocab()
                .map(|v| odf::DatasetVocabulary::deserialize(v))
                .unwrap(),
            offset_interval: proxy
                .offset_interval()
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponse-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::TransformResponse> for odf::TransformResponse {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::TransformResponse, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::TransformResponse::Progress(v) => (
                fb::TransformResponse::TransformResponseProgress,
                v.serialize(fb).as_union_value(),
            ),
            odf::TransformResponse::Success(v) => (
                fb::TransformResponse::TransformResponseSuccess,
                v.serialize(fb).as_union_value(),
            ),
            odf::TransformResponse::InvalidQuery(v) => (
                fb::TransformResponse::TransformResponseInvalidQuery,
                v.serialize(fb).as_union_value(),
            ),
            odf::TransformResponse::InternalError(v) => (
                fb::TransformResponse::TransformResponseInternalError,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::TransformResponse> for odf::TransformResponse {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::TransformResponse) -> Self {
        match t {
            fb::TransformResponse::TransformResponseProgress => {
                odf::TransformResponse::Progress(odf::TransformResponseProgress::deserialize(
                    unsafe { fb::TransformResponseProgress::init_from_table(table) },
                ))
            }
            fb::TransformResponse::TransformResponseSuccess => {
                odf::TransformResponse::Success(odf::TransformResponseSuccess::deserialize(
                    unsafe { fb::TransformResponseSuccess::init_from_table(table) },
                ))
            }
            fb::TransformResponse::TransformResponseInvalidQuery => {
                odf::TransformResponse::InvalidQuery(
                    odf::TransformResponseInvalidQuery::deserialize(unsafe {
                        fb::TransformResponseInvalidQuery::init_from_table(table)
                    }),
                )
            }
            fb::TransformResponse::TransformResponseInternalError => {
                odf::TransformResponse::InternalError(
                    odf::TransformResponseInternalError::deserialize(unsafe {
                        fb::TransformResponseInternalError::init_from_table(table)
                    }),
                )
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponseInternalError
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponseinternalerror-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::TransformResponseInternalError {
    type OffsetT = WIPOffset<fb::TransformResponseInternalError<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let message_offset = { fb.create_string(&self.message) };
        let backtrace_offset = self.backtrace.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::TransformResponseInternalErrorBuilder::new(fb);
        builder.add_message(message_offset);
        backtrace_offset.map(|off| builder.add_backtrace(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TransformResponseInternalError<'fb>>
    for odf::TransformResponseInternalError
{
    fn deserialize(proxy: fb::TransformResponseInternalError<'fb>) -> Self {
        odf::TransformResponseInternalError {
            message: proxy.message().map(|v| v.to_owned()).unwrap(),
            backtrace: proxy.backtrace().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponseInvalidQuery
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponseinvalidquery-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::TransformResponseInvalidQuery {
    type OffsetT = WIPOffset<fb::TransformResponseInvalidQuery<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let message_offset = { fb.create_string(&self.message) };
        let mut builder = fb::TransformResponseInvalidQueryBuilder::new(fb);
        builder.add_message(message_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TransformResponseInvalidQuery<'fb>>
    for odf::TransformResponseInvalidQuery
{
    fn deserialize(proxy: fb::TransformResponseInvalidQuery<'fb>) -> Self {
        odf::TransformResponseInvalidQuery {
            message: proxy.message().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponseProgress
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponseprogress-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::TransformResponseProgress {
    type OffsetT = WIPOffset<fb::TransformResponseProgress<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::TransformResponseProgressBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TransformResponseProgress<'fb>>
    for odf::TransformResponseProgress
{
    fn deserialize(proxy: fb::TransformResponseProgress<'fb>) -> Self {
        odf::TransformResponseProgress {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponseSuccess
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponsesuccess-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::TransformResponseSuccess {
    type OffsetT = WIPOffset<fb::TransformResponseSuccess<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let new_offset_interval_offset = self.new_offset_interval.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::TransformResponseSuccessBuilder::new(fb);
        new_offset_interval_offset.map(|off| builder.add_new_offset_interval(off));
        self.new_watermark
            .map(|v| builder.add_new_watermark(&datetime_to_fb(&v)));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TransformResponseSuccess<'fb>>
    for odf::TransformResponseSuccess
{
    fn deserialize(proxy: fb::TransformResponseSuccess<'fb>) -> Self {
        odf::TransformResponseSuccess {
            new_offset_interval: proxy
                .new_offset_interval()
                .map(|v| odf::OffsetInterval::deserialize(v)),
            new_watermark: proxy.new_watermark().map(|v| fb_to_datetime(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Watermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn datetime_to_fb(dt: &DateTime<Utc>) -> fb::Timestamp {
    fb::Timestamp::new(
        dt.year(),
        dt.ordinal() as u16,
        dt.naive_utc().num_seconds_from_midnight(),
        dt.naive_utc().nanosecond(),
    )
}

fn fb_to_datetime(dt: &fb::Timestamp) -> DateTime<Utc> {
    let naive_date_time = NaiveDate::from_yo_opt(dt.year(), dt.ordinal() as u32)
        .unwrap()
        .and_time(
            NaiveTime::from_num_seconds_from_midnight_opt(
                dt.seconds_from_midnight(),
                dt.nanoseconds(),
            )
            .unwrap(),
        );
    Utc.from_local_datetime(&naive_date_time).unwrap()
}
