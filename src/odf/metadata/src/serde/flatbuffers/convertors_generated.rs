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

use std::convert::TryFrom;
use std::path::PathBuf;

use ::flatbuffers::{FlatBufferBuilder, Table, UnionWIPOffset, WIPOffset};
use chrono::prelude::*;
use setty::types::{ByteSize, DurationString};

use super::proxies_generated as fb;
use crate as odf;

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
// AccountSpec
// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/AccountSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::auth::AccountSpec {
    type OffsetT = WIPOffset<fb::AccountSpec<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let did_offset = self.did.as_ref().map(|v| fb.create_vector(&v.as_bytes()));
        let display_name_offset = self.display_name.as_ref().map(|v| fb.create_string(&v));
        let email_offset = { fb.create_string(&self.email) };
        let avatar_url_offset = self.avatar_url.as_ref().map(|v| fb.create_string(&v));
        let password_offset = self.password.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::AccountSpecBuilder::new(fb);
        did_offset.map(|off| builder.add_did(off));
        self.account_type
            .map(|v| builder.add_account_type(v.into()));
        display_name_offset.map(|off| builder.add_display_name(off));
        builder.add_email(email_offset);
        avatar_url_offset.map(|off| builder.add_avatar_url(off));
        password_offset.map(|off| builder.add_password(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::AccountSpec<'fb>> for odf::auth::AccountSpec {
    fn deserialize(proxy: fb::AccountSpec<'fb>) -> Self {
        odf::auth::AccountSpec {
            did: proxy
                .did()
                .map(|v| odf::auth::AccountID::from_bytes(v.bytes()).unwrap()),
            account_type: proxy.account_type().map(|v| v.into()),
            display_name: proxy.display_name().map(|v| v.to_owned()),
            email: proxy.email().map(|v| v.to_owned()).unwrap(),
            avatar_url: proxy.avatar_url().map(|v| v.to_owned()),
            password: proxy
                .password()
                .map(|v| odf::config::Secret::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountType
// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/AccountType
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::auth::AccountType> for fb::AccountType {
    fn from(v: odf::auth::AccountType) -> Self {
        match v {
            odf::auth::AccountType::User => fb::AccountType::User,
            odf::auth::AccountType::Organization => fb::AccountType::Organization,
        }
    }
}

impl Into<odf::auth::AccountType> for fb::AccountType {
    fn into(self) -> odf::auth::AccountType {
        match self {
            fb::AccountType::User => odf::auth::AccountType::User,
            fb::AccountType::Organization => odf::auth::AccountType::Organization,
            _ => panic!("Invalid enum value: {}", self.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddData
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/AddData
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::AddData {
    type OffsetT = WIPOffset<fb::AddData<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let prev_checkpoint_offset = self
            .prev_checkpoint
            .as_ref()
            .map(|v| fb.create_vector(&v.as_bytes().as_slice()));
        let new_data_offset = self.new_data.as_ref().map(|v| v.serialize(fb));
        let new_checkpoint_offset = self.new_checkpoint.as_ref().map(|v| v.serialize(fb));
        let new_source_state_offset = self.new_source_state.as_ref().map(|v| v.serialize(fb));
        let extra_offset = self.extra.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::AddDataBuilder::new(fb);
        prev_checkpoint_offset.map(|off| builder.add_prev_checkpoint(off));
        self.prev_offset.map(|v| builder.add_prev_offset(v));
        new_data_offset.map(|off| builder.add_new_data(off));
        new_checkpoint_offset.map(|off| builder.add_new_checkpoint(off));
        self.new_watermark
            .map(|v| builder.add_new_watermark(&datetime_to_fb(&v)));
        new_source_state_offset.map(|off| builder.add_new_source_state(off));
        extra_offset.map(|off| builder.add_extra(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::AddData<'fb>> for odf::dataset::AddData {
    fn deserialize(proxy: fb::AddData<'fb>) -> Self {
        odf::dataset::AddData {
            prev_checkpoint: proxy
                .prev_checkpoint()
                .map(|v| odf::Multihash::from_bytes(v.bytes()).unwrap()),
            prev_offset: proxy.prev_offset().map(|v| v),
            new_data: proxy
                .new_data()
                .map(|v| odf::dataset::DataSlice::deserialize(v)),
            new_checkpoint: proxy
                .new_checkpoint()
                .map(|v| odf::dataset::Checkpoint::deserialize(v)),
            new_watermark: proxy.new_watermark().map(|v| fb_to_datetime(v)),
            new_source_state: proxy
                .new_source_state()
                .map(|v| odf::source::SourceState::deserialize(v)),
            extra: proxy
                .extra()
                .map(|v| odf::data::ExtraAttributes::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddPushSource
// Schema: https://opendatafabric.org/schemas/legacy/v0/AddPushSource
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::legacy::AddPushSource {
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

impl<'fb> FlatbuffersDeserializable<fb::AddPushSource<'fb>> for odf::legacy::AddPushSource {
    fn deserialize(proxy: fb::AddPushSource<'fb>) -> Self {
        odf::legacy::AddPushSource {
            source_name: proxy.source_name().map(|v| v.to_owned()).unwrap(),
            read: proxy
                .read()
                .map(|v| odf::source::ReadStep::deserialize(v, proxy.read_type()))
                .unwrap(),
            preprocess: proxy
                .preprocess()
                .map(|v| odf::dataset::Transform::deserialize(v, proxy.preprocess_type())),
            merge: proxy
                .merge()
                .map(|v| odf::source::MergeStrategy::deserialize(v, proxy.merge_type()))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AttachmentEmbedded
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/AttachmentEmbedded
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::AttachmentEmbedded {
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

impl<'fb> FlatbuffersDeserializable<fb::AttachmentEmbedded<'fb>>
    for odf::dataset::AttachmentEmbedded
{
    fn deserialize(proxy: fb::AttachmentEmbedded<'fb>) -> Self {
        odf::dataset::AttachmentEmbedded {
            path: proxy.path().map(|v| v.to_owned()).unwrap(),
            content: proxy.content().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Attachments
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Attachments
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::Attachments> for odf::dataset::Attachments {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::Attachments, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::dataset::Attachments::Embedded(v) => (
                fb::Attachments::AttachmentsEmbedded,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::Attachments> for odf::dataset::Attachments {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::Attachments) -> Self {
        match t {
            fb::Attachments::AttachmentsEmbedded => {
                odf::dataset::Attachments::Embedded(odf::dataset::AttachmentsEmbedded::deserialize(
                    unsafe { fb::AttachmentsEmbedded::init_from_table(table) },
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AttachmentsEmbedded
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Attachments#/$defs/Embedded
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::AttachmentsEmbedded {
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

impl<'fb> FlatbuffersDeserializable<fb::AttachmentsEmbedded<'fb>>
    for odf::dataset::AttachmentsEmbedded
{
    fn deserialize(proxy: fb::AttachmentsEmbedded<'fb>) -> Self {
        odf::dataset::AttachmentsEmbedded {
            items: proxy
                .items()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::dataset::AttachmentEmbedded::deserialize(i))
                        .collect()
                })
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Attribute
// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Attribute
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::auth::Attribute {
    type OffsetT = WIPOffset<fb::Attribute<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let object_offset = { self.object.serialize(fb) };
        let name_offset = { fb.create_string(&self.name) };
        let value_offset = { fb.create_string(&serde_json::to_string(&self.value).unwrap()) };
        let mut builder = fb::AttributeBuilder::new(fb);
        builder.add_object(object_offset);
        builder.add_name(name_offset);
        builder.add_value(value_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Attribute<'fb>> for odf::auth::Attribute {
    fn deserialize(proxy: fb::Attribute<'fb>) -> Self {
        odf::auth::Attribute {
            object: proxy
                .object()
                .map(|v| odf::resource::ResourceRef::deserialize(v))
                .unwrap(),
            name: proxy.name().map(|v| v.to_owned()).unwrap(),
            value: proxy
                .value()
                .map(|v| serde_json::from_str(v).unwrap())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AwsCredentials
// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/AwsCredentials
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::storage::AwsCredentials {
    type OffsetT = WIPOffset<fb::AwsCredentials<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let access_key_offset = self.access_key.as_ref().map(|v| v.serialize(fb));
        let secret_key_offset = self.secret_key.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::AwsCredentialsBuilder::new(fb);
        access_key_offset.map(|off| builder.add_access_key(off));
        secret_key_offset.map(|off| builder.add_secret_key(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::AwsCredentials<'fb>> for odf::storage::AwsCredentials {
    fn deserialize(proxy: fb::AwsCredentials<'fb>) -> Self {
        odf::storage::AwsCredentials {
            access_key: proxy
                .access_key()
                .map(|v| odf::config::ValueRef::deserialize(v)),
            secret_key: proxy
                .secret_key()
                .map(|v| odf::config::ValueRef::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Checkpoint
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Checkpoint
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::Checkpoint {
    type OffsetT = WIPOffset<fb::Checkpoint<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let physical_hash_offset = { fb.create_vector(&self.physical_hash.as_bytes().as_slice()) };
        let mut builder = fb::CheckpointBuilder::new(fb);
        builder.add_physical_hash(physical_hash_offset);
        builder.add_size(self.size);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Checkpoint<'fb>> for odf::dataset::Checkpoint {
    fn deserialize(proxy: fb::Checkpoint<'fb>) -> Self {
        odf::dataset::Checkpoint {
            physical_hash: proxy
                .physical_hash()
                .map(|v| odf::Multihash::from_bytes(v.bytes()).unwrap())
                .unwrap(),
            size: proxy.size(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CompactionParams
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/CompactionParams
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::CompactionParams {
    type OffsetT = WIPOffset<fb::CompactionParams<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::CompactionParamsBuilder::new(fb);
        self.max_slice_size
            .map(|v| builder.add_max_slice_size(v.as_u64()));
        self.max_slice_records
            .map(|v| builder.add_max_slice_records(v));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::CompactionParams<'fb>> for odf::dataset::CompactionParams {
    fn deserialize(proxy: fb::CompactionParams<'fb>) -> Self {
        odf::dataset::CompactionParams {
            max_slice_size: proxy.max_slice_size().map(|v| ByteSize::from(v)),
            max_slice_records: proxy.max_slice_records().map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CompressionFormat
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/CompressionFormat
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::source::CompressionFormat> for fb::CompressionFormat {
    fn from(v: odf::source::CompressionFormat) -> Self {
        match v {
            odf::source::CompressionFormat::Gzip => fb::CompressionFormat::Gzip,
            odf::source::CompressionFormat::Zip => fb::CompressionFormat::Zip,
        }
    }
}

impl Into<odf::source::CompressionFormat> for fb::CompressionFormat {
    fn into(self) -> odf::source::CompressionFormat {
        match self {
            fb::CompressionFormat::Gzip => odf::source::CompressionFormat::Gzip,
            fb::CompressionFormat::Zip => odf::source::CompressionFormat::Zip,
            _ => panic!("Invalid enum value: {}", self.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataField
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataField
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataField {
    type OffsetT = WIPOffset<fb::DataField<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let name_offset = { fb.create_string(&self.name) };
        let r#type_offset = { self.r#type.serialize(fb) };
        let extra_offset = self.extra.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::DataFieldBuilder::new(fb);
        builder.add_name(name_offset);
        builder.add_type_type(type_offset.0);
        builder.add_type_(type_offset.1);
        extra_offset.map(|off| builder.add_extra(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataField<'fb>> for odf::data::DataField {
    fn deserialize(proxy: fb::DataField<'fb>) -> Self {
        odf::data::DataField {
            name: proxy.name().map(|v| v.to_owned()).unwrap(),
            r#type: proxy
                .type_()
                .map(|v| odf::data::DataType::deserialize(v, proxy.r#type_type()))
                .unwrap(),
            extra: proxy
                .extra()
                .map(|v| odf::data::ExtraAttributes::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSchema
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataSchema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataSchema {
    type OffsetT = WIPOffset<fb::DataSchema<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let fields_offset = {
            let offsets: Vec<_> = self.fields.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        };
        let extra_offset = self.extra.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::DataSchemaBuilder::new(fb);
        builder.add_fields(fields_offset);
        extra_offset.map(|off| builder.add_extra(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataSchema<'fb>> for odf::data::DataSchema {
    fn deserialize(proxy: fb::DataSchema<'fb>) -> Self {
        odf::data::DataSchema {
            fields: proxy
                .fields()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::data::DataField::deserialize(i))
                        .collect()
                })
                .unwrap(),
            extra: proxy
                .extra()
                .map(|v| odf::data::ExtraAttributes::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSlice
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DataSlice
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::DataSlice {
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

impl<'fb> FlatbuffersDeserializable<fb::DataSlice<'fb>> for odf::dataset::DataSlice {
    fn deserialize(proxy: fb::DataSlice<'fb>) -> Self {
        odf::dataset::DataSlice {
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
                .map(|v| odf::dataset::OffsetInterval::deserialize(v))
                .unwrap(),
            size: proxy.size(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataType
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::DataType> for odf::data::DataType {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::DataType, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::data::DataType::Binary(v) => (
                fb::DataType::DataTypeBinary,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::Bool(v) => {
                (fb::DataType::DataTypeBool, v.serialize(fb).as_union_value())
            }
            odf::data::DataType::Date(v) => {
                (fb::DataType::DataTypeDate, v.serialize(fb).as_union_value())
            }
            odf::data::DataType::Decimal(v) => (
                fb::DataType::DataTypeDecimal,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::Duration(v) => (
                fb::DataType::DataTypeDuration,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::Float16(v) => (
                fb::DataType::DataTypeFloat16,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::Float32(v) => (
                fb::DataType::DataTypeFloat32,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::Float64(v) => (
                fb::DataType::DataTypeFloat64,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::Int8(v) => {
                (fb::DataType::DataTypeInt8, v.serialize(fb).as_union_value())
            }
            odf::data::DataType::Int16(v) => (
                fb::DataType::DataTypeInt16,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::Int32(v) => (
                fb::DataType::DataTypeInt32,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::Int64(v) => (
                fb::DataType::DataTypeInt64,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::UInt8(v) => (
                fb::DataType::DataTypeUInt8,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::UInt16(v) => (
                fb::DataType::DataTypeUInt16,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::UInt32(v) => (
                fb::DataType::DataTypeUInt32,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::UInt64(v) => (
                fb::DataType::DataTypeUInt64,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::List(v) => {
                (fb::DataType::DataTypeList, v.serialize(fb).as_union_value())
            }
            odf::data::DataType::Map(v) => {
                (fb::DataType::DataTypeMap, v.serialize(fb).as_union_value())
            }
            odf::data::DataType::Null(v) => {
                (fb::DataType::DataTypeNull, v.serialize(fb).as_union_value())
            }
            odf::data::DataType::Option(v) => (
                fb::DataType::DataTypeOption,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::Struct(v) => (
                fb::DataType::DataTypeStruct,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::Time(v) => {
                (fb::DataType::DataTypeTime, v.serialize(fb).as_union_value())
            }
            odf::data::DataType::Timestamp(v) => (
                fb::DataType::DataTypeTimestamp,
                v.serialize(fb).as_union_value(),
            ),
            odf::data::DataType::String(v) => (
                fb::DataType::DataTypeString,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::DataType> for odf::data::DataType {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::DataType) -> Self {
        match t {
            fb::DataType::DataTypeBinary => {
                odf::data::DataType::Binary(odf::data::DataTypeBinary::deserialize(unsafe {
                    fb::DataTypeBinary::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeBool => {
                odf::data::DataType::Bool(odf::data::DataTypeBool::deserialize(unsafe {
                    fb::DataTypeBool::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeDate => {
                odf::data::DataType::Date(odf::data::DataTypeDate::deserialize(unsafe {
                    fb::DataTypeDate::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeDecimal => {
                odf::data::DataType::Decimal(odf::data::DataTypeDecimal::deserialize(unsafe {
                    fb::DataTypeDecimal::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeDuration => {
                odf::data::DataType::Duration(odf::data::DataTypeDuration::deserialize(unsafe {
                    fb::DataTypeDuration::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeFloat16 => {
                odf::data::DataType::Float16(odf::data::DataTypeFloat16::deserialize(unsafe {
                    fb::DataTypeFloat16::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeFloat32 => {
                odf::data::DataType::Float32(odf::data::DataTypeFloat32::deserialize(unsafe {
                    fb::DataTypeFloat32::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeFloat64 => {
                odf::data::DataType::Float64(odf::data::DataTypeFloat64::deserialize(unsafe {
                    fb::DataTypeFloat64::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeInt8 => {
                odf::data::DataType::Int8(odf::data::DataTypeInt8::deserialize(unsafe {
                    fb::DataTypeInt8::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeInt16 => {
                odf::data::DataType::Int16(odf::data::DataTypeInt16::deserialize(unsafe {
                    fb::DataTypeInt16::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeInt32 => {
                odf::data::DataType::Int32(odf::data::DataTypeInt32::deserialize(unsafe {
                    fb::DataTypeInt32::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeInt64 => {
                odf::data::DataType::Int64(odf::data::DataTypeInt64::deserialize(unsafe {
                    fb::DataTypeInt64::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeUInt8 => {
                odf::data::DataType::UInt8(odf::data::DataTypeUInt8::deserialize(unsafe {
                    fb::DataTypeUInt8::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeUInt16 => {
                odf::data::DataType::UInt16(odf::data::DataTypeUInt16::deserialize(unsafe {
                    fb::DataTypeUInt16::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeUInt32 => {
                odf::data::DataType::UInt32(odf::data::DataTypeUInt32::deserialize(unsafe {
                    fb::DataTypeUInt32::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeUInt64 => {
                odf::data::DataType::UInt64(odf::data::DataTypeUInt64::deserialize(unsafe {
                    fb::DataTypeUInt64::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeList => {
                odf::data::DataType::List(odf::data::DataTypeList::deserialize(unsafe {
                    fb::DataTypeList::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeMap => {
                odf::data::DataType::Map(odf::data::DataTypeMap::deserialize(unsafe {
                    fb::DataTypeMap::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeNull => {
                odf::data::DataType::Null(odf::data::DataTypeNull::deserialize(unsafe {
                    fb::DataTypeNull::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeOption => {
                odf::data::DataType::Option(odf::data::DataTypeOption::deserialize(unsafe {
                    fb::DataTypeOption::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeStruct => {
                odf::data::DataType::Struct(odf::data::DataTypeStruct::deserialize(unsafe {
                    fb::DataTypeStruct::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeTime => {
                odf::data::DataType::Time(odf::data::DataTypeTime::deserialize(unsafe {
                    fb::DataTypeTime::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeTimestamp => {
                odf::data::DataType::Timestamp(odf::data::DataTypeTimestamp::deserialize(unsafe {
                    fb::DataTypeTimestamp::init_from_table(table)
                }))
            }
            fb::DataType::DataTypeString => {
                odf::data::DataType::String(odf::data::DataTypeString::deserialize(unsafe {
                    fb::DataTypeString::init_from_table(table)
                }))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeBinary
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Binary
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeBinary {
    type OffsetT = WIPOffset<fb::DataTypeBinary<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeBinaryBuilder::new(fb);
        self.fixed_length.map(|v| builder.add_fixed_length(v));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeBinary<'fb>> for odf::data::DataTypeBinary {
    fn deserialize(proxy: fb::DataTypeBinary<'fb>) -> Self {
        odf::data::DataTypeBinary {
            fixed_length: proxy.fixed_length().map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeBool
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Bool
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeBool {
    type OffsetT = WIPOffset<fb::DataTypeBool<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeBoolBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeBool<'fb>> for odf::data::DataTypeBool {
    fn deserialize(proxy: fb::DataTypeBool<'fb>) -> Self {
        odf::data::DataTypeBool {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeDate
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Date
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeDate {
    type OffsetT = WIPOffset<fb::DataTypeDate<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeDateBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeDate<'fb>> for odf::data::DataTypeDate {
    fn deserialize(proxy: fb::DataTypeDate<'fb>) -> Self {
        odf::data::DataTypeDate {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeDecimal
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Decimal
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeDecimal {
    type OffsetT = WIPOffset<fb::DataTypeDecimal<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeDecimalBuilder::new(fb);
        builder.add_precision(self.precision);
        builder.add_scale(self.scale);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeDecimal<'fb>> for odf::data::DataTypeDecimal {
    fn deserialize(proxy: fb::DataTypeDecimal<'fb>) -> Self {
        odf::data::DataTypeDecimal {
            precision: proxy.precision(),
            scale: proxy.scale(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeDuration
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Duration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeDuration {
    type OffsetT = WIPOffset<fb::DataTypeDuration<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeDurationBuilder::new(fb);
        self.unit.map(|v| builder.add_unit(v.into()));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeDuration<'fb>> for odf::data::DataTypeDuration {
    fn deserialize(proxy: fb::DataTypeDuration<'fb>) -> Self {
        odf::data::DataTypeDuration {
            unit: proxy.unit().map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeFloat16
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Float16
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeFloat16 {
    type OffsetT = WIPOffset<fb::DataTypeFloat16<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeFloat16Builder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeFloat16<'fb>> for odf::data::DataTypeFloat16 {
    fn deserialize(proxy: fb::DataTypeFloat16<'fb>) -> Self {
        odf::data::DataTypeFloat16 {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeFloat32
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Float32
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeFloat32 {
    type OffsetT = WIPOffset<fb::DataTypeFloat32<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeFloat32Builder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeFloat32<'fb>> for odf::data::DataTypeFloat32 {
    fn deserialize(proxy: fb::DataTypeFloat32<'fb>) -> Self {
        odf::data::DataTypeFloat32 {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeFloat64
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Float64
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeFloat64 {
    type OffsetT = WIPOffset<fb::DataTypeFloat64<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeFloat64Builder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeFloat64<'fb>> for odf::data::DataTypeFloat64 {
    fn deserialize(proxy: fb::DataTypeFloat64<'fb>) -> Self {
        odf::data::DataTypeFloat64 {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeInt16
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int16
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeInt16 {
    type OffsetT = WIPOffset<fb::DataTypeInt16<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeInt16Builder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeInt16<'fb>> for odf::data::DataTypeInt16 {
    fn deserialize(proxy: fb::DataTypeInt16<'fb>) -> Self {
        odf::data::DataTypeInt16 {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeInt32
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int32
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeInt32 {
    type OffsetT = WIPOffset<fb::DataTypeInt32<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeInt32Builder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeInt32<'fb>> for odf::data::DataTypeInt32 {
    fn deserialize(proxy: fb::DataTypeInt32<'fb>) -> Self {
        odf::data::DataTypeInt32 {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeInt64
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int64
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeInt64 {
    type OffsetT = WIPOffset<fb::DataTypeInt64<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeInt64Builder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeInt64<'fb>> for odf::data::DataTypeInt64 {
    fn deserialize(proxy: fb::DataTypeInt64<'fb>) -> Self {
        odf::data::DataTypeInt64 {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeInt8
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Int8
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeInt8 {
    type OffsetT = WIPOffset<fb::DataTypeInt8<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeInt8Builder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeInt8<'fb>> for odf::data::DataTypeInt8 {
    fn deserialize(proxy: fb::DataTypeInt8<'fb>) -> Self {
        odf::data::DataTypeInt8 {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeList
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/List
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeList {
    type OffsetT = WIPOffset<fb::DataTypeList<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let item_type_offset = { self.item_type.serialize(fb) };
        let mut builder = fb::DataTypeListBuilder::new(fb);
        builder.add_item_type_type(item_type_offset.0);
        builder.add_item_type(item_type_offset.1);
        self.fixed_length.map(|v| builder.add_fixed_length(v));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeList<'fb>> for odf::data::DataTypeList {
    fn deserialize(proxy: fb::DataTypeList<'fb>) -> Self {
        odf::data::DataTypeList {
            item_type: proxy
                .item_type()
                .map(|v| Box::new(odf::data::DataType::deserialize(v, proxy.item_type_type())))
                .unwrap(),
            fixed_length: proxy.fixed_length().map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeMap
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Map
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeMap {
    type OffsetT = WIPOffset<fb::DataTypeMap<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let key_type_offset = { self.key_type.serialize(fb) };
        let value_type_offset = { self.value_type.serialize(fb) };
        let mut builder = fb::DataTypeMapBuilder::new(fb);
        builder.add_key_type_type(key_type_offset.0);
        builder.add_key_type(key_type_offset.1);
        builder.add_value_type_type(value_type_offset.0);
        builder.add_value_type(value_type_offset.1);
        self.keys_sorted.map(|v| builder.add_keys_sorted(v));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeMap<'fb>> for odf::data::DataTypeMap {
    fn deserialize(proxy: fb::DataTypeMap<'fb>) -> Self {
        odf::data::DataTypeMap {
            key_type: proxy
                .key_type()
                .map(|v| Box::new(odf::data::DataType::deserialize(v, proxy.key_type_type())))
                .unwrap(),
            value_type: proxy
                .value_type()
                .map(|v| Box::new(odf::data::DataType::deserialize(v, proxy.value_type_type())))
                .unwrap(),
            keys_sorted: proxy.keys_sorted().map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeNull
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Null
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeNull {
    type OffsetT = WIPOffset<fb::DataTypeNull<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeNullBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeNull<'fb>> for odf::data::DataTypeNull {
    fn deserialize(proxy: fb::DataTypeNull<'fb>) -> Self {
        odf::data::DataTypeNull {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeOption
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Option
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeOption {
    type OffsetT = WIPOffset<fb::DataTypeOption<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let inner_offset = { self.inner.serialize(fb) };
        let mut builder = fb::DataTypeOptionBuilder::new(fb);
        builder.add_inner_type(inner_offset.0);
        builder.add_inner(inner_offset.1);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeOption<'fb>> for odf::data::DataTypeOption {
    fn deserialize(proxy: fb::DataTypeOption<'fb>) -> Self {
        odf::data::DataTypeOption {
            inner: proxy
                .inner()
                .map(|v| Box::new(odf::data::DataType::deserialize(v, proxy.inner_type())))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeString
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/String
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeString {
    type OffsetT = WIPOffset<fb::DataTypeString<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeStringBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeString<'fb>> for odf::data::DataTypeString {
    fn deserialize(proxy: fb::DataTypeString<'fb>) -> Self {
        odf::data::DataTypeString {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeStruct
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Struct
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeStruct {
    type OffsetT = WIPOffset<fb::DataTypeStruct<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let fields_offset = {
            let offsets: Vec<_> = self.fields.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        };
        let mut builder = fb::DataTypeStructBuilder::new(fb);
        builder.add_fields(fields_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeStruct<'fb>> for odf::data::DataTypeStruct {
    fn deserialize(proxy: fb::DataTypeStruct<'fb>) -> Self {
        odf::data::DataTypeStruct {
            fields: proxy
                .fields()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::data::DataField::deserialize(i))
                        .collect()
                })
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeTime
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Time
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeTime {
    type OffsetT = WIPOffset<fb::DataTypeTime<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeTimeBuilder::new(fb);
        self.unit.map(|v| builder.add_unit(v.into()));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeTime<'fb>> for odf::data::DataTypeTime {
    fn deserialize(proxy: fb::DataTypeTime<'fb>) -> Self {
        odf::data::DataTypeTime {
            unit: proxy.unit().map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeTimestamp
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/Timestamp
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeTimestamp {
    type OffsetT = WIPOffset<fb::DataTypeTimestamp<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let timezone_offset = self.timezone.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::DataTypeTimestampBuilder::new(fb);
        self.unit.map(|v| builder.add_unit(v.into()));
        timezone_offset.map(|off| builder.add_timezone(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeTimestamp<'fb>> for odf::data::DataTypeTimestamp {
    fn deserialize(proxy: fb::DataTypeTimestamp<'fb>) -> Self {
        odf::data::DataTypeTimestamp {
            unit: proxy.unit().map(|v| v.into()),
            timezone: proxy.timezone().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeUInt16
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt16
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeUInt16 {
    type OffsetT = WIPOffset<fb::DataTypeUInt16<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeUInt16Builder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeUInt16<'fb>> for odf::data::DataTypeUInt16 {
    fn deserialize(proxy: fb::DataTypeUInt16<'fb>) -> Self {
        odf::data::DataTypeUInt16 {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeUInt32
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt32
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeUInt32 {
    type OffsetT = WIPOffset<fb::DataTypeUInt32<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeUInt32Builder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeUInt32<'fb>> for odf::data::DataTypeUInt32 {
    fn deserialize(proxy: fb::DataTypeUInt32<'fb>) -> Self {
        odf::data::DataTypeUInt32 {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeUInt64
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt64
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeUInt64 {
    type OffsetT = WIPOffset<fb::DataTypeUInt64<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeUInt64Builder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeUInt64<'fb>> for odf::data::DataTypeUInt64 {
    fn deserialize(proxy: fb::DataTypeUInt64<'fb>) -> Self {
        odf::data::DataTypeUInt64 {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeUInt8
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/DataType#/$defs/UInt8
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::DataTypeUInt8 {
    type OffsetT = WIPOffset<fb::DataTypeUInt8<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DataTypeUInt8Builder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DataTypeUInt8<'fb>> for odf::data::DataTypeUInt8 {
    fn deserialize(proxy: fb::DataTypeUInt8<'fb>) -> Self {
        odf::data::DataTypeUInt8 {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetKind
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetKind
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::dataset::DatasetKind> for fb::DatasetKind {
    fn from(v: odf::dataset::DatasetKind) -> Self {
        match v {
            odf::dataset::DatasetKind::Root => fb::DatasetKind::Root,
            odf::dataset::DatasetKind::Derivative => fb::DatasetKind::Derivative,
        }
    }
}

impl Into<odf::dataset::DatasetKind> for fb::DatasetKind {
    fn into(self) -> odf::dataset::DatasetKind {
        match self {
            fb::DatasetKind::Root => odf::dataset::DatasetKind::Root,
            fb::DatasetKind::Derivative => odf::dataset::DatasetKind::Derivative,
            _ => panic!("Invalid enum value: {}", self.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetSpec
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::DatasetSpec {
    type OffsetT = WIPOffset<fb::DatasetSpec<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let did_offset = self
            .did
            .as_ref()
            .map(|v| fb.create_vector(&v.as_bytes().as_slice()));
        let metadata_offset = {
            let offsets: Vec<_> = self
                .metadata
                .iter()
                .map(|i| {
                    let (value_type, value_offset) = i.serialize(fb);
                    let mut builder = fb::MetadataEventWrapperBuilder::new(fb);
                    builder.add_value_type(value_type);
                    builder.add_value(value_offset);
                    builder.finish()
                })
                .collect();
            fb.create_vector(&offsets)
        };
        let volume_offset = self.volume.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::DatasetSpecBuilder::new(fb);
        did_offset.map(|off| builder.add_did(off));
        builder.add_kind(self.kind.into());
        builder.add_metadata(metadata_offset);
        volume_offset.map(|off| builder.add_volume(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DatasetSpec<'fb>> for odf::dataset::DatasetSpec {
    fn deserialize(proxy: fb::DatasetSpec<'fb>) -> Self {
        odf::dataset::DatasetSpec {
            did: proxy
                .did()
                .map(|v| odf::dataset::DatasetID::from_bytes(v.bytes()).unwrap()),
            kind: proxy.kind().into(),
            metadata: proxy
                .metadata()
                .map(|v| {
                    v.iter()
                        .map(|i| {
                            odf::dataset::MetadataEvent::deserialize(
                                i.value().unwrap(),
                                i.value_type(),
                            )
                        })
                        .collect()
                })
                .unwrap(),
            volume: proxy
                .volume()
                .map(|v| odf::storage::PersistentVolumeRef::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetVocabulary
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::DatasetVocabulary {
    type OffsetT = WIPOffset<fb::DatasetVocabulary<'fb>>;

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
        let mut builder = fb::DatasetVocabularyBuilder::new(fb);
        offset_column_offset.map(|off| builder.add_offset_column(off));
        operation_type_column_offset.map(|off| builder.add_operation_type_column(off));
        system_time_column_offset.map(|off| builder.add_system_time_column(off));
        event_time_column_offset.map(|off| builder.add_event_time_column(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DatasetVocabulary<'fb>>
    for odf::dataset::DatasetVocabulary
{
    fn deserialize(proxy: fb::DatasetVocabulary<'fb>) -> Self {
        odf::dataset::DatasetVocabulary {
            offset_column: proxy.offset_column().map(|v| v.to_owned()),
            operation_type_column: proxy.operation_type_column().map(|v| v.to_owned()),
            system_time_column: proxy.system_time_column().map(|v| v.to_owned()),
            event_time_column: proxy.event_time_column().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DisablePollingSource
// Schema: https://opendatafabric.org/schemas/legacy/v0/DisablePollingSource
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::legacy::DisablePollingSource {
    type OffsetT = WIPOffset<fb::DisablePollingSource<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::DisablePollingSourceBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DisablePollingSource<'fb>>
    for odf::legacy::DisablePollingSource
{
    fn deserialize(proxy: fb::DisablePollingSource<'fb>) -> Self {
        odf::legacy::DisablePollingSource {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DisablePushSource
// Schema: https://opendatafabric.org/schemas/legacy/v0/DisablePushSource
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::legacy::DisablePushSource {
    type OffsetT = WIPOffset<fb::DisablePushSource<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let source_name_offset = { fb.create_string(&self.source_name) };
        let mut builder = fb::DisablePushSourceBuilder::new(fb);
        builder.add_source_name(source_name_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::DisablePushSource<'fb>> for odf::legacy::DisablePushSource {
    fn deserialize(proxy: fb::DisablePushSource<'fb>) -> Self {
        odf::legacy::DisablePushSource {
            source_name: proxy.source_name().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EnvVar
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EnvVar
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::EnvVar {
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

impl<'fb> FlatbuffersDeserializable<fb::EnvVar<'fb>> for odf::source::EnvVar {
    fn deserialize(proxy: fb::EnvVar<'fb>) -> Self {
        odf::source::EnvVar {
            name: proxy.name().map(|v| v.to_owned()).unwrap(),
            value: proxy.value().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventFilter
// Schema: https://opendatafabric.org/schemas/event/v1alpha1/EventFilter
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::event::EventFilter {
    type OffsetT = WIPOffset<fb::EventFilter<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let entries: Vec<_> = self
            .entries
            .iter()
            .map(|(key, value)| {
                let key_offset = fb.create_string(key.as_str());
                let value_offset = fb.create_string(&serde_json::to_string(value).unwrap());
                let mut entry_builder = fb::EventFilterEntryBuilder::new(fb);
                entry_builder.add_key(key_offset);
                entry_builder.add_value(value_offset);
                entry_builder.finish()
            })
            .collect();
        let entries_offset = fb.create_vector(&entries);
        let mut builder = fb::EventFilterBuilder::new(fb);
        builder.add_entries(entries_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::EventFilter<'fb>> for odf::event::EventFilter {
    fn deserialize(proxy: fb::EventFilter<'fb>) -> Self {
        Self {
            entries: proxy
                .entries()
                .unwrap_or_default()
                .iter()
                .map(|entry| {
                    let key = entry.key().parse().unwrap();
                    let value = serde_json::from_str(entry.value().unwrap()).unwrap();
                    (key, value)
                })
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSource
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::EventTimeSource> for odf::source::EventTimeSource {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::EventTimeSource, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::source::EventTimeSource::FromMetadata(v) => (
                fb::EventTimeSource::EventTimeSourceFromMetadata,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::EventTimeSource::FromPath(v) => (
                fb::EventTimeSource::EventTimeSourceFromPath,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::EventTimeSource::FromSystemTime(v) => (
                fb::EventTimeSource::EventTimeSourceFromSystemTime,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::EventTimeSource> for odf::source::EventTimeSource {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::EventTimeSource) -> Self {
        match t {
            fb::EventTimeSource::EventTimeSourceFromMetadata => {
                odf::source::EventTimeSource::FromMetadata(
                    odf::source::EventTimeSourceFromMetadata::deserialize(unsafe {
                        fb::EventTimeSourceFromMetadata::init_from_table(table)
                    }),
                )
            }
            fb::EventTimeSource::EventTimeSourceFromPath => odf::source::EventTimeSource::FromPath(
                odf::source::EventTimeSourceFromPath::deserialize(unsafe {
                    fb::EventTimeSourceFromPath::init_from_table(table)
                }),
            ),
            fb::EventTimeSource::EventTimeSourceFromSystemTime => {
                odf::source::EventTimeSource::FromSystemTime(
                    odf::source::EventTimeSourceFromSystemTime::deserialize(unsafe {
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
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource#/$defs/FromMetadata
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::EventTimeSourceFromMetadata {
    type OffsetT = WIPOffset<fb::EventTimeSourceFromMetadata<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::EventTimeSourceFromMetadataBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::EventTimeSourceFromMetadata<'fb>>
    for odf::source::EventTimeSourceFromMetadata
{
    fn deserialize(proxy: fb::EventTimeSourceFromMetadata<'fb>) -> Self {
        odf::source::EventTimeSourceFromMetadata {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSourceFromPath
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource#/$defs/FromPath
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::EventTimeSourceFromPath {
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
    for odf::source::EventTimeSourceFromPath
{
    fn deserialize(proxy: fb::EventTimeSourceFromPath<'fb>) -> Self {
        odf::source::EventTimeSourceFromPath {
            pattern: proxy.pattern().map(|v| v.to_owned()).unwrap(),
            timestamp_format: proxy.timestamp_format().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSourceFromSystemTime
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource#/$defs/FromSystemTime
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::EventTimeSourceFromSystemTime {
    type OffsetT = WIPOffset<fb::EventTimeSourceFromSystemTime<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::EventTimeSourceFromSystemTimeBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::EventTimeSourceFromSystemTime<'fb>>
    for odf::source::EventTimeSourceFromSystemTime
{
    fn deserialize(proxy: fb::EventTimeSourceFromSystemTime<'fb>) -> Self {
        odf::source::EventTimeSourceFromSystemTime {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransform
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/ExecuteTransform
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::ExecuteTransform {
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

impl<'fb> FlatbuffersDeserializable<fb::ExecuteTransform<'fb>> for odf::dataset::ExecuteTransform {
    fn deserialize(proxy: fb::ExecuteTransform<'fb>) -> Self {
        odf::dataset::ExecuteTransform {
            query_inputs: proxy
                .query_inputs()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::dataset::ExecuteTransformInput::deserialize(i))
                        .collect()
                })
                .unwrap(),
            prev_checkpoint: proxy
                .prev_checkpoint()
                .map(|v| odf::Multihash::from_bytes(v.bytes()).unwrap()),
            prev_offset: proxy.prev_offset().map(|v| v),
            new_data: proxy
                .new_data()
                .map(|v| odf::dataset::DataSlice::deserialize(v)),
            new_checkpoint: proxy
                .new_checkpoint()
                .map(|v| odf::dataset::Checkpoint::deserialize(v)),
            new_watermark: proxy.new_watermark().map(|v| fb_to_datetime(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransformInput
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/ExecuteTransformInput
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::ExecuteTransformInput {
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

impl<'fb> FlatbuffersDeserializable<fb::ExecuteTransformInput<'fb>>
    for odf::dataset::ExecuteTransformInput
{
    fn deserialize(proxy: fb::ExecuteTransformInput<'fb>) -> Self {
        odf::dataset::ExecuteTransformInput {
            dataset_id: proxy
                .dataset_id()
                .map(|v| odf::dataset::DatasetID::from_bytes(v.bytes()).unwrap())
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
// ExtraAttributes
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/ExtraAttributes
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::data::ExtraAttributes {
    type OffsetT = WIPOffset<fb::ExtraAttributes<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let entries_offset = fb.create_string(&serde_json::to_string(&self.entries).unwrap());
        let mut builder = fb::ExtraAttributesBuilder::new(fb);
        builder.add_entries(entries_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ExtraAttributes<'fb>> for odf::data::ExtraAttributes {
    fn deserialize(proxy: fb::ExtraAttributes<'fb>) -> Self {
        let entries = serde_json::from_str(proxy.entries().unwrap()).unwrap();
        Self { entries }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStep
// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::FetchStep> for odf::legacy::FetchStep {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::FetchStep, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::legacy::FetchStep::Url(v) => (
                fb::FetchStep::FetchStepUrl,
                v.serialize(fb).as_union_value(),
            ),
            odf::legacy::FetchStep::FilesGlob(v) => (
                fb::FetchStep::FetchStepFilesGlob,
                v.serialize(fb).as_union_value(),
            ),
            odf::legacy::FetchStep::Container(v) => (
                fb::FetchStep::FetchStepContainer,
                v.serialize(fb).as_union_value(),
            ),
            odf::legacy::FetchStep::Mqtt(v) => (
                fb::FetchStep::FetchStepMqtt,
                v.serialize(fb).as_union_value(),
            ),
            odf::legacy::FetchStep::EthereumLogs(v) => (
                fb::FetchStep::FetchStepEthereumLogs,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::FetchStep> for odf::legacy::FetchStep {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::FetchStep) -> Self {
        match t {
            fb::FetchStep::FetchStepUrl => {
                odf::legacy::FetchStep::Url(odf::legacy::FetchStepUrl::deserialize(unsafe {
                    fb::FetchStepUrl::init_from_table(table)
                }))
            }
            fb::FetchStep::FetchStepFilesGlob => {
                odf::legacy::FetchStep::FilesGlob(odf::legacy::FetchStepFilesGlob::deserialize(
                    unsafe { fb::FetchStepFilesGlob::init_from_table(table) },
                ))
            }
            fb::FetchStep::FetchStepContainer => {
                odf::legacy::FetchStep::Container(odf::legacy::FetchStepContainer::deserialize(
                    unsafe { fb::FetchStepContainer::init_from_table(table) },
                ))
            }
            fb::FetchStep::FetchStepMqtt => {
                odf::legacy::FetchStep::Mqtt(odf::legacy::FetchStepMqtt::deserialize(unsafe {
                    fb::FetchStepMqtt::init_from_table(table)
                }))
            }
            fb::FetchStep::FetchStepEthereumLogs => odf::legacy::FetchStep::EthereumLogs(
                odf::legacy::FetchStepEthereumLogs::deserialize(unsafe {
                    fb::FetchStepEthereumLogs::init_from_table(table)
                }),
            ),
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepContainer
// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Container
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::legacy::FetchStepContainer {
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

impl<'fb> FlatbuffersDeserializable<fb::FetchStepContainer<'fb>>
    for odf::legacy::FetchStepContainer
{
    fn deserialize(proxy: fb::FetchStepContainer<'fb>) -> Self {
        odf::legacy::FetchStepContainer {
            image: proxy.image().map(|v| v.to_owned()).unwrap(),
            command: proxy
                .command()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            args: proxy
                .args()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            env: proxy.env().map(|v| {
                v.iter()
                    .map(|i| odf::source::EnvVar::deserialize(i))
                    .collect()
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepEthereumLogs
// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/EthereumLogs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::legacy::FetchStepEthereumLogs {
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

impl<'fb> FlatbuffersDeserializable<fb::FetchStepEthereumLogs<'fb>>
    for odf::legacy::FetchStepEthereumLogs
{
    fn deserialize(proxy: fb::FetchStepEthereumLogs<'fb>) -> Self {
        odf::legacy::FetchStepEthereumLogs {
            chain_id: proxy.chain_id().map(|v| v),
            node_url: proxy.node_url().map(|v| v.to_owned()),
            filter: proxy.filter().map(|v| v.to_owned()),
            signature: proxy.signature().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepFilesGlob
// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/FilesGlob
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::legacy::FetchStepFilesGlob {
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

impl<'fb> FlatbuffersDeserializable<fb::FetchStepFilesGlob<'fb>>
    for odf::legacy::FetchStepFilesGlob
{
    fn deserialize(proxy: fb::FetchStepFilesGlob<'fb>) -> Self {
        odf::legacy::FetchStepFilesGlob {
            path: proxy.path().map(|v| v.to_owned()).unwrap(),
            event_time: proxy
                .event_time()
                .map(|v| odf::source::EventTimeSource::deserialize(v, proxy.event_time_type())),
            cache: proxy
                .cache()
                .map(|v| odf::source::SourceCaching::deserialize(v, proxy.cache_type())),
            order: proxy.order().map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepMqtt
// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Mqtt
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::legacy::FetchStepMqtt {
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

impl<'fb> FlatbuffersDeserializable<fb::FetchStepMqtt<'fb>> for odf::legacy::FetchStepMqtt {
    fn deserialize(proxy: fb::FetchStepMqtt<'fb>) -> Self {
        odf::legacy::FetchStepMqtt {
            host: proxy.host().map(|v| v.to_owned()).unwrap(),
            port: proxy.port(),
            username: proxy.username().map(|v| v.to_owned()),
            password: proxy.password().map(|v| v.to_owned()),
            topics: proxy
                .topics()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::source::MqttTopicSubscription::deserialize(i))
                        .collect()
                })
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepUrl
// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Url
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::legacy::FetchStepUrl {
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

impl<'fb> FlatbuffersDeserializable<fb::FetchStepUrl<'fb>> for odf::legacy::FetchStepUrl {
    fn deserialize(proxy: fb::FetchStepUrl<'fb>) -> Self {
        odf::legacy::FetchStepUrl {
            url: proxy.url().map(|v| v.to_owned()).unwrap(),
            event_time: proxy
                .event_time()
                .map(|v| odf::source::EventTimeSource::deserialize(v, proxy.event_time_type())),
            cache: proxy
                .cache()
                .map(|v| odf::source::SourceCaching::deserialize(v, proxy.cache_type())),
            headers: proxy.headers().map(|v| {
                v.iter()
                    .map(|i| odf::source::RequestHeader::deserialize(i))
                    .collect()
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FlowSpec
// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::flow::FlowSpec {
    type OffsetT = WIPOffset<fb::FlowSpec<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let target_offset = { self.target.serialize(fb) };
        let triggers_offset = {
            let offsets: Vec<_> = self
                .triggers
                .iter()
                .map(|i| {
                    let (value_type, value_offset) = i.serialize(fb);
                    let mut builder = fb::FlowTriggerWrapperBuilder::new(fb);
                    builder.add_value_type(value_type);
                    builder.add_value(value_offset);
                    builder.finish()
                })
                .collect();
            fb.create_vector(&offsets)
        };
        let tasks_offset = {
            let offsets: Vec<_> = self
                .tasks
                .iter()
                .map(|i| {
                    let (value_type, value_offset) = i.serialize(fb);
                    let mut builder = fb::TaskSpecWrapperBuilder::new(fb);
                    builder.add_value_type(value_type);
                    builder.add_value(value_offset);
                    builder.finish()
                })
                .collect();
            fb.create_vector(&offsets)
        };
        let mut builder = fb::FlowSpecBuilder::new(fb);
        builder.add_target(target_offset);
        builder.add_triggers(triggers_offset);
        builder.add_tasks(tasks_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FlowSpec<'fb>> for odf::flow::FlowSpec {
    fn deserialize(proxy: fb::FlowSpec<'fb>) -> Self {
        odf::flow::FlowSpec {
            target: proxy
                .target()
                .map(|v| odf::resource::ResourceSelector::deserialize(v))
                .unwrap(),
            triggers: proxy
                .triggers()
                .map(|v| {
                    v.iter()
                        .map(|i| {
                            odf::flow::FlowTrigger::deserialize(i.value().unwrap(), i.value_type())
                        })
                        .collect()
                })
                .unwrap(),
            tasks: proxy
                .tasks()
                .map(|v| {
                    v.iter()
                        .map(|i| {
                            odf::flow::TaskSpec::deserialize(i.value().unwrap(), i.value_type())
                        })
                        .collect()
                })
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FlowTrigger
// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::FlowTrigger> for odf::flow::FlowTrigger {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::FlowTrigger, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::flow::FlowTrigger::Schedule(v) => (
                fb::FlowTrigger::FlowTriggerSchedule,
                v.serialize(fb).as_union_value(),
            ),
            odf::flow::FlowTrigger::Event(v) => (
                fb::FlowTrigger::FlowTriggerEvent,
                v.serialize(fb).as_union_value(),
            ),
            odf::flow::FlowTrigger::Source(v) => (
                fb::FlowTrigger::FlowTriggerSource,
                v.serialize(fb).as_union_value(),
            ),
            odf::flow::FlowTrigger::Dataset(v) => (
                fb::FlowTrigger::FlowTriggerDataset,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::FlowTrigger> for odf::flow::FlowTrigger {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::FlowTrigger) -> Self {
        match t {
            fb::FlowTrigger::FlowTriggerSchedule => {
                odf::flow::FlowTrigger::Schedule(odf::flow::FlowTriggerSchedule::deserialize(
                    unsafe { fb::FlowTriggerSchedule::init_from_table(table) },
                ))
            }
            fb::FlowTrigger::FlowTriggerEvent => {
                odf::flow::FlowTrigger::Event(odf::flow::FlowTriggerEvent::deserialize(unsafe {
                    fb::FlowTriggerEvent::init_from_table(table)
                }))
            }
            fb::FlowTrigger::FlowTriggerSource => {
                odf::flow::FlowTrigger::Source(odf::flow::FlowTriggerSource::deserialize(unsafe {
                    fb::FlowTriggerSource::init_from_table(table)
                }))
            }
            fb::FlowTrigger::FlowTriggerDataset => {
                odf::flow::FlowTrigger::Dataset(odf::flow::FlowTriggerDataset::deserialize(
                    unsafe { fb::FlowTriggerDataset::init_from_table(table) },
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FlowTriggerDataset
// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Dataset
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::flow::FlowTriggerDataset {
    type OffsetT = WIPOffset<fb::FlowTriggerDataset<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let dataset_offset = { self.dataset.serialize(fb) };
        let events_offset = self.events.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let mut builder = fb::FlowTriggerDatasetBuilder::new(fb);
        builder.add_dataset(dataset_offset);
        events_offset.map(|off| builder.add_events(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FlowTriggerDataset<'fb>> for odf::flow::FlowTriggerDataset {
    fn deserialize(proxy: fb::FlowTriggerDataset<'fb>) -> Self {
        odf::flow::FlowTriggerDataset {
            dataset: proxy
                .dataset()
                .map(|v| odf::dataset::DatasetSelector::deserialize(v))
                .unwrap(),
            events: proxy
                .events()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FlowTriggerEvent
// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Event
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::flow::FlowTriggerEvent {
    type OffsetT = WIPOffset<fb::FlowTriggerEvent<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let events_offset = { self.events.serialize(fb) };
        let mut builder = fb::FlowTriggerEventBuilder::new(fb);
        builder.add_events(events_offset);
        self.cooldown
            .map(|v| builder.add_cooldown(&duration_to_fb(&v)));
        self.cooldown_max_batch
            .map(|v| builder.add_cooldown_max_batch(v));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FlowTriggerEvent<'fb>> for odf::flow::FlowTriggerEvent {
    fn deserialize(proxy: fb::FlowTriggerEvent<'fb>) -> Self {
        odf::flow::FlowTriggerEvent {
            events: proxy
                .events()
                .map(|v| odf::event::EventFilter::deserialize(v))
                .unwrap(),
            cooldown: proxy.cooldown().map(|v| fb_to_duration(v)),
            cooldown_max_batch: proxy.cooldown_max_batch().map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FlowTriggerSchedule
// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Schedule
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::flow::FlowTriggerSchedule {
    type OffsetT = WIPOffset<fb::FlowTriggerSchedule<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let cron_offset = { fb.create_string(&self.cron) };
        let mut builder = fb::FlowTriggerScheduleBuilder::new(fb);
        builder.add_cron(cron_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FlowTriggerSchedule<'fb>>
    for odf::flow::FlowTriggerSchedule
{
    fn deserialize(proxy: fb::FlowTriggerSchedule<'fb>) -> Self {
        odf::flow::FlowTriggerSchedule {
            cron: proxy.cron().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FlowTriggerSource
// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Source
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::flow::FlowTriggerSource {
    type OffsetT = WIPOffset<fb::FlowTriggerSource<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let source_offset = { self.source.serialize(fb) };
        let mut builder = fb::FlowTriggerSourceBuilder::new(fb);
        builder.add_source(source_offset);
        self.min_records_to_await
            .map(|v| builder.add_min_records_to_await(v));
        self.max_await_interval
            .map(|v| builder.add_max_await_interval(&duration_to_fb(&v)));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::FlowTriggerSource<'fb>> for odf::flow::FlowTriggerSource {
    fn deserialize(proxy: fb::FlowTriggerSource<'fb>) -> Self {
        odf::flow::FlowTriggerSource {
            source: proxy
                .source()
                .map(|v| odf::resource::ResourceRef::deserialize(v))
                .unwrap(),
            min_records_to_await: proxy.min_records_to_await().map(|v| v),
            max_await_interval: proxy.max_await_interval().map(|v| fb_to_duration(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IngestParams
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/IngestParams
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::IngestParams {
    type OffsetT = WIPOffset<fb::IngestParams<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::IngestParamsBuilder::new(fb);
        self.target_slice_records
            .map(|v| builder.add_target_slice_records(v));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::IngestParams<'fb>> for odf::source::IngestParams {
    fn deserialize(proxy: fb::IngestParams<'fb>) -> Self {
        odf::source::IngestParams {
            target_slice_records: proxy.target_slice_records().map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Ingress
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::Ingress> for odf::source::Ingress {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::Ingress, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::source::Ingress::Url(v) => {
                (fb::Ingress::IngressUrl, v.serialize(fb).as_union_value())
            }
            odf::source::Ingress::FilesGlob(v) => (
                fb::Ingress::IngressFilesGlob,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::Ingress::Container(v) => (
                fb::Ingress::IngressContainer,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::Ingress::Mqtt(v) => {
                (fb::Ingress::IngressMqtt, v.serialize(fb).as_union_value())
            }
            odf::source::Ingress::EvmLogs(v) => (
                fb::Ingress::IngressEvmLogs,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::Ingress::RestEndpoint(v) => (
                fb::Ingress::IngressRestEndpoint,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::Ingress> for odf::source::Ingress {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::Ingress) -> Self {
        match t {
            fb::Ingress::IngressUrl => {
                odf::source::Ingress::Url(odf::source::IngressUrl::deserialize(unsafe {
                    fb::IngressUrl::init_from_table(table)
                }))
            }
            fb::Ingress::IngressFilesGlob => {
                odf::source::Ingress::FilesGlob(odf::source::IngressFilesGlob::deserialize(
                    unsafe { fb::IngressFilesGlob::init_from_table(table) },
                ))
            }
            fb::Ingress::IngressContainer => {
                odf::source::Ingress::Container(odf::source::IngressContainer::deserialize(
                    unsafe { fb::IngressContainer::init_from_table(table) },
                ))
            }
            fb::Ingress::IngressMqtt => {
                odf::source::Ingress::Mqtt(odf::source::IngressMqtt::deserialize(unsafe {
                    fb::IngressMqtt::init_from_table(table)
                }))
            }
            fb::Ingress::IngressEvmLogs => {
                odf::source::Ingress::EvmLogs(odf::source::IngressEvmLogs::deserialize(unsafe {
                    fb::IngressEvmLogs::init_from_table(table)
                }))
            }
            fb::Ingress::IngressRestEndpoint => {
                odf::source::Ingress::RestEndpoint(odf::source::IngressRestEndpoint::deserialize(
                    unsafe { fb::IngressRestEndpoint::init_from_table(table) },
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IngressContainer
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/Container
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::IngressContainer {
    type OffsetT = WIPOffset<fb::IngressContainer<'fb>>;

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
        let mut builder = fb::IngressContainerBuilder::new(fb);
        builder.add_image(image_offset);
        command_offset.map(|off| builder.add_command(off));
        args_offset.map(|off| builder.add_args(off));
        env_offset.map(|off| builder.add_env(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::IngressContainer<'fb>> for odf::source::IngressContainer {
    fn deserialize(proxy: fb::IngressContainer<'fb>) -> Self {
        odf::source::IngressContainer {
            image: proxy.image().map(|v| v.to_owned()).unwrap(),
            command: proxy
                .command()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            args: proxy
                .args()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            env: proxy.env().map(|v| {
                v.iter()
                    .map(|i| odf::source::EnvVar::deserialize(i))
                    .collect()
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IngressEvmLogs
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/EvmLogs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::IngressEvmLogs {
    type OffsetT = WIPOffset<fb::IngressEvmLogs<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let node_url_offset = self.node_url.as_ref().map(|v| fb.create_string(&v));
        let filter_offset = self.filter.as_ref().map(|v| fb.create_string(&v));
        let signature_offset = self.signature.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::IngressEvmLogsBuilder::new(fb);
        self.chain_id.map(|v| builder.add_chain_id(v));
        node_url_offset.map(|off| builder.add_node_url(off));
        filter_offset.map(|off| builder.add_filter(off));
        signature_offset.map(|off| builder.add_signature(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::IngressEvmLogs<'fb>> for odf::source::IngressEvmLogs {
    fn deserialize(proxy: fb::IngressEvmLogs<'fb>) -> Self {
        odf::source::IngressEvmLogs {
            chain_id: proxy.chain_id().map(|v| v),
            node_url: proxy.node_url().map(|v| v.to_owned()),
            filter: proxy.filter().map(|v| v.to_owned()),
            signature: proxy.signature().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IngressFilesGlob
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/FilesGlob
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::IngressFilesGlob {
    type OffsetT = WIPOffset<fb::IngressFilesGlob<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let path_offset = { fb.create_string(&self.path) };
        let event_time_offset = self.event_time.as_ref().map(|v| v.serialize(fb));
        let cache_offset = self.cache.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::IngressFilesGlobBuilder::new(fb);
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

impl<'fb> FlatbuffersDeserializable<fb::IngressFilesGlob<'fb>> for odf::source::IngressFilesGlob {
    fn deserialize(proxy: fb::IngressFilesGlob<'fb>) -> Self {
        odf::source::IngressFilesGlob {
            path: proxy.path().map(|v| v.to_owned()).unwrap(),
            event_time: proxy
                .event_time()
                .map(|v| odf::source::EventTimeSource::deserialize(v, proxy.event_time_type())),
            cache: proxy
                .cache()
                .map(|v| odf::source::SourceCaching::deserialize(v, proxy.cache_type())),
            order: proxy.order().map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IngressMqtt
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/Mqtt
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::IngressMqtt {
    type OffsetT = WIPOffset<fb::IngressMqtt<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let host_offset = { fb.create_string(&self.host) };
        let username_offset = self.username.as_ref().map(|v| fb.create_string(&v));
        let password_offset = self.password.as_ref().map(|v| fb.create_string(&v));
        let topics_offset = {
            let offsets: Vec<_> = self.topics.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        };
        let mut builder = fb::IngressMqttBuilder::new(fb);
        builder.add_host(host_offset);
        builder.add_port(self.port);
        username_offset.map(|off| builder.add_username(off));
        password_offset.map(|off| builder.add_password(off));
        builder.add_topics(topics_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::IngressMqtt<'fb>> for odf::source::IngressMqtt {
    fn deserialize(proxy: fb::IngressMqtt<'fb>) -> Self {
        odf::source::IngressMqtt {
            host: proxy.host().map(|v| v.to_owned()).unwrap(),
            port: proxy.port(),
            username: proxy.username().map(|v| v.to_owned()),
            password: proxy.password().map(|v| v.to_owned()),
            topics: proxy
                .topics()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::source::MqttTopicSubscription::deserialize(i))
                        .collect()
                })
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IngressRestEndpoint
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/RestEndpoint
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::IngressRestEndpoint {
    type OffsetT = WIPOffset<fb::IngressRestEndpoint<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let buffer_offset = self.buffer.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::IngressRestEndpointBuilder::new(fb);
        buffer_offset.map(|(e, off)| {
            builder.add_buffer_type(e);
            builder.add_buffer(off)
        });
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::IngressRestEndpoint<'fb>>
    for odf::source::IngressRestEndpoint
{
    fn deserialize(proxy: fb::IngressRestEndpoint<'fb>) -> Self {
        odf::source::IngressRestEndpoint {
            buffer: proxy
                .buffer()
                .map(|v| odf::source::IngressBuffer::deserialize(v, proxy.buffer_type())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IngressUrl
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/Url
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::IngressUrl {
    type OffsetT = WIPOffset<fb::IngressUrl<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let url_offset = { fb.create_string(&self.url) };
        let event_time_offset = self.event_time.as_ref().map(|v| v.serialize(fb));
        let cache_offset = self.cache.as_ref().map(|v| v.serialize(fb));
        let headers_offset = self.headers.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        });
        let mut builder = fb::IngressUrlBuilder::new(fb);
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

impl<'fb> FlatbuffersDeserializable<fb::IngressUrl<'fb>> for odf::source::IngressUrl {
    fn deserialize(proxy: fb::IngressUrl<'fb>) -> Self {
        odf::source::IngressUrl {
            url: proxy.url().map(|v| v.to_owned()).unwrap(),
            event_time: proxy
                .event_time()
                .map(|v| odf::source::EventTimeSource::deserialize(v, proxy.event_time_type())),
            cache: proxy
                .cache()
                .map(|v| odf::source::SourceCaching::deserialize(v, proxy.cache_type())),
            headers: proxy.headers().map(|v| {
                v.iter()
                    .map(|i| odf::source::RequestHeader::deserialize(i))
                    .collect()
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IngressBuffer
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/IngressBuffer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::IngressBuffer> for odf::source::IngressBuffer {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::IngressBuffer, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::source::IngressBuffer::Memory(v) => (
                fb::IngressBuffer::IngressBufferMemory,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::IngressBuffer> for odf::source::IngressBuffer {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::IngressBuffer) -> Self {
        match t {
            fb::IngressBuffer::IngressBufferMemory => {
                odf::source::IngressBuffer::Memory(odf::source::IngressBufferMemory::deserialize(
                    unsafe { fb::IngressBufferMemory::init_from_table(table) },
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IngressBufferMemory
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/IngressBuffer#/$defs/Memory
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::IngressBufferMemory {
    type OffsetT = WIPOffset<fb::IngressBufferMemory<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let overflow_policy_offset = self.overflow_policy.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::IngressBufferMemoryBuilder::new(fb);
        self.buffer_size.map(|v| builder.add_buffer_size(v));
        overflow_policy_offset.map(|off| builder.add_overflow_policy(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::IngressBufferMemory<'fb>>
    for odf::source::IngressBufferMemory
{
    fn deserialize(proxy: fb::IngressBufferMemory<'fb>) -> Self {
        odf::source::IngressBufferMemory {
            buffer_size: proxy.buffer_size().map(|v| v),
            overflow_policy: proxy.overflow_policy().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// LabelFilter
// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/LabelFilter
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::resource::LabelFilter {
    type OffsetT = WIPOffset<fb::LabelFilter<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let entries: Vec<_> = self
            .entries
            .iter()
            .map(|(key, value)| {
                let key_offset = fb.create_string(key.as_str());
                let value_offset = fb.create_string(&serde_json::to_string(value).unwrap());
                let mut entry_builder = fb::LabelFilterEntryBuilder::new(fb);
                entry_builder.add_key(key_offset);
                entry_builder.add_value(value_offset);
                entry_builder.finish()
            })
            .collect();
        let entries_offset = fb.create_vector(&entries);
        let mut builder = fb::LabelFilterBuilder::new(fb);
        builder.add_entries(entries_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::LabelFilter<'fb>> for odf::resource::LabelFilter {
    fn deserialize(proxy: fb::LabelFilter<'fb>) -> Self {
        Self {
            entries: proxy
                .entries()
                .unwrap_or_default()
                .iter()
                .map(|entry| {
                    let key = entry.key().parse().unwrap();
                    let value = serde_json::from_str(entry.value().unwrap()).unwrap();
                    (key, value)
                })
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::MergeStrategy> for odf::source::MergeStrategy {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::MergeStrategy, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::source::MergeStrategy::Append(v) => (
                fb::MergeStrategy::MergeStrategyAppend,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::MergeStrategy::Ledger(v) => (
                fb::MergeStrategy::MergeStrategyLedger,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::MergeStrategy::Snapshot(v) => (
                fb::MergeStrategy::MergeStrategySnapshot,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::MergeStrategy::ChangelogStream(v) => (
                fb::MergeStrategy::MergeStrategyChangelogStream,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::MergeStrategy::UpsertStream(v) => (
                fb::MergeStrategy::MergeStrategyUpsertStream,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::MergeStrategy> for odf::source::MergeStrategy {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::MergeStrategy) -> Self {
        match t {
            fb::MergeStrategy::MergeStrategyAppend => {
                odf::source::MergeStrategy::Append(odf::source::MergeStrategyAppend::deserialize(
                    unsafe { fb::MergeStrategyAppend::init_from_table(table) },
                ))
            }
            fb::MergeStrategy::MergeStrategyLedger => {
                odf::source::MergeStrategy::Ledger(odf::source::MergeStrategyLedger::deserialize(
                    unsafe { fb::MergeStrategyLedger::init_from_table(table) },
                ))
            }
            fb::MergeStrategy::MergeStrategySnapshot => odf::source::MergeStrategy::Snapshot(
                odf::source::MergeStrategySnapshot::deserialize(unsafe {
                    fb::MergeStrategySnapshot::init_from_table(table)
                }),
            ),
            fb::MergeStrategy::MergeStrategyChangelogStream => {
                odf::source::MergeStrategy::ChangelogStream(
                    odf::source::MergeStrategyChangelogStream::deserialize(unsafe {
                        fb::MergeStrategyChangelogStream::init_from_table(table)
                    }),
                )
            }
            fb::MergeStrategy::MergeStrategyUpsertStream => {
                odf::source::MergeStrategy::UpsertStream(
                    odf::source::MergeStrategyUpsertStream::deserialize(unsafe {
                        fb::MergeStrategyUpsertStream::init_from_table(table)
                    }),
                )
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategyAppend
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/Append
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::MergeStrategyAppend {
    type OffsetT = WIPOffset<fb::MergeStrategyAppend<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::MergeStrategyAppendBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::MergeStrategyAppend<'fb>>
    for odf::source::MergeStrategyAppend
{
    fn deserialize(proxy: fb::MergeStrategyAppend<'fb>) -> Self {
        odf::source::MergeStrategyAppend {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategyChangelogStream
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/ChangelogStream
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::MergeStrategyChangelogStream {
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
    for odf::source::MergeStrategyChangelogStream
{
    fn deserialize(proxy: fb::MergeStrategyChangelogStream<'fb>) -> Self {
        odf::source::MergeStrategyChangelogStream {
            primary_key: proxy
                .primary_key()
                .map(|v| v.iter().map(|i| i.to_owned()).collect())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategyLedger
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/Ledger
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::MergeStrategyLedger {
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

impl<'fb> FlatbuffersDeserializable<fb::MergeStrategyLedger<'fb>>
    for odf::source::MergeStrategyLedger
{
    fn deserialize(proxy: fb::MergeStrategyLedger<'fb>) -> Self {
        odf::source::MergeStrategyLedger {
            primary_key: proxy
                .primary_key()
                .map(|v| v.iter().map(|i| i.to_owned()).collect())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategySnapshot
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/Snapshot
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::MergeStrategySnapshot {
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

impl<'fb> FlatbuffersDeserializable<fb::MergeStrategySnapshot<'fb>>
    for odf::source::MergeStrategySnapshot
{
    fn deserialize(proxy: fb::MergeStrategySnapshot<'fb>) -> Self {
        odf::source::MergeStrategySnapshot {
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
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/UpsertStream
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::MergeStrategyUpsertStream {
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
    for odf::source::MergeStrategyUpsertStream
{
    fn deserialize(proxy: fb::MergeStrategyUpsertStream<'fb>) -> Self {
        odf::source::MergeStrategyUpsertStream {
            primary_key: proxy
                .primary_key()
                .map(|v| v.iter().map(|i| i.to_owned()).collect())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/MetadataBlock
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::MetadataBlock {
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

impl<'fb> FlatbuffersDeserializable<fb::MetadataBlock<'fb>> for odf::dataset::MetadataBlock {
    fn deserialize(proxy: fb::MetadataBlock<'fb>) -> Self {
        odf::dataset::MetadataBlock {
            system_time: proxy.system_time().map(|v| fb_to_datetime(v)).unwrap(),
            prev_block_hash: proxy
                .prev_block_hash()
                .map(|v| odf::Multihash::from_bytes(v.bytes()).unwrap()),
            sequence_number: proxy.sequence_number(),
            event: proxy
                .event()
                .map(|v| odf::dataset::MetadataEvent::deserialize(v, proxy.event_type()))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/MetadataEvent
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::MetadataEvent> for odf::dataset::MetadataEvent {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::MetadataEvent, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::dataset::MetadataEvent::AddData(v) => {
                (fb::MetadataEvent::AddData, v.serialize(fb).as_union_value())
            }
            odf::dataset::MetadataEvent::ExecuteTransform(v) => (
                fb::MetadataEvent::ExecuteTransform,
                v.serialize(fb).as_union_value(),
            ),
            odf::dataset::MetadataEvent::Seed(v) => {
                (fb::MetadataEvent::Seed, v.serialize(fb).as_union_value())
            }
            odf::dataset::MetadataEvent::SetPollingSource(v) => (
                fb::MetadataEvent::SetPollingSource,
                v.serialize(fb).as_union_value(),
            ),
            odf::dataset::MetadataEvent::SetTransform(v) => (
                fb::MetadataEvent::SetTransform,
                v.serialize(fb).as_union_value(),
            ),
            odf::dataset::MetadataEvent::SetVocab(v) => (
                fb::MetadataEvent::SetVocab,
                v.serialize(fb).as_union_value(),
            ),
            odf::dataset::MetadataEvent::SetAttachments(v) => (
                fb::MetadataEvent::SetAttachments,
                v.serialize(fb).as_union_value(),
            ),
            odf::dataset::MetadataEvent::SetInfo(v) => {
                (fb::MetadataEvent::SetInfo, v.serialize(fb).as_union_value())
            }
            odf::dataset::MetadataEvent::SetLicense(v) => (
                fb::MetadataEvent::SetLicense,
                v.serialize(fb).as_union_value(),
            ),
            odf::dataset::MetadataEvent::SetDataSchema(v) => (
                fb::MetadataEvent::SetDataSchema,
                v.serialize(fb).as_union_value(),
            ),
            odf::dataset::MetadataEvent::AddPushSource(v) => (
                fb::MetadataEvent::AddPushSource,
                v.serialize(fb).as_union_value(),
            ),
            odf::dataset::MetadataEvent::DisablePushSource(v) => (
                fb::MetadataEvent::DisablePushSource,
                v.serialize(fb).as_union_value(),
            ),
            odf::dataset::MetadataEvent::DisablePollingSource(v) => (
                fb::MetadataEvent::DisablePollingSource,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::MetadataEvent> for odf::dataset::MetadataEvent {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::MetadataEvent) -> Self {
        match t {
            fb::MetadataEvent::AddData => {
                odf::dataset::MetadataEvent::AddData(odf::dataset::AddData::deserialize(unsafe {
                    fb::AddData::init_from_table(table)
                }))
            }
            fb::MetadataEvent::ExecuteTransform => odf::dataset::MetadataEvent::ExecuteTransform(
                odf::dataset::ExecuteTransform::deserialize(unsafe {
                    fb::ExecuteTransform::init_from_table(table)
                }),
            ),
            fb::MetadataEvent::Seed => {
                odf::dataset::MetadataEvent::Seed(odf::dataset::Seed::deserialize(unsafe {
                    fb::Seed::init_from_table(table)
                }))
            }
            fb::MetadataEvent::SetPollingSource => odf::dataset::MetadataEvent::SetPollingSource(
                odf::legacy::SetPollingSource::deserialize(unsafe {
                    fb::SetPollingSource::init_from_table(table)
                }),
            ),
            fb::MetadataEvent::SetTransform => {
                odf::dataset::MetadataEvent::SetTransform(odf::dataset::SetTransform::deserialize(
                    unsafe { fb::SetTransform::init_from_table(table) },
                ))
            }
            fb::MetadataEvent::SetVocab => {
                odf::dataset::MetadataEvent::SetVocab(odf::dataset::SetVocab::deserialize(unsafe {
                    fb::SetVocab::init_from_table(table)
                }))
            }
            fb::MetadataEvent::SetAttachments => odf::dataset::MetadataEvent::SetAttachments(
                odf::dataset::SetAttachments::deserialize(unsafe {
                    fb::SetAttachments::init_from_table(table)
                }),
            ),
            fb::MetadataEvent::SetInfo => {
                odf::dataset::MetadataEvent::SetInfo(odf::dataset::SetInfo::deserialize(unsafe {
                    fb::SetInfo::init_from_table(table)
                }))
            }
            fb::MetadataEvent::SetLicense => {
                odf::dataset::MetadataEvent::SetLicense(odf::dataset::SetLicense::deserialize(
                    unsafe { fb::SetLicense::init_from_table(table) },
                ))
            }
            fb::MetadataEvent::SetDataSchema => odf::dataset::MetadataEvent::SetDataSchema(
                odf::dataset::SetDataSchema::deserialize(unsafe {
                    fb::SetDataSchema::init_from_table(table)
                }),
            ),
            fb::MetadataEvent::AddPushSource => {
                odf::dataset::MetadataEvent::AddPushSource(odf::legacy::AddPushSource::deserialize(
                    unsafe { fb::AddPushSource::init_from_table(table) },
                ))
            }
            fb::MetadataEvent::DisablePushSource => odf::dataset::MetadataEvent::DisablePushSource(
                odf::legacy::DisablePushSource::deserialize(unsafe {
                    fb::DisablePushSource::init_from_table(table)
                }),
            ),
            fb::MetadataEvent::DisablePollingSource => {
                odf::dataset::MetadataEvent::DisablePollingSource(
                    odf::legacy::DisablePollingSource::deserialize(unsafe {
                        fb::DisablePollingSource::init_from_table(table)
                    }),
                )
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MqttQos
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MqttQos
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::source::MqttQos> for fb::MqttQos {
    fn from(v: odf::source::MqttQos) -> Self {
        match v {
            odf::source::MqttQos::AtMostOnce => fb::MqttQos::AtMostOnce,
            odf::source::MqttQos::AtLeastOnce => fb::MqttQos::AtLeastOnce,
            odf::source::MqttQos::ExactlyOnce => fb::MqttQos::ExactlyOnce,
        }
    }
}

impl Into<odf::source::MqttQos> for fb::MqttQos {
    fn into(self) -> odf::source::MqttQos {
        match self {
            fb::MqttQos::AtMostOnce => odf::source::MqttQos::AtMostOnce,
            fb::MqttQos::AtLeastOnce => odf::source::MqttQos::AtLeastOnce,
            fb::MqttQos::ExactlyOnce => odf::source::MqttQos::ExactlyOnce,
            _ => panic!("Invalid enum value: {}", self.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MqttTopicSubscription
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MqttTopicSubscription
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::MqttTopicSubscription {
    type OffsetT = WIPOffset<fb::MqttTopicSubscription<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let path_offset = { fb.create_string(&self.path) };
        let mut builder = fb::MqttTopicSubscriptionBuilder::new(fb);
        builder.add_path(path_offset);
        self.qos.map(|v| builder.add_qos(v.into()));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::MqttTopicSubscription<'fb>>
    for odf::source::MqttTopicSubscription
{
    fn deserialize(proxy: fb::MqttTopicSubscription<'fb>) -> Self {
        odf::source::MqttTopicSubscription {
            path: proxy.path().map(|v| v.to_owned()).unwrap(),
            qos: proxy.qos().map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/OffsetInterval
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::OffsetInterval {
    type OffsetT = WIPOffset<fb::OffsetInterval<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::OffsetIntervalBuilder::new(fb);
        builder.add_start(self.start);
        builder.add_end(self.end);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::OffsetInterval<'fb>> for odf::dataset::OffsetInterval {
    fn deserialize(proxy: fb::OffsetInterval<'fb>) -> Self {
        odf::dataset::OffsetInterval {
            start: proxy.start(),
            end: proxy.end(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PersistentVolumeSpec
// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::PersistentVolumeSpec>
    for odf::storage::PersistentVolumeSpec
{
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::PersistentVolumeSpec, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::storage::PersistentVolumeSpec::S3(v) => (
                fb::PersistentVolumeSpec::PersistentVolumeSpecS3,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::PersistentVolumeSpec>
    for odf::storage::PersistentVolumeSpec
{
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::PersistentVolumeSpec) -> Self {
        match t {
            fb::PersistentVolumeSpec::PersistentVolumeSpecS3 => {
                odf::storage::PersistentVolumeSpec::S3(
                    odf::storage::PersistentVolumeSpecS3::deserialize(unsafe {
                        fb::PersistentVolumeSpecS3::init_from_table(table)
                    }),
                )
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PersistentVolumeSpecS3
// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpec#/$defs/S3
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::storage::PersistentVolumeSpecS3 {
    type OffsetT = WIPOffset<fb::PersistentVolumeSpecS3<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let endpoint_offset = self.endpoint.as_ref().map(|v| fb.create_string(&v));
        let region_offset = self.region.as_ref().map(|v| fb.create_string(&v));
        let bucket_offset = { fb.create_string(&self.bucket) };
        let prefix_offset = self.prefix.as_ref().map(|v| fb.create_string(&v));
        let capacity_offset = self.capacity.as_ref().map(|v| v.serialize(fb));
        let credentials_offset = self.credentials.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::PersistentVolumeSpecS3Builder::new(fb);
        endpoint_offset.map(|off| builder.add_endpoint(off));
        region_offset.map(|off| builder.add_region(off));
        builder.add_bucket(bucket_offset);
        prefix_offset.map(|off| builder.add_prefix(off));
        capacity_offset.map(|off| builder.add_capacity(off));
        credentials_offset.map(|off| builder.add_credentials(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::PersistentVolumeSpecS3<'fb>>
    for odf::storage::PersistentVolumeSpecS3
{
    fn deserialize(proxy: fb::PersistentVolumeSpecS3<'fb>) -> Self {
        odf::storage::PersistentVolumeSpecS3 {
            endpoint: proxy.endpoint().map(|v| v.to_owned()),
            region: proxy.region().map(|v| v.to_owned()),
            bucket: proxy.bucket().map(|v| v.to_owned()).unwrap(),
            prefix: proxy.prefix().map(|v| v.to_owned()),
            capacity: proxy
                .capacity()
                .map(|v| odf::storage::VolumeCapacity::deserialize(v)),
            credentials: proxy
                .credentials()
                .map(|v| odf::storage::AwsCredentials::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PrepStep
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/PrepStep
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::PrepStep> for odf::source::PrepStep {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::PrepStep, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::source::PrepStep::Decompress(v) => (
                fb::PrepStep::PrepStepDecompress,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::PrepStep::Pipe(v) => {
                (fb::PrepStep::PrepStepPipe, v.serialize(fb).as_union_value())
            }
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::PrepStep> for odf::source::PrepStep {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::PrepStep) -> Self {
        match t {
            fb::PrepStep::PrepStepDecompress => {
                odf::source::PrepStep::Decompress(odf::source::PrepStepDecompress::deserialize(
                    unsafe { fb::PrepStepDecompress::init_from_table(table) },
                ))
            }
            fb::PrepStep::PrepStepPipe => {
                odf::source::PrepStep::Pipe(odf::source::PrepStepPipe::deserialize(unsafe {
                    fb::PrepStepPipe::init_from_table(table)
                }))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PrepStepDecompress
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/PrepStep#/$defs/Decompress
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::PrepStepDecompress {
    type OffsetT = WIPOffset<fb::PrepStepDecompress<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let sub_path_offset = self.sub_path.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::PrepStepDecompressBuilder::new(fb);
        builder.add_format(self.format.into());
        sub_path_offset.map(|off| builder.add_sub_path(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::PrepStepDecompress<'fb>>
    for odf::source::PrepStepDecompress
{
    fn deserialize(proxy: fb::PrepStepDecompress<'fb>) -> Self {
        odf::source::PrepStepDecompress {
            format: proxy.format().into(),
            sub_path: proxy.sub_path().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PrepStepPipe
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/PrepStep#/$defs/Pipe
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::PrepStepPipe {
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

impl<'fb> FlatbuffersDeserializable<fb::PrepStepPipe<'fb>> for odf::source::PrepStepPipe {
    fn deserialize(proxy: fb::PrepStepPipe<'fb>) -> Self {
        odf::source::PrepStepPipe {
            command: proxy
                .command()
                .map(|v| v.iter().map(|i| i.to_owned()).collect())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ProjectionSpec
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/ProjectionSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::ProjectionSpec {
    type OffsetT = WIPOffset<fb::ProjectionSpec<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let inputs_offset = {
            let offsets: Vec<_> = self.inputs.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        };
        let project_offset = { self.project.serialize(fb) };
        let mut builder = fb::ProjectionSpecBuilder::new(fb);
        builder.add_inputs(inputs_offset);
        builder.add_project_type(project_offset.0);
        builder.add_project(project_offset.1);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ProjectionSpec<'fb>> for odf::dataset::ProjectionSpec {
    fn deserialize(proxy: fb::ProjectionSpec<'fb>) -> Self {
        odf::dataset::ProjectionSpec {
            inputs: proxy
                .inputs()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::dataset::TransformInput::deserialize(i))
                        .collect()
                })
                .unwrap(),
            project: proxy
                .project()
                .map(|v| odf::dataset::Transform::deserialize(v, proxy.project_type()))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryRequest
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryRequest
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::engine::RawQueryRequest {
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

impl<'fb> FlatbuffersDeserializable<fb::RawQueryRequest<'fb>> for odf::engine::RawQueryRequest {
    fn deserialize(proxy: fb::RawQueryRequest<'fb>) -> Self {
        odf::engine::RawQueryRequest {
            input_data_paths: proxy
                .input_data_paths()
                .map(|v| v.iter().map(|i| PathBuf::from(i)).collect())
                .unwrap(),
            transform: proxy
                .transform()
                .map(|v| odf::dataset::Transform::deserialize(v, proxy.transform_type()))
                .unwrap(),
            output_data_path: proxy.output_data_path().map(|v| PathBuf::from(v)).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponse
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::RawQueryResponse> for odf::engine::RawQueryResponse {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::RawQueryResponse, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::engine::RawQueryResponse::Progress(v) => (
                fb::RawQueryResponse::RawQueryResponseProgress,
                v.serialize(fb).as_union_value(),
            ),
            odf::engine::RawQueryResponse::Success(v) => (
                fb::RawQueryResponse::RawQueryResponseSuccess,
                v.serialize(fb).as_union_value(),
            ),
            odf::engine::RawQueryResponse::InvalidQuery(v) => (
                fb::RawQueryResponse::RawQueryResponseInvalidQuery,
                v.serialize(fb).as_union_value(),
            ),
            odf::engine::RawQueryResponse::InternalError(v) => (
                fb::RawQueryResponse::RawQueryResponseInternalError,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::RawQueryResponse>
    for odf::engine::RawQueryResponse
{
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::RawQueryResponse) -> Self {
        match t {
            fb::RawQueryResponse::RawQueryResponseProgress => {
                odf::engine::RawQueryResponse::Progress(
                    odf::engine::RawQueryResponseProgress::deserialize(unsafe {
                        fb::RawQueryResponseProgress::init_from_table(table)
                    }),
                )
            }
            fb::RawQueryResponse::RawQueryResponseSuccess => {
                odf::engine::RawQueryResponse::Success(
                    odf::engine::RawQueryResponseSuccess::deserialize(unsafe {
                        fb::RawQueryResponseSuccess::init_from_table(table)
                    }),
                )
            }
            fb::RawQueryResponse::RawQueryResponseInvalidQuery => {
                odf::engine::RawQueryResponse::InvalidQuery(
                    odf::engine::RawQueryResponseInvalidQuery::deserialize(unsafe {
                        fb::RawQueryResponseInvalidQuery::init_from_table(table)
                    }),
                )
            }
            fb::RawQueryResponse::RawQueryResponseInternalError => {
                odf::engine::RawQueryResponse::InternalError(
                    odf::engine::RawQueryResponseInternalError::deserialize(unsafe {
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
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/InternalError
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::engine::RawQueryResponseInternalError {
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
    for odf::engine::RawQueryResponseInternalError
{
    fn deserialize(proxy: fb::RawQueryResponseInternalError<'fb>) -> Self {
        odf::engine::RawQueryResponseInternalError {
            message: proxy.message().map(|v| v.to_owned()).unwrap(),
            backtrace: proxy.backtrace().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponseInvalidQuery
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/InvalidQuery
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::engine::RawQueryResponseInvalidQuery {
    type OffsetT = WIPOffset<fb::RawQueryResponseInvalidQuery<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let message_offset = { fb.create_string(&self.message) };
        let mut builder = fb::RawQueryResponseInvalidQueryBuilder::new(fb);
        builder.add_message(message_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::RawQueryResponseInvalidQuery<'fb>>
    for odf::engine::RawQueryResponseInvalidQuery
{
    fn deserialize(proxy: fb::RawQueryResponseInvalidQuery<'fb>) -> Self {
        odf::engine::RawQueryResponseInvalidQuery {
            message: proxy.message().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponseProgress
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/Progress
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::engine::RawQueryResponseProgress {
    type OffsetT = WIPOffset<fb::RawQueryResponseProgress<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::RawQueryResponseProgressBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::RawQueryResponseProgress<'fb>>
    for odf::engine::RawQueryResponseProgress
{
    fn deserialize(proxy: fb::RawQueryResponseProgress<'fb>) -> Self {
        odf::engine::RawQueryResponseProgress {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponseSuccess
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/Success
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::engine::RawQueryResponseSuccess {
    type OffsetT = WIPOffset<fb::RawQueryResponseSuccess<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::RawQueryResponseSuccessBuilder::new(fb);
        builder.add_num_records(self.num_records);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::RawQueryResponseSuccess<'fb>>
    for odf::engine::RawQueryResponseSuccess
{
    fn deserialize(proxy: fb::RawQueryResponseSuccess<'fb>) -> Self {
        odf::engine::RawQueryResponseSuccess {
            num_records: proxy.num_records(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStep
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::ReadStep> for odf::source::ReadStep {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::ReadStep, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::source::ReadStep::Csv(v) => {
                (fb::ReadStep::ReadStepCsv, v.serialize(fb).as_union_value())
            }
            odf::source::ReadStep::GeoJson(v) => (
                fb::ReadStep::ReadStepGeoJson,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::ReadStep::EsriShapefile(v) => (
                fb::ReadStep::ReadStepEsriShapefile,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::ReadStep::Parquet(v) => (
                fb::ReadStep::ReadStepParquet,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::ReadStep::Json(v) => {
                (fb::ReadStep::ReadStepJson, v.serialize(fb).as_union_value())
            }
            odf::source::ReadStep::NdJson(v) => (
                fb::ReadStep::ReadStepNdJson,
                v.serialize(fb).as_union_value(),
            ),
            odf::source::ReadStep::NdGeoJson(v) => (
                fb::ReadStep::ReadStepNdGeoJson,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::ReadStep> for odf::source::ReadStep {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::ReadStep) -> Self {
        match t {
            fb::ReadStep::ReadStepCsv => {
                odf::source::ReadStep::Csv(odf::source::ReadStepCsv::deserialize(unsafe {
                    fb::ReadStepCsv::init_from_table(table)
                }))
            }
            fb::ReadStep::ReadStepGeoJson => {
                odf::source::ReadStep::GeoJson(odf::source::ReadStepGeoJson::deserialize(unsafe {
                    fb::ReadStepGeoJson::init_from_table(table)
                }))
            }
            fb::ReadStep::ReadStepEsriShapefile => odf::source::ReadStep::EsriShapefile(
                odf::source::ReadStepEsriShapefile::deserialize(unsafe {
                    fb::ReadStepEsriShapefile::init_from_table(table)
                }),
            ),
            fb::ReadStep::ReadStepParquet => {
                odf::source::ReadStep::Parquet(odf::source::ReadStepParquet::deserialize(unsafe {
                    fb::ReadStepParquet::init_from_table(table)
                }))
            }
            fb::ReadStep::ReadStepJson => {
                odf::source::ReadStep::Json(odf::source::ReadStepJson::deserialize(unsafe {
                    fb::ReadStepJson::init_from_table(table)
                }))
            }
            fb::ReadStep::ReadStepNdJson => {
                odf::source::ReadStep::NdJson(odf::source::ReadStepNdJson::deserialize(unsafe {
                    fb::ReadStepNdJson::init_from_table(table)
                }))
            }
            fb::ReadStep::ReadStepNdGeoJson => {
                odf::source::ReadStep::NdGeoJson(odf::source::ReadStepNdGeoJson::deserialize(
                    unsafe { fb::ReadStepNdGeoJson::init_from_table(table) },
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepCsv
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/Csv
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::ReadStepCsv {
    type OffsetT = WIPOffset<fb::ReadStepCsv<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let ddl_schema_offset = self.ddl_schema.as_ref().map(|v| {
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
        let schema_offset = self.schema.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::ReadStepCsvBuilder::new(fb);
        ddl_schema_offset.map(|off| builder.add_ddl_schema(off));
        separator_offset.map(|off| builder.add_separator(off));
        encoding_offset.map(|off| builder.add_encoding(off));
        quote_offset.map(|off| builder.add_quote(off));
        escape_offset.map(|off| builder.add_escape(off));
        self.header.map(|v| builder.add_header(v));
        self.infer_schema.map(|v| builder.add_infer_schema(v));
        null_value_offset.map(|off| builder.add_null_value(off));
        date_format_offset.map(|off| builder.add_date_format(off));
        timestamp_format_offset.map(|off| builder.add_timestamp_format(off));
        schema_offset.map(|off| builder.add_schema(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepCsv<'fb>> for odf::source::ReadStepCsv {
    fn deserialize(proxy: fb::ReadStepCsv<'fb>) -> Self {
        odf::source::ReadStepCsv {
            ddl_schema: proxy
                .ddl_schema()
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
            schema: proxy
                .schema()
                .map(|v| odf::data::DataSchema::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepEsriShapefile
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/EsriShapefile
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::ReadStepEsriShapefile {
    type OffsetT = WIPOffset<fb::ReadStepEsriShapefile<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let ddl_schema_offset = self.ddl_schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let sub_path_offset = self.sub_path.as_ref().map(|v| fb.create_string(&v));
        let schema_offset = self.schema.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::ReadStepEsriShapefileBuilder::new(fb);
        ddl_schema_offset.map(|off| builder.add_ddl_schema(off));
        sub_path_offset.map(|off| builder.add_sub_path(off));
        schema_offset.map(|off| builder.add_schema(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepEsriShapefile<'fb>>
    for odf::source::ReadStepEsriShapefile
{
    fn deserialize(proxy: fb::ReadStepEsriShapefile<'fb>) -> Self {
        odf::source::ReadStepEsriShapefile {
            ddl_schema: proxy
                .ddl_schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            sub_path: proxy.sub_path().map(|v| v.to_owned()),
            schema: proxy
                .schema()
                .map(|v| odf::data::DataSchema::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepGeoJson
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/GeoJson
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::ReadStepGeoJson {
    type OffsetT = WIPOffset<fb::ReadStepGeoJson<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let ddl_schema_offset = self.ddl_schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let schema_offset = self.schema.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::ReadStepGeoJsonBuilder::new(fb);
        ddl_schema_offset.map(|off| builder.add_ddl_schema(off));
        schema_offset.map(|off| builder.add_schema(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepGeoJson<'fb>> for odf::source::ReadStepGeoJson {
    fn deserialize(proxy: fb::ReadStepGeoJson<'fb>) -> Self {
        odf::source::ReadStepGeoJson {
            ddl_schema: proxy
                .ddl_schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            schema: proxy
                .schema()
                .map(|v| odf::data::DataSchema::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepJson
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/Json
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::ReadStepJson {
    type OffsetT = WIPOffset<fb::ReadStepJson<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let sub_path_offset = self.sub_path.as_ref().map(|v| fb.create_string(&v));
        let ddl_schema_offset = self.ddl_schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let date_format_offset = self.date_format.as_ref().map(|v| fb.create_string(&v));
        let encoding_offset = self.encoding.as_ref().map(|v| fb.create_string(&v));
        let timestamp_format_offset = self.timestamp_format.as_ref().map(|v| fb.create_string(&v));
        let schema_offset = self.schema.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::ReadStepJsonBuilder::new(fb);
        sub_path_offset.map(|off| builder.add_sub_path(off));
        ddl_schema_offset.map(|off| builder.add_ddl_schema(off));
        date_format_offset.map(|off| builder.add_date_format(off));
        encoding_offset.map(|off| builder.add_encoding(off));
        timestamp_format_offset.map(|off| builder.add_timestamp_format(off));
        schema_offset.map(|off| builder.add_schema(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepJson<'fb>> for odf::source::ReadStepJson {
    fn deserialize(proxy: fb::ReadStepJson<'fb>) -> Self {
        odf::source::ReadStepJson {
            sub_path: proxy.sub_path().map(|v| v.to_owned()),
            ddl_schema: proxy
                .ddl_schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            date_format: proxy.date_format().map(|v| v.to_owned()),
            encoding: proxy.encoding().map(|v| v.to_owned()),
            timestamp_format: proxy.timestamp_format().map(|v| v.to_owned()),
            schema: proxy
                .schema()
                .map(|v| odf::data::DataSchema::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepNdGeoJson
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/NdGeoJson
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::ReadStepNdGeoJson {
    type OffsetT = WIPOffset<fb::ReadStepNdGeoJson<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let ddl_schema_offset = self.ddl_schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let schema_offset = self.schema.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::ReadStepNdGeoJsonBuilder::new(fb);
        ddl_schema_offset.map(|off| builder.add_ddl_schema(off));
        schema_offset.map(|off| builder.add_schema(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepNdGeoJson<'fb>> for odf::source::ReadStepNdGeoJson {
    fn deserialize(proxy: fb::ReadStepNdGeoJson<'fb>) -> Self {
        odf::source::ReadStepNdGeoJson {
            ddl_schema: proxy
                .ddl_schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            schema: proxy
                .schema()
                .map(|v| odf::data::DataSchema::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepNdJson
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/NdJson
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::ReadStepNdJson {
    type OffsetT = WIPOffset<fb::ReadStepNdJson<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let ddl_schema_offset = self.ddl_schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let date_format_offset = self.date_format.as_ref().map(|v| fb.create_string(&v));
        let encoding_offset = self.encoding.as_ref().map(|v| fb.create_string(&v));
        let timestamp_format_offset = self.timestamp_format.as_ref().map(|v| fb.create_string(&v));
        let schema_offset = self.schema.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::ReadStepNdJsonBuilder::new(fb);
        ddl_schema_offset.map(|off| builder.add_ddl_schema(off));
        date_format_offset.map(|off| builder.add_date_format(off));
        encoding_offset.map(|off| builder.add_encoding(off));
        timestamp_format_offset.map(|off| builder.add_timestamp_format(off));
        schema_offset.map(|off| builder.add_schema(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepNdJson<'fb>> for odf::source::ReadStepNdJson {
    fn deserialize(proxy: fb::ReadStepNdJson<'fb>) -> Self {
        odf::source::ReadStepNdJson {
            ddl_schema: proxy
                .ddl_schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            date_format: proxy.date_format().map(|v| v.to_owned()),
            encoding: proxy.encoding().map(|v| v.to_owned()),
            timestamp_format: proxy.timestamp_format().map(|v| v.to_owned()),
            schema: proxy
                .schema()
                .map(|v| odf::data::DataSchema::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepParquet
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/Parquet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::ReadStepParquet {
    type OffsetT = WIPOffset<fb::ReadStepParquet<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let ddl_schema_offset = self.ddl_schema.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| fb.create_string(&i)).collect();
            fb.create_vector(&offsets)
        });
        let schema_offset = self.schema.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::ReadStepParquetBuilder::new(fb);
        ddl_schema_offset.map(|off| builder.add_ddl_schema(off));
        schema_offset.map(|off| builder.add_schema(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ReadStepParquet<'fb>> for odf::source::ReadStepParquet {
    fn deserialize(proxy: fb::ReadStepParquet<'fb>) -> Self {
        odf::source::ReadStepParquet {
            ddl_schema: proxy
                .ddl_schema()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
            schema: proxy
                .schema()
                .map(|v| odf::data::DataSchema::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Relation
// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Relation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::auth::Relation {
    type OffsetT = WIPOffset<fb::Relation<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let subject_offset = { self.subject.serialize(fb) };
        let relation_offset = { fb.create_string(&self.relation) };
        let value_offset = self
            .value
            .as_ref()
            .map(|v| fb.create_string(&serde_json::to_string(&v).unwrap()));
        let object_offset = { self.object.serialize(fb) };
        let mut builder = fb::RelationBuilder::new(fb);
        builder.add_subject(subject_offset);
        builder.add_relation(relation_offset);
        value_offset.map(|off| builder.add_value(off));
        builder.add_object(object_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Relation<'fb>> for odf::auth::Relation {
    fn deserialize(proxy: fb::Relation<'fb>) -> Self {
        odf::auth::Relation {
            subject: proxy
                .subject()
                .map(|v| odf::resource::ResourceRef::deserialize(v))
                .unwrap(),
            relation: proxy.relation().map(|v| v.to_owned()).unwrap(),
            value: proxy.value().map(|v| serde_json::from_str(v).unwrap()),
            object: proxy
                .object()
                .map(|v| odf::resource::ResourceRef::deserialize(v))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RelationsSpec
// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/RelationsSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::auth::RelationsSpec {
    type OffsetT = WIPOffset<fb::RelationsSpec<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let relations_offset = self.relations.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        });
        let attributes_offset = self.attributes.as_ref().map(|v| {
            let offsets: Vec<_> = v.iter().map(|i| i.serialize(fb)).collect();
            fb.create_vector(&offsets)
        });
        let mut builder = fb::RelationsSpecBuilder::new(fb);
        relations_offset.map(|off| builder.add_relations(off));
        attributes_offset.map(|off| builder.add_attributes(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::RelationsSpec<'fb>> for odf::auth::RelationsSpec {
    fn deserialize(proxy: fb::RelationsSpec<'fb>) -> Self {
        odf::auth::RelationsSpec {
            relations: proxy.relations().map(|v| {
                v.iter()
                    .map(|i| odf::auth::Relation::deserialize(i))
                    .collect()
            }),
            attributes: proxy.attributes().map(|v| {
                v.iter()
                    .map(|i| odf::auth::Attribute::deserialize(i))
                    .collect()
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RequestHeader
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/RequestHeader
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::RequestHeader {
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

impl<'fb> FlatbuffersDeserializable<fb::RequestHeader<'fb>> for odf::source::RequestHeader {
    fn deserialize(proxy: fb::RequestHeader<'fb>) -> Self {
        odf::source::RequestHeader {
            name: proxy.name().map(|v| v.to_owned()).unwrap(),
            value: proxy.value().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceAnnotations
// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceAnnotations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::resource::ResourceAnnotations {
    type OffsetT = WIPOffset<fb::ResourceAnnotations<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let entries: Vec<_> = self
            .entries
            .iter()
            .map(|(key, value)| {
                let key_offset = fb.create_string(key.as_str());
                let value_offset = fb.create_string(&serde_json::to_string(value).unwrap());
                let mut entry_builder = fb::ResourceAnnotationsEntryBuilder::new(fb);
                entry_builder.add_key(key_offset);
                entry_builder.add_value(value_offset);
                entry_builder.finish()
            })
            .collect();
        let entries_offset = fb.create_vector(&entries);
        let mut builder = fb::ResourceAnnotationsBuilder::new(fb);
        builder.add_entries(entries_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ResourceAnnotations<'fb>>
    for odf::resource::ResourceAnnotations
{
    fn deserialize(proxy: fb::ResourceAnnotations<'fb>) -> Self {
        Self {
            entries: proxy
                .entries()
                .unwrap_or_default()
                .iter()
                .map(|entry| {
                    let key = entry.key().parse().unwrap();
                    let value = serde_json::from_str(entry.value().unwrap()).unwrap();
                    (key, value)
                })
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceConditions
// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceConditions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::resource::ResourceConditions {
    type OffsetT = WIPOffset<fb::ResourceConditions<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let entries: Vec<_> = self
            .entries
            .iter()
            .map(|(key, value)| {
                let key_offset = fb.create_string(key.as_str());
                let value_offset = fb.create_string(&serde_json::to_string(value).unwrap());
                let mut entry_builder = fb::ResourceConditionsEntryBuilder::new(fb);
                entry_builder.add_key(key_offset);
                entry_builder.add_value(value_offset);
                entry_builder.finish()
            })
            .collect();
        let entries_offset = fb.create_vector(&entries);
        let mut builder = fb::ResourceConditionsBuilder::new(fb);
        builder.add_entries(entries_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ResourceConditions<'fb>>
    for odf::resource::ResourceConditions
{
    fn deserialize(proxy: fb::ResourceConditions<'fb>) -> Self {
        Self {
            entries: proxy
                .entries()
                .unwrap_or_default()
                .iter()
                .map(|entry| {
                    let key = entry.key().parse().unwrap();
                    let value = serde_json::from_str(entry.value().unwrap()).unwrap();
                    (key, value)
                })
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceHeaders
// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceHeaders
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::resource::ResourceHeaders {
    type OffsetT = WIPOffset<fb::ResourceHeaders<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let id_offset = self.id.as_ref().map(|v| fb.create_vector(&v.as_bytes()));
        let name_offset = { fb.create_string(&self.name.to_string()) };
        let account_offset = self.account.as_ref().map(|v| v.serialize(fb));
        let labels_offset = self.labels.as_ref().map(|v| v.serialize(fb));
        let annotations_offset = self.annotations.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::ResourceHeadersBuilder::new(fb);
        id_offset.map(|off| builder.add_id(off));
        builder.add_name(name_offset);
        account_offset.map(|off| builder.add_account(off));
        labels_offset.map(|off| builder.add_labels(off));
        annotations_offset.map(|off| builder.add_annotations(off));
        self.generation.map(|v| builder.add_generation(v));
        self.created_at
            .map(|v| builder.add_created_at(&datetime_to_fb(&v)));
        self.updated_at
            .map(|v| builder.add_updated_at(&datetime_to_fb(&v)));
        self.deleted_at
            .map(|v| builder.add_deleted_at(&datetime_to_fb(&v)));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ResourceHeaders<'fb>> for odf::resource::ResourceHeaders {
    fn deserialize(proxy: fb::ResourceHeaders<'fb>) -> Self {
        odf::resource::ResourceHeaders {
            id: proxy
                .id()
                .map(|v| odf::resource::ResourceID::from_bytes(v.bytes()).unwrap()),
            name: proxy
                .name()
                .map(|v| odf::resource::ResourceName::try_from(v).unwrap())
                .unwrap(),
            account: proxy
                .account()
                .map(|v| odf::auth::AccountRef::deserialize(v)),
            labels: proxy
                .labels()
                .map(|v| odf::resource::ResourceLabels::deserialize(v)),
            annotations: proxy
                .annotations()
                .map(|v| odf::resource::ResourceAnnotations::deserialize(v)),
            generation: proxy.generation().map(|v| v),
            created_at: proxy.created_at().map(|v| fb_to_datetime(v)),
            updated_at: proxy.updated_at().map(|v| fb_to_datetime(v)),
            deleted_at: proxy.deleted_at().map(|v| fb_to_datetime(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceLabels
// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceLabels
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::resource::ResourceLabels {
    type OffsetT = WIPOffset<fb::ResourceLabels<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let entries: Vec<_> = self
            .entries
            .iter()
            .map(|(key, value)| {
                let key_offset = fb.create_string(key.as_str());
                let value_offset = fb.create_string(&serde_json::to_string(value).unwrap());
                let mut entry_builder = fb::ResourceLabelsEntryBuilder::new(fb);
                entry_builder.add_key(key_offset);
                entry_builder.add_value(value_offset);
                entry_builder.finish()
            })
            .collect();
        let entries_offset = fb.create_vector(&entries);
        let mut builder = fb::ResourceLabelsBuilder::new(fb);
        builder.add_entries(entries_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ResourceLabels<'fb>> for odf::resource::ResourceLabels {
    fn deserialize(proxy: fb::ResourceLabels<'fb>) -> Self {
        Self {
            entries: proxy
                .entries()
                .unwrap_or_default()
                .iter()
                .map(|entry| {
                    let key = entry.key().parse().unwrap();
                    let value = serde_json::from_str(entry.value().unwrap()).unwrap();
                    (key, value)
                })
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourcePhase
// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourcePhase
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::resource::ResourcePhase> for fb::ResourcePhase {
    fn from(v: odf::resource::ResourcePhase) -> Self {
        match v {
            odf::resource::ResourcePhase::Pending => fb::ResourcePhase::Pending,
            odf::resource::ResourcePhase::Reconciling => fb::ResourcePhase::Reconciling,
            odf::resource::ResourcePhase::Ready => fb::ResourcePhase::Ready,
            odf::resource::ResourcePhase::Failed => fb::ResourcePhase::Failed,
        }
    }
}

impl Into<odf::resource::ResourcePhase> for fb::ResourcePhase {
    fn into(self) -> odf::resource::ResourcePhase {
        match self {
            fb::ResourcePhase::Pending => odf::resource::ResourcePhase::Pending,
            fb::ResourcePhase::Reconciling => odf::resource::ResourcePhase::Reconciling,
            fb::ResourcePhase::Ready => odf::resource::ResourcePhase::Ready,
            fb::ResourcePhase::Failed => odf::resource::ResourcePhase::Failed,
            _ => panic!("Invalid enum value: {}", self.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceStatus
// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceStatus
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::resource::ResourceStatus {
    type OffsetT = WIPOffset<fb::ResourceStatus<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let conditions_offset = self.conditions.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::ResourceStatusBuilder::new(fb);
        builder.add_phase(self.phase.into());
        self.observed_generation
            .map(|v| builder.add_observed_generation(v));
        self.reconciled_at
            .map(|v| builder.add_reconciled_at(&datetime_to_fb(&v)));
        conditions_offset.map(|off| builder.add_conditions(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ResourceStatus<'fb>> for odf::resource::ResourceStatus {
    fn deserialize(proxy: fb::ResourceStatus<'fb>) -> Self {
        odf::resource::ResourceStatus {
            phase: proxy.phase().into(),
            observed_generation: proxy.observed_generation().map(|v| v),
            reconciled_at: proxy.reconciled_at().map(|v| fb_to_datetime(v)),
            conditions: proxy
                .conditions()
                .map(|v| odf::resource::ResourceConditions::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Secret
// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Secret
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::config::Secret {
    type OffsetT = WIPOffset<fb::Secret<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let value_offset = { fb.create_string(&self.value) };
        let content_encoding_offset = self.content_encoding.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::SecretBuilder::new(fb);
        builder.add_value(value_offset);
        content_encoding_offset.map(|off| builder.add_content_encoding(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Secret<'fb>> for odf::config::Secret {
    fn deserialize(proxy: fb::Secret<'fb>) -> Self {
        odf::config::Secret {
            value: proxy.value().map(|v| v.to_owned()).unwrap(),
            content_encoding: proxy.content_encoding().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SecretSetSpec
// Schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSetSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::config::SecretSetSpec {
    type OffsetT = WIPOffset<fb::SecretSetSpec<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let secrets_offset = { self.secrets.serialize(fb) };
        let mut builder = fb::SecretSetSpecBuilder::new(fb);
        builder.add_secrets(secrets_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SecretSetSpec<'fb>> for odf::config::SecretSetSpec {
    fn deserialize(proxy: fb::SecretSetSpec<'fb>) -> Self {
        odf::config::SecretSetSpec {
            secrets: proxy
                .secrets()
                .map(|v| odf::config::Secrets::deserialize(v))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Secrets
// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Secrets
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::config::Secrets {
    type OffsetT = WIPOffset<fb::Secrets<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let entries: Vec<_> = self
            .entries
            .iter()
            .map(|(key, value)| {
                let key_offset = fb.create_string(key.as_str());
                let value_offset = value.serialize(fb);
                let mut entry_builder = fb::SecretsEntryBuilder::new(fb);
                entry_builder.add_key(key_offset);
                entry_builder.add_value(value_offset);
                entry_builder.finish()
            })
            .collect();
        let entries_offset = fb.create_vector(&entries);
        let mut builder = fb::SecretsBuilder::new(fb);
        builder.add_entries(entries_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Secrets<'fb>> for odf::config::Secrets {
    fn deserialize(proxy: fb::Secrets<'fb>) -> Self {
        Self {
            entries: proxy
                .entries()
                .unwrap_or_default()
                .iter()
                .map(|entry| {
                    let key = entry.key().parse().unwrap();
                    let value = odf::config::Secret::deserialize(entry.value().unwrap());
                    (key, value)
                })
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Seed
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Seed
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::Seed {
    type OffsetT = WIPOffset<fb::Seed<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let dataset_id_offset = { fb.create_vector(&self.dataset_id.as_bytes().as_slice()) };
        let mut builder = fb::SeedBuilder::new(fb);
        builder.add_dataset_id(dataset_id_offset);
        builder.add_dataset_kind(self.dataset_kind.into());
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Seed<'fb>> for odf::dataset::Seed {
    fn deserialize(proxy: fb::Seed<'fb>) -> Self {
        odf::dataset::Seed {
            dataset_id: proxy
                .dataset_id()
                .map(|v| odf::dataset::DatasetID::from_bytes(v.bytes()).unwrap())
                .unwrap(),
            dataset_kind: proxy.dataset_kind().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetAttachments
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetAttachments
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::SetAttachments {
    type OffsetT = WIPOffset<fb::SetAttachments<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let attachments_offset = { self.attachments.serialize(fb) };
        let mut builder = fb::SetAttachmentsBuilder::new(fb);
        builder.add_attachments_type(attachments_offset.0);
        builder.add_attachments(attachments_offset.1);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SetAttachments<'fb>> for odf::dataset::SetAttachments {
    fn deserialize(proxy: fb::SetAttachments<'fb>) -> Self {
        odf::dataset::SetAttachments {
            attachments: proxy
                .attachments()
                .map(|v| odf::dataset::Attachments::deserialize(v, proxy.attachments_type()))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetDataSchema
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetDataSchema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::SetDataSchema {
    type OffsetT = WIPOffset<fb::SetDataSchema<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let raw_arrow_schema_offset = self
            .raw_arrow_schema
            .as_ref()
            .map(|v| fb.create_vector(&v[..]));
        let schema_offset = self.schema.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::SetDataSchemaBuilder::new(fb);
        raw_arrow_schema_offset.map(|off| builder.add_raw_arrow_schema(off));
        schema_offset.map(|off| builder.add_schema(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SetDataSchema<'fb>> for odf::dataset::SetDataSchema {
    fn deserialize(proxy: fb::SetDataSchema<'fb>) -> Self {
        odf::dataset::SetDataSchema {
            raw_arrow_schema: proxy.raw_arrow_schema().map(|v| v.bytes().to_vec()),
            schema: proxy
                .schema()
                .map(|v| odf::data::DataSchema::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetInfo
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetInfo
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::SetInfo {
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

impl<'fb> FlatbuffersDeserializable<fb::SetInfo<'fb>> for odf::dataset::SetInfo {
    fn deserialize(proxy: fb::SetInfo<'fb>) -> Self {
        odf::dataset::SetInfo {
            description: proxy.description().map(|v| v.to_owned()),
            keywords: proxy
                .keywords()
                .map(|v| v.iter().map(|i| i.to_owned()).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetLicense
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetLicense
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::SetLicense {
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

impl<'fb> FlatbuffersDeserializable<fb::SetLicense<'fb>> for odf::dataset::SetLicense {
    fn deserialize(proxy: fb::SetLicense<'fb>) -> Self {
        odf::dataset::SetLicense {
            short_name: proxy.short_name().map(|v| v.to_owned()).unwrap(),
            name: proxy.name().map(|v| v.to_owned()).unwrap(),
            spdx_id: proxy.spdx_id().map(|v| v.to_owned()),
            website_url: proxy.website_url().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetPollingSource
// Schema: https://opendatafabric.org/schemas/legacy/v0/SetPollingSource
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::legacy::SetPollingSource {
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

impl<'fb> FlatbuffersDeserializable<fb::SetPollingSource<'fb>> for odf::legacy::SetPollingSource {
    fn deserialize(proxy: fb::SetPollingSource<'fb>) -> Self {
        odf::legacy::SetPollingSource {
            fetch: proxy
                .fetch()
                .map(|v| odf::legacy::FetchStep::deserialize(v, proxy.fetch_type()))
                .unwrap(),
            prepare: proxy.prepare().map(|v| {
                v.iter()
                    .map(|i| odf::source::PrepStep::deserialize(i.value().unwrap(), i.value_type()))
                    .collect()
            }),
            read: proxy
                .read()
                .map(|v| odf::source::ReadStep::deserialize(v, proxy.read_type()))
                .unwrap(),
            preprocess: proxy
                .preprocess()
                .map(|v| odf::dataset::Transform::deserialize(v, proxy.preprocess_type())),
            merge: proxy
                .merge()
                .map(|v| odf::source::MergeStrategy::deserialize(v, proxy.merge_type()))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetTransform
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetTransform
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::SetTransform {
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

impl<'fb> FlatbuffersDeserializable<fb::SetTransform<'fb>> for odf::dataset::SetTransform {
    fn deserialize(proxy: fb::SetTransform<'fb>) -> Self {
        odf::dataset::SetTransform {
            inputs: proxy
                .inputs()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::dataset::TransformInput::deserialize(i))
                        .collect()
                })
                .unwrap(),
            transform: proxy
                .transform()
                .map(|v| odf::dataset::Transform::deserialize(v, proxy.transform_type()))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetVocab
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetVocab
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::SetVocab {
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

impl<'fb> FlatbuffersDeserializable<fb::SetVocab<'fb>> for odf::dataset::SetVocab {
    fn deserialize(proxy: fb::SetVocab<'fb>) -> Self {
        odf::dataset::SetVocab {
            offset_column: proxy.offset_column().map(|v| v.to_owned()),
            operation_type_column: proxy.operation_type_column().map(|v| v.to_owned()),
            system_time_column: proxy.system_time_column().map(|v| v.to_owned()),
            event_time_column: proxy.event_time_column().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceCaching
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::SourceCaching> for odf::source::SourceCaching {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::SourceCaching, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::source::SourceCaching::Forever(v) => (
                fb::SourceCaching::SourceCachingForever,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::SourceCaching> for odf::source::SourceCaching {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::SourceCaching) -> Self {
        match t {
            fb::SourceCaching::SourceCachingForever => {
                odf::source::SourceCaching::Forever(odf::source::SourceCachingForever::deserialize(
                    unsafe { fb::SourceCachingForever::init_from_table(table) },
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceCachingForever
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceCaching#/$defs/Forever
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::SourceCachingForever {
    type OffsetT = WIPOffset<fb::SourceCachingForever<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::SourceCachingForeverBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SourceCachingForever<'fb>>
    for odf::source::SourceCachingForever
{
    fn deserialize(proxy: fb::SourceCachingForever<'fb>) -> Self {
        odf::source::SourceCachingForever {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceOrdering
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceOrdering
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::source::SourceOrdering> for fb::SourceOrdering {
    fn from(v: odf::source::SourceOrdering) -> Self {
        match v {
            odf::source::SourceOrdering::ByEventTime => fb::SourceOrdering::ByEventTime,
            odf::source::SourceOrdering::ByName => fb::SourceOrdering::ByName,
        }
    }
}

impl Into<odf::source::SourceOrdering> for fb::SourceOrdering {
    fn into(self) -> odf::source::SourceOrdering {
        match self {
            fb::SourceOrdering::ByEventTime => odf::source::SourceOrdering::ByEventTime,
            fb::SourceOrdering::ByName => odf::source::SourceOrdering::ByName,
            _ => panic!("Invalid enum value: {}", self.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceSpec
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::SourceSpec {
    type OffsetT = WIPOffset<fb::SourceSpec<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let config_offset = self.config.as_ref().map(|v| v.serialize(fb));
        let ingress_offset = self.ingress.as_ref().map(|v| v.serialize(fb));
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
        let merge_offset = self.merge.as_ref().map(|v| v.serialize(fb));
        let vocab_offset = self.vocab.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::SourceSpecBuilder::new(fb);
        config_offset.map(|off| builder.add_config(off));
        ingress_offset.map(|(e, off)| {
            builder.add_ingress_type(e);
            builder.add_ingress(off)
        });
        prepare_offset.map(|off| builder.add_prepare(off));
        builder.add_read_type(read_offset.0);
        builder.add_read(read_offset.1);
        preprocess_offset.map(|(e, off)| {
            builder.add_preprocess_type(e);
            builder.add_preprocess(off)
        });
        merge_offset.map(|(e, off)| {
            builder.add_merge_type(e);
            builder.add_merge(off)
        });
        vocab_offset.map(|off| builder.add_vocab(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::SourceSpec<'fb>> for odf::source::SourceSpec {
    fn deserialize(proxy: fb::SourceSpec<'fb>) -> Self {
        odf::source::SourceSpec {
            config: proxy
                .config()
                .map(|v| odf::config::ValueRefs::deserialize(v)),
            ingress: proxy
                .ingress()
                .map(|v| odf::source::Ingress::deserialize(v, proxy.ingress_type())),
            prepare: proxy.prepare().map(|v| {
                v.iter()
                    .map(|i| odf::source::PrepStep::deserialize(i.value().unwrap(), i.value_type()))
                    .collect()
            }),
            read: proxy
                .read()
                .map(|v| odf::source::ReadStep::deserialize(v, proxy.read_type()))
                .unwrap(),
            preprocess: proxy
                .preprocess()
                .map(|v| odf::dataset::Transform::deserialize(v, proxy.preprocess_type())),
            merge: proxy
                .merge()
                .map(|v| odf::source::MergeStrategy::deserialize(v, proxy.merge_type())),
            vocab: proxy
                .vocab()
                .map(|v| odf::dataset::DatasetVocabulary::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceState
// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceState
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::source::SourceState {
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

impl<'fb> FlatbuffersDeserializable<fb::SourceState<'fb>> for odf::source::SourceState {
    fn deserialize(proxy: fb::SourceState<'fb>) -> Self {
        odf::source::SourceState {
            source_name: proxy.source_name().map(|v| v.to_owned()).unwrap(),
            kind: proxy.kind().map(|v| v.to_owned()).unwrap(),
            value: proxy.value().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SqlQueryStep
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SqlQueryStep
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::SqlQueryStep {
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

impl<'fb> FlatbuffersDeserializable<fb::SqlQueryStep<'fb>> for odf::dataset::SqlQueryStep {
    fn deserialize(proxy: fb::SqlQueryStep<'fb>) -> Self {
        odf::dataset::SqlQueryStep {
            alias: proxy.alias().map(|v| v.to_owned()),
            query: proxy.query().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TaskSpec
// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::TaskSpec> for odf::flow::TaskSpec {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::TaskSpec, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::flow::TaskSpec::Ingest(v) => (
                fb::TaskSpec::TaskSpecIngest,
                v.serialize(fb).as_union_value(),
            ),
            odf::flow::TaskSpec::Compaction(v) => (
                fb::TaskSpec::TaskSpecCompaction,
                v.serialize(fb).as_union_value(),
            ),
            odf::flow::TaskSpec::GarbageCollection(v) => (
                fb::TaskSpec::TaskSpecGarbageCollection,
                v.serialize(fb).as_union_value(),
            ),
            odf::flow::TaskSpec::WebhookCall(v) => (
                fb::TaskSpec::TaskSpecWebhookCall,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::TaskSpec> for odf::flow::TaskSpec {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::TaskSpec) -> Self {
        match t {
            fb::TaskSpec::TaskSpecIngest => {
                odf::flow::TaskSpec::Ingest(odf::flow::TaskSpecIngest::deserialize(unsafe {
                    fb::TaskSpecIngest::init_from_table(table)
                }))
            }
            fb::TaskSpec::TaskSpecCompaction => {
                odf::flow::TaskSpec::Compaction(odf::flow::TaskSpecCompaction::deserialize(
                    unsafe { fb::TaskSpecCompaction::init_from_table(table) },
                ))
            }
            fb::TaskSpec::TaskSpecGarbageCollection => odf::flow::TaskSpec::GarbageCollection(
                odf::flow::TaskSpecGarbageCollection::deserialize(unsafe {
                    fb::TaskSpecGarbageCollection::init_from_table(table)
                }),
            ),
            fb::TaskSpec::TaskSpecWebhookCall => {
                odf::flow::TaskSpec::WebhookCall(odf::flow::TaskSpecWebhookCall::deserialize(
                    unsafe { fb::TaskSpecWebhookCall::init_from_table(table) },
                ))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TaskSpecCompaction
// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/Compaction
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::flow::TaskSpecCompaction {
    type OffsetT = WIPOffset<fb::TaskSpecCompaction<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let params_offset = self.params.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::TaskSpecCompactionBuilder::new(fb);
        params_offset.map(|off| builder.add_params(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TaskSpecCompaction<'fb>> for odf::flow::TaskSpecCompaction {
    fn deserialize(proxy: fb::TaskSpecCompaction<'fb>) -> Self {
        odf::flow::TaskSpecCompaction {
            params: proxy
                .params()
                .map(|v| odf::dataset::CompactionParams::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TaskSpecGarbageCollection
// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/GarbageCollection
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::flow::TaskSpecGarbageCollection {
    type OffsetT = WIPOffset<fb::TaskSpecGarbageCollection<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::TaskSpecGarbageCollectionBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TaskSpecGarbageCollection<'fb>>
    for odf::flow::TaskSpecGarbageCollection
{
    fn deserialize(proxy: fb::TaskSpecGarbageCollection<'fb>) -> Self {
        odf::flow::TaskSpecGarbageCollection {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TaskSpecIngest
// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/Ingest
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::flow::TaskSpecIngest {
    type OffsetT = WIPOffset<fb::TaskSpecIngest<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let source_offset = { self.source.serialize(fb) };
        let params_offset = self.params.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::TaskSpecIngestBuilder::new(fb);
        builder.add_source(source_offset);
        params_offset.map(|off| builder.add_params(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TaskSpecIngest<'fb>> for odf::flow::TaskSpecIngest {
    fn deserialize(proxy: fb::TaskSpecIngest<'fb>) -> Self {
        odf::flow::TaskSpecIngest {
            source: proxy
                .source()
                .map(|v| odf::resource::ResourceRef::deserialize(v))
                .unwrap(),
            params: proxy
                .params()
                .map(|v| odf::source::IngestParams::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TaskSpecWebhookCall
// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/WebhookCall
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::flow::TaskSpecWebhookCall {
    type OffsetT = WIPOffset<fb::TaskSpecWebhookCall<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let target_offset = { self.target.serialize(fb) };
        let payload_offset = self.payload.as_ref().map(|v| fb.create_string(&v));
        let mut builder = fb::TaskSpecWebhookCallBuilder::new(fb);
        builder.add_target(target_offset);
        payload_offset.map(|off| builder.add_payload(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TaskSpecWebhookCall<'fb>>
    for odf::flow::TaskSpecWebhookCall
{
    fn deserialize(proxy: fb::TaskSpecWebhookCall<'fb>) -> Self {
        odf::flow::TaskSpecWebhookCall {
            target: proxy
                .target()
                .map(|v| odf::resource::ResourceRef::deserialize(v))
                .unwrap(),
            payload: proxy.payload().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TemporalTable
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/TemporalTable
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::TemporalTable {
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

impl<'fb> FlatbuffersDeserializable<fb::TemporalTable<'fb>> for odf::dataset::TemporalTable {
    fn deserialize(proxy: fb::TemporalTable<'fb>) -> Self {
        odf::dataset::TemporalTable {
            name: proxy.name().map(|v| v.to_owned()).unwrap(),
            primary_key: proxy
                .primary_key()
                .map(|v| v.iter().map(|i| i.to_owned()).collect())
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TimeUnit
// Schema: https://opendatafabric.org/schemas/data/v1alpha1/TimeUnit
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::data::TimeUnit> for fb::TimeUnit {
    fn from(v: odf::data::TimeUnit) -> Self {
        match v {
            odf::data::TimeUnit::Second => fb::TimeUnit::Second,
            odf::data::TimeUnit::Millisecond => fb::TimeUnit::Millisecond,
            odf::data::TimeUnit::Microsecond => fb::TimeUnit::Microsecond,
            odf::data::TimeUnit::Nanosecond => fb::TimeUnit::Nanosecond,
        }
    }
}

impl Into<odf::data::TimeUnit> for fb::TimeUnit {
    fn into(self) -> odf::data::TimeUnit {
        match self {
            fb::TimeUnit::Second => odf::data::TimeUnit::Second,
            fb::TimeUnit::Millisecond => odf::data::TimeUnit::Millisecond,
            fb::TimeUnit::Microsecond => odf::data::TimeUnit::Microsecond,
            fb::TimeUnit::Nanosecond => odf::data::TimeUnit::Nanosecond,
            _ => panic!("Invalid enum value: {}", self.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Transform
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Transform
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::Transform> for odf::dataset::Transform {
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::Transform, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::dataset::Transform::Sql(v) => (
                fb::Transform::TransformSql,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::Transform> for odf::dataset::Transform {
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::Transform) -> Self {
        match t {
            fb::Transform::TransformSql => {
                odf::dataset::Transform::Sql(odf::dataset::TransformSql::deserialize(unsafe {
                    fb::TransformSql::init_from_table(table)
                }))
            }
            _ => panic!("Invalid enum value: {}", t.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformSql
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Transform#/$defs/Sql
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::TransformSql {
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

impl<'fb> FlatbuffersDeserializable<fb::TransformSql<'fb>> for odf::dataset::TransformSql {
    fn deserialize(proxy: fb::TransformSql<'fb>) -> Self {
        odf::dataset::TransformSql {
            engine: proxy.engine().map(|v| v.to_owned()).unwrap(),
            version: proxy.version().map(|v| v.to_owned()),
            query: proxy.query().map(|v| v.to_owned()),
            queries: proxy.queries().map(|v| {
                v.iter()
                    .map(|i| odf::dataset::SqlQueryStep::deserialize(i))
                    .collect()
            }),
            temporal_tables: proxy.temporal_tables().map(|v| {
                v.iter()
                    .map(|i| odf::dataset::TemporalTable::deserialize(i))
                    .collect()
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformInput
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/TransformInput
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::TransformInput {
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

impl<'fb> FlatbuffersDeserializable<fb::TransformInput<'fb>> for odf::dataset::TransformInput {
    fn deserialize(proxy: fb::TransformInput<'fb>) -> Self {
        odf::dataset::TransformInput {
            dataset_ref: proxy
                .dataset_ref()
                .map(|v| odf::dataset::DatasetRef::try_from(v).unwrap())
                .unwrap(),
            alias: proxy.alias().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformRequest
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformRequest
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::engine::TransformRequest {
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

impl<'fb> FlatbuffersDeserializable<fb::TransformRequest<'fb>> for odf::engine::TransformRequest {
    fn deserialize(proxy: fb::TransformRequest<'fb>) -> Self {
        odf::engine::TransformRequest {
            dataset_id: proxy
                .dataset_id()
                .map(|v| odf::dataset::DatasetID::from_bytes(v.bytes()).unwrap())
                .unwrap(),
            dataset_alias: proxy
                .dataset_alias()
                .map(|v| odf::dataset::DatasetAlias::try_from(v).unwrap())
                .unwrap(),
            system_time: proxy.system_time().map(|v| fb_to_datetime(v)).unwrap(),
            vocab: proxy
                .vocab()
                .map(|v| odf::dataset::DatasetVocabulary::deserialize(v))
                .unwrap(),
            transform: proxy
                .transform()
                .map(|v| odf::dataset::Transform::deserialize(v, proxy.transform_type()))
                .unwrap(),
            query_inputs: proxy
                .query_inputs()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::engine::TransformRequestInput::deserialize(i))
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
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformRequestInput
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::engine::TransformRequestInput {
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

impl<'fb> FlatbuffersDeserializable<fb::TransformRequestInput<'fb>>
    for odf::engine::TransformRequestInput
{
    fn deserialize(proxy: fb::TransformRequestInput<'fb>) -> Self {
        odf::engine::TransformRequestInput {
            dataset_id: proxy
                .dataset_id()
                .map(|v| odf::dataset::DatasetID::from_bytes(v.bytes()).unwrap())
                .unwrap(),
            dataset_alias: proxy
                .dataset_alias()
                .map(|v| odf::dataset::DatasetAlias::try_from(v).unwrap())
                .unwrap(),
            query_alias: proxy.query_alias().map(|v| v.to_owned()).unwrap(),
            vocab: proxy
                .vocab()
                .map(|v| odf::dataset::DatasetVocabulary::deserialize(v))
                .unwrap(),
            offset_interval: proxy
                .offset_interval()
                .map(|v| odf::dataset::OffsetInterval::deserialize(v)),
            data_paths: proxy
                .data_paths()
                .map(|v| v.iter().map(|i| PathBuf::from(i)).collect())
                .unwrap(),
            schema_file: proxy.schema_file().map(|v| PathBuf::from(v)).unwrap(),
            explicit_watermarks: proxy
                .explicit_watermarks()
                .map(|v| {
                    v.iter()
                        .map(|i| odf::dataset::Watermark::deserialize(i))
                        .collect()
                })
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponse
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersEnumSerializable<'fb, fb::TransformResponse>
    for odf::engine::TransformResponse
{
    fn serialize(
        &self,
        fb: &mut FlatBufferBuilder<'fb>,
    ) -> (fb::TransformResponse, WIPOffset<UnionWIPOffset>) {
        match self {
            odf::engine::TransformResponse::Progress(v) => (
                fb::TransformResponse::TransformResponseProgress,
                v.serialize(fb).as_union_value(),
            ),
            odf::engine::TransformResponse::Success(v) => (
                fb::TransformResponse::TransformResponseSuccess,
                v.serialize(fb).as_union_value(),
            ),
            odf::engine::TransformResponse::InvalidQuery(v) => (
                fb::TransformResponse::TransformResponseInvalidQuery,
                v.serialize(fb).as_union_value(),
            ),
            odf::engine::TransformResponse::InternalError(v) => (
                fb::TransformResponse::TransformResponseInternalError,
                v.serialize(fb).as_union_value(),
            ),
        }
    }
}

impl<'fb> FlatbuffersEnumDeserializable<'fb, fb::TransformResponse>
    for odf::engine::TransformResponse
{
    fn deserialize(table: flatbuffers::Table<'fb>, t: fb::TransformResponse) -> Self {
        match t {
            fb::TransformResponse::TransformResponseProgress => {
                odf::engine::TransformResponse::Progress(
                    odf::engine::TransformResponseProgress::deserialize(unsafe {
                        fb::TransformResponseProgress::init_from_table(table)
                    }),
                )
            }
            fb::TransformResponse::TransformResponseSuccess => {
                odf::engine::TransformResponse::Success(
                    odf::engine::TransformResponseSuccess::deserialize(unsafe {
                        fb::TransformResponseSuccess::init_from_table(table)
                    }),
                )
            }
            fb::TransformResponse::TransformResponseInvalidQuery => {
                odf::engine::TransformResponse::InvalidQuery(
                    odf::engine::TransformResponseInvalidQuery::deserialize(unsafe {
                        fb::TransformResponseInvalidQuery::init_from_table(table)
                    }),
                )
            }
            fb::TransformResponse::TransformResponseInternalError => {
                odf::engine::TransformResponse::InternalError(
                    odf::engine::TransformResponseInternalError::deserialize(unsafe {
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
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/InternalError
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::engine::TransformResponseInternalError {
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
    for odf::engine::TransformResponseInternalError
{
    fn deserialize(proxy: fb::TransformResponseInternalError<'fb>) -> Self {
        odf::engine::TransformResponseInternalError {
            message: proxy.message().map(|v| v.to_owned()).unwrap(),
            backtrace: proxy.backtrace().map(|v| v.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponseInvalidQuery
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/InvalidQuery
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::engine::TransformResponseInvalidQuery {
    type OffsetT = WIPOffset<fb::TransformResponseInvalidQuery<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let message_offset = { fb.create_string(&self.message) };
        let mut builder = fb::TransformResponseInvalidQueryBuilder::new(fb);
        builder.add_message(message_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TransformResponseInvalidQuery<'fb>>
    for odf::engine::TransformResponseInvalidQuery
{
    fn deserialize(proxy: fb::TransformResponseInvalidQuery<'fb>) -> Self {
        odf::engine::TransformResponseInvalidQuery {
            message: proxy.message().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponseProgress
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/Progress
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::engine::TransformResponseProgress {
    type OffsetT = WIPOffset<fb::TransformResponseProgress<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::TransformResponseProgressBuilder::new(fb);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::TransformResponseProgress<'fb>>
    for odf::engine::TransformResponseProgress
{
    fn deserialize(proxy: fb::TransformResponseProgress<'fb>) -> Self {
        odf::engine::TransformResponseProgress {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponseSuccess
// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/Success
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::engine::TransformResponseSuccess {
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
    for odf::engine::TransformResponseSuccess
{
    fn deserialize(proxy: fb::TransformResponseSuccess<'fb>) -> Self {
        odf::engine::TransformResponseSuccess {
            new_offset_interval: proxy
                .new_offset_interval()
                .map(|v| odf::dataset::OffsetInterval::deserialize(v)),
            new_watermark: proxy.new_watermark().map(|v| fb_to_datetime(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ValueRefs
// Schema: https://opendatafabric.org/schemas/config/v1alpha1/ValueRefs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::config::ValueRefs {
    type OffsetT = WIPOffset<fb::ValueRefs<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let entries: Vec<_> = self
            .entries
            .iter()
            .map(|(key, value)| {
                let key_offset = fb.create_string(key.as_str());
                let value_offset = value.serialize(fb);
                let mut entry_builder = fb::ValueRefsEntryBuilder::new(fb);
                entry_builder.add_key(key_offset);
                entry_builder.add_value(value_offset);
                entry_builder.finish()
            })
            .collect();
        let entries_offset = fb.create_vector(&entries);
        let mut builder = fb::ValueRefsBuilder::new(fb);
        builder.add_entries(entries_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ValueRefs<'fb>> for odf::config::ValueRefs {
    fn deserialize(proxy: fb::ValueRefs<'fb>) -> Self {
        Self {
            entries: proxy
                .entries()
                .unwrap_or_default()
                .iter()
                .map(|entry| {
                    let key = entry.key().parse().unwrap();
                    let value = odf::config::ValueRef::deserialize(entry.value().unwrap());
                    (key, value)
                })
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Variable
// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Variable
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::config::Variable {
    type OffsetT = WIPOffset<fb::Variable<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let value_offset = { fb.create_string(&self.value) };
        let mut builder = fb::VariableBuilder::new(fb);
        builder.add_value(value_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Variable<'fb>> for odf::config::Variable {
    fn deserialize(proxy: fb::Variable<'fb>) -> Self {
        odf::config::Variable {
            value: proxy.value().map(|v| v.to_owned()).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// VariableSetSpec
// Schema: https://opendatafabric.org/schemas/config/v1alpha1/VariableSetSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::config::VariableSetSpec {
    type OffsetT = WIPOffset<fb::VariableSetSpec<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let variables_offset = { self.variables.serialize(fb) };
        let mut builder = fb::VariableSetSpecBuilder::new(fb);
        builder.add_variables(variables_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::VariableSetSpec<'fb>> for odf::config::VariableSetSpec {
    fn deserialize(proxy: fb::VariableSetSpec<'fb>) -> Self {
        odf::config::VariableSetSpec {
            variables: proxy
                .variables()
                .map(|v| odf::config::Variables::deserialize(v))
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Variables
// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Variables
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::config::Variables {
    type OffsetT = WIPOffset<fb::Variables<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let entries: Vec<_> = self
            .entries
            .iter()
            .map(|(key, value)| {
                let key_offset = fb.create_string(key.as_str());
                let value_offset = value.serialize(fb);
                let mut entry_builder = fb::VariablesEntryBuilder::new(fb);
                entry_builder.add_key(key_offset);
                entry_builder.add_value(value_offset);
                entry_builder.finish()
            })
            .collect();
        let entries_offset = fb.create_vector(&entries);
        let mut builder = fb::VariablesBuilder::new(fb);
        builder.add_entries(entries_offset);
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Variables<'fb>> for odf::config::Variables {
    fn deserialize(proxy: fb::Variables<'fb>) -> Self {
        Self {
            entries: proxy
                .entries()
                .unwrap_or_default()
                .iter()
                .map(|entry| {
                    let key = entry.key().parse().unwrap();
                    let value = odf::config::Variable::deserialize(entry.value().unwrap());
                    (key, value)
                })
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// VolumeCapacity
// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/VolumeCapacity
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::storage::VolumeCapacity {
    type OffsetT = WIPOffset<fb::VolumeCapacity<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::VolumeCapacityBuilder::new(fb);
        self.storage.map(|v| builder.add_storage(v.as_u64()));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::VolumeCapacity<'fb>> for odf::storage::VolumeCapacity {
    fn deserialize(proxy: fb::VolumeCapacity<'fb>) -> Self {
        odf::storage::VolumeCapacity {
            storage: proxy.storage().map(|v| ByteSize::from(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Watermark
// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Watermark
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::dataset::Watermark {
    type OffsetT = WIPOffset<fb::Watermark<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::WatermarkBuilder::new(fb);
        builder.add_system_time(&datetime_to_fb(&self.system_time));
        builder.add_event_time(&datetime_to_fb(&self.event_time));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::Watermark<'fb>> for odf::dataset::Watermark {
    fn deserialize(proxy: fb::Watermark<'fb>) -> Self {
        odf::dataset::Watermark {
            system_time: proxy.system_time().map(|v| fb_to_datetime(v)).unwrap(),
            event_time: proxy.event_time().map(|v| fb_to_datetime(v)).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WebhookTargetSpec
// Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTargetSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::sink::WebhookTargetSpec {
    type OffsetT = WIPOffset<fb::WebhookTargetSpec<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let url_offset = { fb.create_string(&self.url) };
        let secret_offset = self.secret.as_ref().map(|v| v.serialize(fb));
        let mut builder = fb::WebhookTargetSpecBuilder::new(fb);
        builder.add_url(url_offset);
        secret_offset.map(|off| builder.add_secret(off));
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::WebhookTargetSpec<'fb>> for odf::sink::WebhookTargetSpec {
    fn deserialize(proxy: fb::WebhookTargetSpec<'fb>) -> Self {
        odf::sink::WebhookTargetSpec {
            url: proxy.url().map(|v| v.to_owned()).unwrap(),
            secret: proxy.secret().map(|v| odf::config::Secret::deserialize(v)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WebhookTargetStatus
// Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTargetStatus
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for odf::sink::WebhookTargetStatus {
    type OffsetT = WIPOffset<fb::WebhookTargetStatus<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let mut builder = fb::WebhookTargetStatusBuilder::new(fb);
        builder.add_value(self.value.into());
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::WebhookTargetStatus<'fb>>
    for odf::sink::WebhookTargetStatus
{
    fn deserialize(proxy: fb::WebhookTargetStatus<'fb>) -> Self {
        odf::sink::WebhookTargetStatus {
            value: proxy.value().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WebhookTargetStatusValue
// Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTargetStatus#/$defs/Value
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::sink::WebhookTargetStatusValue> for fb::WebhookTargetStatusValue {
    fn from(v: odf::sink::WebhookTargetStatusValue) -> Self {
        match v {
            odf::sink::WebhookTargetStatusValue::Ready => fb::WebhookTargetStatusValue::Ready,
            odf::sink::WebhookTargetStatusValue::Failed => fb::WebhookTargetStatusValue::Failed,
        }
    }
}

impl Into<odf::sink::WebhookTargetStatusValue> for fb::WebhookTargetStatusValue {
    fn into(self) -> odf::sink::WebhookTargetStatusValue {
        match self {
            fb::WebhookTargetStatusValue::Ready => odf::sink::WebhookTargetStatusValue::Ready,
            fb::WebhookTargetStatusValue::Failed => odf::sink::WebhookTargetStatusValue::Failed,
            _ => panic!("Invalid enum value: {}", self.0),
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

fn duration_to_fb(v: &DurationString) -> fb::Duration {
    fb::Duration::new(v.as_nanos() as u64)
}

fn fb_to_duration(v: &fb::Duration) -> DurationString {
    DurationString::new(std::time::Duration::from_nanos(v.nanoseconds()))
}
