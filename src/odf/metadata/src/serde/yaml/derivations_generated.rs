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

#![allow(clippy::all)]
#![allow(clippy::pedantic)]
#![allow(unused_variables)]

use std::path::PathBuf;

use ::serde::{Deserialize, Deserializer, Serialize, Serializer};
use chrono::{DateTime, Utc};
use multiformats::*;

use super::formats::*;
use crate::dtos;
use crate::identity::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait IntoDto {
    type Dto;
    fn into_dto(self) -> Self::Dto;
}

impl IntoDto for ::serde::de::IgnoredAny {
    type Dto = Self;
    fn into_dto(self) -> Self::Dto {
        self
    }
}

impl IntoDto for ::serde_json::Value {
    type Dto = Self;
    fn into_dto(self) -> Self::Dto {
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! implement_serde_as {
    ($dto:ty, $proxy:ty) => {
        impl ::serde_with::SerializeAs<$dto> for $proxy {
            fn serialize_as<S>(value: &$dto, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                // TODO: PERF: Avoid cloning on serialize
                let value: $proxy = value.clone().into();
                value.serialize(serializer)
            }
        }

        impl<'de> serde_with::DeserializeAs<'de, $dto> for $proxy {
            fn deserialize_as<D>(deserializer: D) -> Result<$dto, D::Error>
            where
                D: Deserializer<'de>,
            {
                <$proxy>::deserialize(deserializer).map(Into::into)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountRef
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#accountref-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct AccountRef {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl IntoDto for AccountRef {
    type Dto = dtos::AccountRef;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::AccountRef> for AccountRef {
    fn from(v: dtos::AccountRef) -> Self {
        Self {
            id: v.id.map(|v| v),
            name: v.name.map(|v| v),
        }
    }
}

impl From<AccountRef> for dtos::AccountRef {
    fn from(v: AccountRef) -> Self {
        Self {
            id: v.id.map(|v| v),
            name: v.name.map(|v| v),
        }
    }
}

impl From<dtos::AccountRef> for StructOrString<AccountRef> {
    fn from(v: dtos::AccountRef) -> Self {
        Self(v.into())
    }
}
impl From<StructOrString<AccountRef>> for dtos::AccountRef {
    fn from(v: StructOrString<AccountRef>) -> Self {
        v.0.into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddData
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct AddData {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_checkpoint: Option<Multihash>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_offset: Option<u64>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_data: Option<DataSlice>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_checkpoint: Option<Checkpoint>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "datetime_rfc3339_opt")]
    pub new_watermark: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_source_state: Option<SourceState>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<ExtraAttributes>,
}

impl IntoDto for AddData {
    type Dto = dtos::AddData;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::AddData> for AddData {
    fn from(v: dtos::AddData) -> Self {
        Self {
            prev_checkpoint: v.prev_checkpoint.map(|v| v),
            prev_offset: v.prev_offset.map(|v| v),
            new_data: v.new_data.map(|v| v.into()),
            new_checkpoint: v.new_checkpoint.map(|v| v.into()),
            new_watermark: v.new_watermark.map(|v| v),
            new_source_state: v.new_source_state.map(|v| v.into()),
            extra: v.extra.map(|v| v.into()),
        }
    }
}

impl From<AddData> for dtos::AddData {
    fn from(v: AddData) -> Self {
        Self {
            prev_checkpoint: v.prev_checkpoint.map(|v| v),
            prev_offset: v.prev_offset.map(|v| v),
            new_data: v.new_data.map(|v| v.into()),
            new_checkpoint: v.new_checkpoint.map(|v| v.into()),
            new_watermark: v.new_watermark.map(|v| v),
            new_source_state: v.new_source_state.map(|v| v.into()),
            extra: v.extra.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddPushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#addpushsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct AddPushSource {
    pub source_name: String,
    pub read: ReadStep,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preprocess: Option<Transform>,
    pub merge: MergeStrategy,
}

impl IntoDto for AddPushSource {
    type Dto = dtos::AddPushSource;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::AddPushSource> for AddPushSource {
    fn from(v: dtos::AddPushSource) -> Self {
        Self {
            source_name: v.source_name,
            read: v.read.into(),
            preprocess: v.preprocess.map(|v| v.into()),
            merge: v.merge.into(),
        }
    }
}

impl From<AddPushSource> for dtos::AddPushSource {
    fn from(v: AddPushSource) -> Self {
        Self {
            source_name: v.source_name,
            read: v.read.into(),
            preprocess: v.preprocess.map(|v| v.into()),
            merge: v.merge.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AttachmentEmbedded
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentembedded-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentEmbedded {
    pub path: String,
    pub content: String,
}

impl IntoDto for AttachmentEmbedded {
    type Dto = dtos::AttachmentEmbedded;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::AttachmentEmbedded> for AttachmentEmbedded {
    fn from(v: dtos::AttachmentEmbedded) -> Self {
        Self {
            path: v.path,
            content: v.content,
        }
    }
}

impl From<AttachmentEmbedded> for dtos::AttachmentEmbedded {
    fn from(v: AttachmentEmbedded) -> Self {
        Self {
            path: v.path,
            content: v.content,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Attachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachments-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum Attachments {
    #[serde(alias = "embedded")]
    Embedded(AttachmentsEmbedded),
}

impl IntoDto for Attachments {
    type Dto = dtos::Attachments;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::Attachments> for Attachments {
    fn from(v: dtos::Attachments) -> Self {
        match v {
            dtos::Attachments::Embedded(v) => Self::Embedded(v.into()),
        }
    }
}

impl From<Attachments> for dtos::Attachments {
    fn from(v: Attachments) -> Self {
        match v {
            Attachments::Embedded(v) => Self::Embedded(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AttachmentsEmbedded
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentsembedded-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentsEmbedded {
    pub items: Vec<AttachmentEmbedded>,
}

impl IntoDto for AttachmentsEmbedded {
    type Dto = dtos::AttachmentsEmbedded;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::AttachmentsEmbedded> for AttachmentsEmbedded {
    fn from(v: dtos::AttachmentsEmbedded) -> Self {
        Self {
            items: v.items.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<AttachmentsEmbedded> for dtos::AttachmentsEmbedded {
    fn from(v: AttachmentsEmbedded) -> Self {
        Self {
            items: v.items.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Checkpoint
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    pub physical_hash: Multihash,
    pub size: u64,
}

impl IntoDto for Checkpoint {
    type Dto = dtos::Checkpoint;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::Checkpoint> for Checkpoint {
    fn from(v: dtos::Checkpoint) -> Self {
        Self {
            physical_hash: v.physical_hash,
            size: v.size,
        }
    }
}

impl From<Checkpoint> for dtos::Checkpoint {
    fn from(v: Checkpoint) -> Self {
        Self {
            physical_hash: v.physical_hash,
            size: v.size,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CompressionFormat
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#compressionformat-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CompressionFormat {
    #[serde(alias = "gzip")]
    Gzip,
    #[serde(alias = "zip")]
    Zip,
}

impl IntoDto for CompressionFormat {
    type Dto = dtos::CompressionFormat;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::CompressionFormat> for CompressionFormat {
    fn from(v: dtos::CompressionFormat) -> Self {
        match v {
            dtos::CompressionFormat::Gzip => Self::Gzip,
            dtos::CompressionFormat::Zip => Self::Zip,
        }
    }
}

impl From<CompressionFormat> for dtos::CompressionFormat {
    fn from(v: CompressionFormat) -> Self {
        match v {
            CompressionFormat::Gzip => Self::Gzip,
            CompressionFormat::Zip => Self::Zip,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataField
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datafield-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataField {
    pub name: String,
    pub r#type: UnionOrString<DataType>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<ExtraAttributes>,
}

impl IntoDto for DataField {
    type Dto = dtos::DataField;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataField> for DataField {
    fn from(v: dtos::DataField) -> Self {
        Self {
            name: v.name,
            r#type: v.r#type.into(),
            extra: v.extra.map(|v| v.into()),
        }
    }
}

impl From<DataField> for dtos::DataField {
    fn from(v: DataField) -> Self {
        Self {
            name: v.name,
            r#type: v.r#type.into(),
            extra: v.extra.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSchema
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataschema-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataSchema {
    pub fields: Vec<DataField>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<ExtraAttributes>,
}

impl IntoDto for DataSchema {
    type Dto = dtos::DataSchema;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataSchema> for DataSchema {
    fn from(v: dtos::DataSchema) -> Self {
        Self {
            fields: v.fields.into_iter().map(Into::into).collect(),
            extra: v.extra.map(|v| v.into()),
        }
    }
}

impl From<DataSchema> for dtos::DataSchema {
    fn from(v: DataSchema) -> Self {
        Self {
            fields: v.fields.into_iter().map(Into::into).collect(),
            extra: v.extra.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataSlice {
    pub logical_hash: Multihash,
    pub physical_hash: Multihash,
    pub offset_interval: OffsetInterval,
    pub size: u64,
}

impl IntoDto for DataSlice {
    type Dto = dtos::DataSlice;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataSlice> for DataSlice {
    fn from(v: dtos::DataSlice) -> Self {
        Self {
            logical_hash: v.logical_hash,
            physical_hash: v.physical_hash,
            offset_interval: v.offset_interval.into(),
            size: v.size,
        }
    }
}

impl From<DataSlice> for dtos::DataSlice {
    fn from(v: DataSlice) -> Self {
        Self {
            logical_hash: v.logical_hash,
            physical_hash: v.physical_hash,
            offset_interval: v.offset_interval.into(),
            size: v.size,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataType
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatype-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum DataType {
    #[serde(alias = "binary")]
    Binary(DataTypeBinary),
    #[serde(alias = "bool")]
    Bool(DataTypeBool),
    #[serde(alias = "date")]
    Date(DataTypeDate),
    #[serde(alias = "decimal")]
    Decimal(DataTypeDecimal),
    #[serde(alias = "duration")]
    Duration(DataTypeDuration),
    #[serde(alias = "float16")]
    Float16(DataTypeFloat16),
    #[serde(alias = "float32")]
    Float32(DataTypeFloat32),
    #[serde(alias = "float64")]
    Float64(DataTypeFloat64),
    #[serde(alias = "int8")]
    Int8(DataTypeInt8),
    #[serde(alias = "int16")]
    Int16(DataTypeInt16),
    #[serde(alias = "int32")]
    Int32(DataTypeInt32),
    #[serde(alias = "int64")]
    Int64(DataTypeInt64),
    #[serde(alias = "uInt8", alias = "uint8")]
    UInt8(DataTypeUInt8),
    #[serde(alias = "uInt16", alias = "uint16")]
    UInt16(DataTypeUInt16),
    #[serde(alias = "uInt32", alias = "uint32")]
    UInt32(DataTypeUInt32),
    #[serde(alias = "uInt64", alias = "uint64")]
    UInt64(DataTypeUInt64),
    #[serde(alias = "list")]
    List(DataTypeList),
    #[serde(alias = "map")]
    Map(DataTypeMap),
    #[serde(alias = "null")]
    Null(DataTypeNull),
    #[serde(alias = "option")]
    Option(DataTypeOption),
    #[serde(alias = "struct")]
    Struct(DataTypeStruct),
    #[serde(alias = "time")]
    Time(DataTypeTime),
    #[serde(alias = "timestamp")]
    Timestamp(DataTypeTimestamp),
    #[serde(alias = "string")]
    String(DataTypeString),
}

impl From<dtos::DataType> for UnionOrString<DataType> {
    fn from(v: dtos::DataType) -> Self {
        Self(v.into())
    }
}
impl From<UnionOrString<DataType>> for dtos::DataType {
    fn from(v: UnionOrString<DataType>) -> Self {
        v.0.into()
    }
}

impl IntoDto for DataType {
    type Dto = dtos::DataType;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataType> for DataType {
    fn from(v: dtos::DataType) -> Self {
        match v {
            dtos::DataType::Binary(v) => Self::Binary(v.into()),
            dtos::DataType::Bool(v) => Self::Bool(v.into()),
            dtos::DataType::Date(v) => Self::Date(v.into()),
            dtos::DataType::Decimal(v) => Self::Decimal(v.into()),
            dtos::DataType::Duration(v) => Self::Duration(v.into()),
            dtos::DataType::Float16(v) => Self::Float16(v.into()),
            dtos::DataType::Float32(v) => Self::Float32(v.into()),
            dtos::DataType::Float64(v) => Self::Float64(v.into()),
            dtos::DataType::Int8(v) => Self::Int8(v.into()),
            dtos::DataType::Int16(v) => Self::Int16(v.into()),
            dtos::DataType::Int32(v) => Self::Int32(v.into()),
            dtos::DataType::Int64(v) => Self::Int64(v.into()),
            dtos::DataType::UInt8(v) => Self::UInt8(v.into()),
            dtos::DataType::UInt16(v) => Self::UInt16(v.into()),
            dtos::DataType::UInt32(v) => Self::UInt32(v.into()),
            dtos::DataType::UInt64(v) => Self::UInt64(v.into()),
            dtos::DataType::List(v) => Self::List(v.into()),
            dtos::DataType::Map(v) => Self::Map(v.into()),
            dtos::DataType::Null(v) => Self::Null(v.into()),
            dtos::DataType::Option(v) => Self::Option(v.into()),
            dtos::DataType::Struct(v) => Self::Struct(v.into()),
            dtos::DataType::Time(v) => Self::Time(v.into()),
            dtos::DataType::Timestamp(v) => Self::Timestamp(v.into()),
            dtos::DataType::String(v) => Self::String(v.into()),
        }
    }
}

impl From<DataType> for dtos::DataType {
    fn from(v: DataType) -> Self {
        match v {
            DataType::Binary(v) => Self::Binary(v.into()),
            DataType::Bool(v) => Self::Bool(v.into()),
            DataType::Date(v) => Self::Date(v.into()),
            DataType::Decimal(v) => Self::Decimal(v.into()),
            DataType::Duration(v) => Self::Duration(v.into()),
            DataType::Float16(v) => Self::Float16(v.into()),
            DataType::Float32(v) => Self::Float32(v.into()),
            DataType::Float64(v) => Self::Float64(v.into()),
            DataType::Int8(v) => Self::Int8(v.into()),
            DataType::Int16(v) => Self::Int16(v.into()),
            DataType::Int32(v) => Self::Int32(v.into()),
            DataType::Int64(v) => Self::Int64(v.into()),
            DataType::UInt8(v) => Self::UInt8(v.into()),
            DataType::UInt16(v) => Self::UInt16(v.into()),
            DataType::UInt32(v) => Self::UInt32(v.into()),
            DataType::UInt64(v) => Self::UInt64(v.into()),
            DataType::List(v) => Self::List(v.into()),
            DataType::Map(v) => Self::Map(v.into()),
            DataType::Null(v) => Self::Null(v.into()),
            DataType::Option(v) => Self::Option(v.into()),
            DataType::Struct(v) => Self::Struct(v.into()),
            DataType::Time(v) => Self::Time(v.into()),
            DataType::Timestamp(v) => Self::Timestamp(v.into()),
            DataType::String(v) => Self::String(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeBinary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypebinary-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeBinary {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixed_length: Option<u64>,
}

impl IntoDto for DataTypeBinary {
    type Dto = dtos::DataTypeBinary;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeBinary> for DataTypeBinary {
    fn from(v: dtos::DataTypeBinary) -> Self {
        Self {
            fixed_length: v.fixed_length.map(|v| v),
        }
    }
}

impl From<DataTypeBinary> for dtos::DataTypeBinary {
    fn from(v: DataTypeBinary) -> Self {
        Self {
            fixed_length: v.fixed_length.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeBool
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypebool-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeBool {}

impl IntoDto for DataTypeBool {
    type Dto = dtos::DataTypeBool;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeBool> for DataTypeBool {
    fn from(v: dtos::DataTypeBool) -> Self {
        Self {}
    }
}

impl From<DataTypeBool> for dtos::DataTypeBool {
    fn from(v: DataTypeBool) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeDate
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypedate-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeDate {}

impl IntoDto for DataTypeDate {
    type Dto = dtos::DataTypeDate;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeDate> for DataTypeDate {
    fn from(v: dtos::DataTypeDate) -> Self {
        Self {}
    }
}

impl From<DataTypeDate> for dtos::DataTypeDate {
    fn from(v: DataTypeDate) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeDecimal
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypedecimal-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeDecimal {
    pub precision: u32,
    pub scale: i32,
}

impl IntoDto for DataTypeDecimal {
    type Dto = dtos::DataTypeDecimal;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeDecimal> for DataTypeDecimal {
    fn from(v: dtos::DataTypeDecimal) -> Self {
        Self {
            precision: v.precision,
            scale: v.scale,
        }
    }
}

impl From<DataTypeDecimal> for dtos::DataTypeDecimal {
    fn from(v: DataTypeDecimal) -> Self {
        Self {
            precision: v.precision,
            scale: v.scale,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeDuration
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeduration-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeDuration {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<TimeUnit>,
}

impl IntoDto for DataTypeDuration {
    type Dto = dtos::DataTypeDuration;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeDuration> for DataTypeDuration {
    fn from(v: dtos::DataTypeDuration) -> Self {
        Self {
            unit: v.unit.map(|v| v.into()),
        }
    }
}

impl From<DataTypeDuration> for dtos::DataTypeDuration {
    fn from(v: DataTypeDuration) -> Self {
        Self {
            unit: v.unit.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeFloat16
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypefloat16-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeFloat16 {}

impl IntoDto for DataTypeFloat16 {
    type Dto = dtos::DataTypeFloat16;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeFloat16> for DataTypeFloat16 {
    fn from(v: dtos::DataTypeFloat16) -> Self {
        Self {}
    }
}

impl From<DataTypeFloat16> for dtos::DataTypeFloat16 {
    fn from(v: DataTypeFloat16) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeFloat32
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypefloat32-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeFloat32 {}

impl IntoDto for DataTypeFloat32 {
    type Dto = dtos::DataTypeFloat32;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeFloat32> for DataTypeFloat32 {
    fn from(v: dtos::DataTypeFloat32) -> Self {
        Self {}
    }
}

impl From<DataTypeFloat32> for dtos::DataTypeFloat32 {
    fn from(v: DataTypeFloat32) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeFloat64
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypefloat64-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeFloat64 {}

impl IntoDto for DataTypeFloat64 {
    type Dto = dtos::DataTypeFloat64;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeFloat64> for DataTypeFloat64 {
    fn from(v: dtos::DataTypeFloat64) -> Self {
        Self {}
    }
}

impl From<DataTypeFloat64> for dtos::DataTypeFloat64 {
    fn from(v: DataTypeFloat64) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeInt16
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeint16-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeInt16 {}

impl IntoDto for DataTypeInt16 {
    type Dto = dtos::DataTypeInt16;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeInt16> for DataTypeInt16 {
    fn from(v: dtos::DataTypeInt16) -> Self {
        Self {}
    }
}

impl From<DataTypeInt16> for dtos::DataTypeInt16 {
    fn from(v: DataTypeInt16) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeInt32
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeint32-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeInt32 {}

impl IntoDto for DataTypeInt32 {
    type Dto = dtos::DataTypeInt32;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeInt32> for DataTypeInt32 {
    fn from(v: dtos::DataTypeInt32) -> Self {
        Self {}
    }
}

impl From<DataTypeInt32> for dtos::DataTypeInt32 {
    fn from(v: DataTypeInt32) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeInt64
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeint64-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeInt64 {}

impl IntoDto for DataTypeInt64 {
    type Dto = dtos::DataTypeInt64;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeInt64> for DataTypeInt64 {
    fn from(v: dtos::DataTypeInt64) -> Self {
        Self {}
    }
}

impl From<DataTypeInt64> for dtos::DataTypeInt64 {
    fn from(v: DataTypeInt64) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeInt8
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeint8-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeInt8 {}

impl IntoDto for DataTypeInt8 {
    type Dto = dtos::DataTypeInt8;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeInt8> for DataTypeInt8 {
    fn from(v: dtos::DataTypeInt8) -> Self {
        Self {}
    }
}

impl From<DataTypeInt8> for dtos::DataTypeInt8 {
    fn from(v: DataTypeInt8) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeList
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypelist-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeList {
    pub item_type: Box<UnionOrString<DataType>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixed_length: Option<u64>,
}

impl IntoDto for DataTypeList {
    type Dto = dtos::DataTypeList;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeList> for DataTypeList {
    fn from(v: dtos::DataTypeList) -> Self {
        Self {
            item_type: Box::new((*v.item_type).into()),
            fixed_length: v.fixed_length.map(|v| v),
        }
    }
}

impl From<DataTypeList> for dtos::DataTypeList {
    fn from(v: DataTypeList) -> Self {
        Self {
            item_type: Box::new((*v.item_type).into()),
            fixed_length: v.fixed_length.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeMap
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypemap-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeMap {
    pub key_type: Box<UnionOrString<DataType>>,
    pub value_type: Box<UnionOrString<DataType>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys_sorted: Option<bool>,
}

impl IntoDto for DataTypeMap {
    type Dto = dtos::DataTypeMap;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeMap> for DataTypeMap {
    fn from(v: dtos::DataTypeMap) -> Self {
        Self {
            key_type: Box::new((*v.key_type).into()),
            value_type: Box::new((*v.value_type).into()),
            keys_sorted: v.keys_sorted.map(|v| v),
        }
    }
}

impl From<DataTypeMap> for dtos::DataTypeMap {
    fn from(v: DataTypeMap) -> Self {
        Self {
            key_type: Box::new((*v.key_type).into()),
            value_type: Box::new((*v.value_type).into()),
            keys_sorted: v.keys_sorted.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeNull
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypenull-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeNull {}

impl IntoDto for DataTypeNull {
    type Dto = dtos::DataTypeNull;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeNull> for DataTypeNull {
    fn from(v: dtos::DataTypeNull) -> Self {
        Self {}
    }
}

impl From<DataTypeNull> for dtos::DataTypeNull {
    fn from(v: DataTypeNull) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeOption
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeoption-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeOption {
    pub inner: Box<UnionOrString<DataType>>,
}

impl IntoDto for DataTypeOption {
    type Dto = dtos::DataTypeOption;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeOption> for DataTypeOption {
    fn from(v: dtos::DataTypeOption) -> Self {
        Self {
            inner: Box::new((*v.inner).into()),
        }
    }
}

impl From<DataTypeOption> for dtos::DataTypeOption {
    fn from(v: DataTypeOption) -> Self {
        Self {
            inner: Box::new((*v.inner).into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeString
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypestring-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeString {}

impl IntoDto for DataTypeString {
    type Dto = dtos::DataTypeString;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeString> for DataTypeString {
    fn from(v: dtos::DataTypeString) -> Self {
        Self {}
    }
}

impl From<DataTypeString> for dtos::DataTypeString {
    fn from(v: DataTypeString) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeStruct
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypestruct-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeStruct {
    pub fields: Vec<DataField>,
}

impl IntoDto for DataTypeStruct {
    type Dto = dtos::DataTypeStruct;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeStruct> for DataTypeStruct {
    fn from(v: dtos::DataTypeStruct) -> Self {
        Self {
            fields: v.fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<DataTypeStruct> for dtos::DataTypeStruct {
    fn from(v: DataTypeStruct) -> Self {
        Self {
            fields: v.fields.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeTime
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypetime-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeTime {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<TimeUnit>,
}

impl IntoDto for DataTypeTime {
    type Dto = dtos::DataTypeTime;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeTime> for DataTypeTime {
    fn from(v: dtos::DataTypeTime) -> Self {
        Self {
            unit: v.unit.map(|v| v.into()),
        }
    }
}

impl From<DataTypeTime> for dtos::DataTypeTime {
    fn from(v: DataTypeTime) -> Self {
        Self {
            unit: v.unit.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeTimestamp
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypetimestamp-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeTimestamp {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<TimeUnit>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

impl IntoDto for DataTypeTimestamp {
    type Dto = dtos::DataTypeTimestamp;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeTimestamp> for DataTypeTimestamp {
    fn from(v: dtos::DataTypeTimestamp) -> Self {
        Self {
            unit: v.unit.map(|v| v.into()),
            timezone: v.timezone.map(|v| v),
        }
    }
}

impl From<DataTypeTimestamp> for dtos::DataTypeTimestamp {
    fn from(v: DataTypeTimestamp) -> Self {
        Self {
            unit: v.unit.map(|v| v.into()),
            timezone: v.timezone.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeUInt16
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeuint16-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeUInt16 {}

impl IntoDto for DataTypeUInt16 {
    type Dto = dtos::DataTypeUInt16;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeUInt16> for DataTypeUInt16 {
    fn from(v: dtos::DataTypeUInt16) -> Self {
        Self {}
    }
}

impl From<DataTypeUInt16> for dtos::DataTypeUInt16 {
    fn from(v: DataTypeUInt16) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeUInt32
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeuint32-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeUInt32 {}

impl IntoDto for DataTypeUInt32 {
    type Dto = dtos::DataTypeUInt32;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeUInt32> for DataTypeUInt32 {
    fn from(v: dtos::DataTypeUInt32) -> Self {
        Self {}
    }
}

impl From<DataTypeUInt32> for dtos::DataTypeUInt32 {
    fn from(v: DataTypeUInt32) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeUInt64
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeuint64-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeUInt64 {}

impl IntoDto for DataTypeUInt64 {
    type Dto = dtos::DataTypeUInt64;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeUInt64> for DataTypeUInt64 {
    fn from(v: dtos::DataTypeUInt64) -> Self {
        Self {}
    }
}

impl From<DataTypeUInt64> for dtos::DataTypeUInt64 {
    fn from(v: DataTypeUInt64) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataTypeUInt8
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeuint8-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DataTypeUInt8 {}

impl IntoDto for DataTypeUInt8 {
    type Dto = dtos::DataTypeUInt8;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DataTypeUInt8> for DataTypeUInt8 {
    fn from(v: dtos::DataTypeUInt8) -> Self {
        Self {}
    }
}

impl From<DataTypeUInt8> for dtos::DataTypeUInt8 {
    fn from(v: DataTypeUInt8) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetKind
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetkind-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DatasetKind {
    #[serde(alias = "root")]
    Root,
    #[serde(alias = "derivative")]
    Derivative,
}

impl IntoDto for DatasetKind {
    type Dto = dtos::DatasetKind;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DatasetKind> for DatasetKind {
    fn from(v: dtos::DatasetKind) -> Self {
        match v {
            dtos::DatasetKind::Root => Self::Root,
            dtos::DatasetKind::Derivative => Self::Derivative,
        }
    }
}

impl From<DatasetKind> for dtos::DatasetKind {
    fn from(v: DatasetKind) -> Self {
        match v {
            DatasetKind::Root => Self::Root,
            DatasetKind::Derivative => Self::Derivative,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DatasetSnapshot {
    pub name: DatasetAlias,
    pub kind: DatasetKind,
    pub metadata: Vec<MetadataEvent>,
}

impl IntoDto for DatasetSnapshot {
    type Dto = dtos::DatasetSnapshot;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DatasetSnapshot> for DatasetSnapshot {
    fn from(v: dtos::DatasetSnapshot) -> Self {
        Self {
            name: v.name,
            kind: v.kind.into(),
            metadata: v.metadata.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<DatasetSnapshot> for dtos::DatasetSnapshot {
    fn from(v: DatasetSnapshot) -> Self {
        Self {
            name: v.name,
            kind: v.kind.into(),
            metadata: v.metadata.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DatasetVocabulary {
    pub offset_column: String,
    pub operation_type_column: String,
    pub system_time_column: String,
    pub event_time_column: String,
}

impl IntoDto for DatasetVocabulary {
    type Dto = dtos::DatasetVocabulary;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DatasetVocabulary> for DatasetVocabulary {
    fn from(v: dtos::DatasetVocabulary) -> Self {
        Self {
            offset_column: v.offset_column,
            operation_type_column: v.operation_type_column,
            system_time_column: v.system_time_column,
            event_time_column: v.event_time_column,
        }
    }
}

impl From<DatasetVocabulary> for dtos::DatasetVocabulary {
    fn from(v: DatasetVocabulary) -> Self {
        Self {
            offset_column: v.offset_column,
            operation_type_column: v.operation_type_column,
            system_time_column: v.system_time_column,
            event_time_column: v.event_time_column,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DisablePollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepollingsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DisablePollingSource {}

impl IntoDto for DisablePollingSource {
    type Dto = dtos::DisablePollingSource;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DisablePollingSource> for DisablePollingSource {
    fn from(v: dtos::DisablePollingSource) -> Self {
        Self {}
    }
}

impl From<DisablePollingSource> for dtos::DisablePollingSource {
    fn from(v: DisablePollingSource) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DisablePushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepushsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DisablePushSource {
    pub source_name: String,
}

impl IntoDto for DisablePushSource {
    type Dto = dtos::DisablePushSource;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::DisablePushSource> for DisablePushSource {
    fn from(v: dtos::DisablePushSource) -> Self {
        Self {
            source_name: v.source_name,
        }
    }
}

impl From<DisablePushSource> for dtos::DisablePushSource {
    fn from(v: DisablePushSource) -> Self {
        Self {
            source_name: v.source_name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EnvVar
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#envvar-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct EnvVar {
    pub name: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

impl IntoDto for EnvVar {
    type Dto = dtos::EnvVar;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::EnvVar> for EnvVar {
    fn from(v: dtos::EnvVar) -> Self {
        Self {
            name: v.name,
            value: v.value.map(|v| v),
        }
    }
}

impl From<EnvVar> for dtos::EnvVar {
    fn from(v: EnvVar) -> Self {
        Self {
            name: v.name,
            value: v.value.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum EventTimeSource {
    #[serde(alias = "fromMetadata", alias = "frommetadata")]
    FromMetadata(EventTimeSourceFromMetadata),
    #[serde(alias = "fromPath", alias = "frompath")]
    FromPath(EventTimeSourceFromPath),
    #[serde(alias = "fromSystemTime", alias = "fromsystemtime")]
    FromSystemTime(EventTimeSourceFromSystemTime),
}

impl From<dtos::EventTimeSource> for UnionOrString<EventTimeSource> {
    fn from(v: dtos::EventTimeSource) -> Self {
        Self(v.into())
    }
}
impl From<UnionOrString<EventTimeSource>> for dtos::EventTimeSource {
    fn from(v: UnionOrString<EventTimeSource>) -> Self {
        v.0.into()
    }
}

impl IntoDto for EventTimeSource {
    type Dto = dtos::EventTimeSource;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::EventTimeSource> for EventTimeSource {
    fn from(v: dtos::EventTimeSource) -> Self {
        match v {
            dtos::EventTimeSource::FromMetadata(v) => Self::FromMetadata(v.into()),
            dtos::EventTimeSource::FromPath(v) => Self::FromPath(v.into()),
            dtos::EventTimeSource::FromSystemTime(v) => Self::FromSystemTime(v.into()),
        }
    }
}

impl From<EventTimeSource> for dtos::EventTimeSource {
    fn from(v: EventTimeSource) -> Self {
        match v {
            EventTimeSource::FromMetadata(v) => Self::FromMetadata(v.into()),
            EventTimeSource::FromPath(v) => Self::FromPath(v.into()),
            EventTimeSource::FromSystemTime(v) => Self::FromSystemTime(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSourceFromMetadata
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesourcefrommetadata-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct EventTimeSourceFromMetadata {}

impl IntoDto for EventTimeSourceFromMetadata {
    type Dto = dtos::EventTimeSourceFromMetadata;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::EventTimeSourceFromMetadata> for EventTimeSourceFromMetadata {
    fn from(v: dtos::EventTimeSourceFromMetadata) -> Self {
        Self {}
    }
}

impl From<EventTimeSourceFromMetadata> for dtos::EventTimeSourceFromMetadata {
    fn from(v: EventTimeSourceFromMetadata) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSourceFromPath
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesourcefrompath-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct EventTimeSourceFromPath {
    pub pattern: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_format: Option<String>,
}

impl IntoDto for EventTimeSourceFromPath {
    type Dto = dtos::EventTimeSourceFromPath;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::EventTimeSourceFromPath> for EventTimeSourceFromPath {
    fn from(v: dtos::EventTimeSourceFromPath) -> Self {
        Self {
            pattern: v.pattern,
            timestamp_format: v.timestamp_format.map(|v| v),
        }
    }
}

impl From<EventTimeSourceFromPath> for dtos::EventTimeSourceFromPath {
    fn from(v: EventTimeSourceFromPath) -> Self {
        Self {
            pattern: v.pattern,
            timestamp_format: v.timestamp_format.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSourceFromSystemTime
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesourcefromsystemtime-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct EventTimeSourceFromSystemTime {}

impl IntoDto for EventTimeSourceFromSystemTime {
    type Dto = dtos::EventTimeSourceFromSystemTime;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::EventTimeSourceFromSystemTime> for EventTimeSourceFromSystemTime {
    fn from(v: dtos::EventTimeSourceFromSystemTime) -> Self {
        Self {}
    }
}

impl From<EventTimeSourceFromSystemTime> for dtos::EventTimeSourceFromSystemTime {
    fn from(v: EventTimeSourceFromSystemTime) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteTransform {
    pub query_inputs: Vec<ExecuteTransformInput>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_checkpoint: Option<Multihash>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_offset: Option<u64>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_data: Option<DataSlice>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_checkpoint: Option<Checkpoint>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "datetime_rfc3339_opt")]
    pub new_watermark: Option<DateTime<Utc>>,
}

impl IntoDto for ExecuteTransform {
    type Dto = dtos::ExecuteTransform;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ExecuteTransform> for ExecuteTransform {
    fn from(v: dtos::ExecuteTransform) -> Self {
        Self {
            query_inputs: v.query_inputs.into_iter().map(Into::into).collect(),
            prev_checkpoint: v.prev_checkpoint.map(|v| v),
            prev_offset: v.prev_offset.map(|v| v),
            new_data: v.new_data.map(|v| v.into()),
            new_checkpoint: v.new_checkpoint.map(|v| v.into()),
            new_watermark: v.new_watermark.map(|v| v),
        }
    }
}

impl From<ExecuteTransform> for dtos::ExecuteTransform {
    fn from(v: ExecuteTransform) -> Self {
        Self {
            query_inputs: v.query_inputs.into_iter().map(Into::into).collect(),
            prev_checkpoint: v.prev_checkpoint.map(|v| v),
            prev_offset: v.prev_offset.map(|v| v),
            new_data: v.new_data.map(|v| v.into()),
            new_checkpoint: v.new_checkpoint.map(|v| v.into()),
            new_watermark: v.new_watermark.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransforminput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteTransformInput {
    pub dataset_id: DatasetID,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_block_hash: Option<Multihash>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_block_hash: Option<Multihash>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_offset: Option<u64>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_offset: Option<u64>,
}

impl IntoDto for ExecuteTransformInput {
    type Dto = dtos::ExecuteTransformInput;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ExecuteTransformInput> for ExecuteTransformInput {
    fn from(v: dtos::ExecuteTransformInput) -> Self {
        Self {
            dataset_id: v.dataset_id,
            prev_block_hash: v.prev_block_hash.map(|v| v),
            new_block_hash: v.new_block_hash.map(|v| v),
            prev_offset: v.prev_offset.map(|v| v),
            new_offset: v.new_offset.map(|v| v),
        }
    }
}

impl From<ExecuteTransformInput> for dtos::ExecuteTransformInput {
    fn from(v: ExecuteTransformInput) -> Self {
        Self {
            dataset_id: v.dataset_id,
            prev_block_hash: v.prev_block_hash.map(|v| v),
            new_block_hash: v.new_block_hash.map(|v| v),
            prev_offset: v.prev_offset.map(|v| v),
            new_offset: v.new_offset.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExtraAttributes
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#extraattributes-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtraAttributes {
    #[serde(flatten)]
    #[serde(with = "map_value_limited_precision")]
    pub entries: std::collections::BTreeMap<String, serde_json::Value>,
}

impl IntoDto for ExtraAttributes {
    type Dto = dtos::ExtraAttributes;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ExtraAttributes> for ExtraAttributes {
    fn from(v: dtos::ExtraAttributes) -> Self {
        Self { entries: v.entries }
    }
}

impl From<ExtraAttributes> for dtos::ExtraAttributes {
    fn from(v: ExtraAttributes) -> Self {
        Self { entries: v.entries }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum FetchStep {
    #[serde(alias = "url")]
    Url(FetchStepUrl),
    #[serde(alias = "filesGlob", alias = "filesglob")]
    FilesGlob(FetchStepFilesGlob),
    #[serde(alias = "container")]
    Container(FetchStepContainer),
    #[serde(alias = "mqtt")]
    Mqtt(FetchStepMqtt),
    #[serde(alias = "ethereumLogs", alias = "ethereumlogs")]
    EthereumLogs(FetchStepEthereumLogs),
}

impl IntoDto for FetchStep {
    type Dto = dtos::FetchStep;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::FetchStep> for FetchStep {
    fn from(v: dtos::FetchStep) -> Self {
        match v {
            dtos::FetchStep::Url(v) => Self::Url(v.into()),
            dtos::FetchStep::FilesGlob(v) => Self::FilesGlob(v.into()),
            dtos::FetchStep::Container(v) => Self::Container(v.into()),
            dtos::FetchStep::Mqtt(v) => Self::Mqtt(v.into()),
            dtos::FetchStep::EthereumLogs(v) => Self::EthereumLogs(v.into()),
        }
    }
}

impl From<FetchStep> for dtos::FetchStep {
    fn from(v: FetchStep) -> Self {
        match v {
            FetchStep::Url(v) => Self::Url(v.into()),
            FetchStep::FilesGlob(v) => Self::FilesGlob(v.into()),
            FetchStep::Container(v) => Self::Container(v.into()),
            FetchStep::Mqtt(v) => Self::Mqtt(v.into()),
            FetchStep::EthereumLogs(v) => Self::EthereumLogs(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepContainer
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepcontainer-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct FetchStepContainer {
    pub image: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<EnvVar>>,
}

impl IntoDto for FetchStepContainer {
    type Dto = dtos::FetchStepContainer;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::FetchStepContainer> for FetchStepContainer {
    fn from(v: dtos::FetchStepContainer) -> Self {
        Self {
            image: v.image,
            command: v.command.map(|v| v.into_iter().map(Into::into).collect()),
            args: v.args.map(|v| v.into_iter().map(Into::into).collect()),
            env: v.env.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

impl From<FetchStepContainer> for dtos::FetchStepContainer {
    fn from(v: FetchStepContainer) -> Self {
        Self {
            image: v.image,
            command: v.command.map(|v| v.into_iter().map(Into::into).collect()),
            args: v.args.map(|v| v.into_iter().map(Into::into).collect()),
            env: v.env.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepEthereumLogs
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepethereumlogs-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct FetchStepEthereumLogs {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<u64>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_url: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

impl IntoDto for FetchStepEthereumLogs {
    type Dto = dtos::FetchStepEthereumLogs;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::FetchStepEthereumLogs> for FetchStepEthereumLogs {
    fn from(v: dtos::FetchStepEthereumLogs) -> Self {
        Self {
            chain_id: v.chain_id.map(|v| v),
            node_url: v.node_url.map(|v| v),
            filter: v.filter.map(|v| v),
            signature: v.signature.map(|v| v),
        }
    }
}

impl From<FetchStepEthereumLogs> for dtos::FetchStepEthereumLogs {
    fn from(v: FetchStepEthereumLogs) -> Self {
        Self {
            chain_id: v.chain_id.map(|v| v),
            node_url: v.node_url.map(|v| v),
            filter: v.filter.map(|v| v),
            signature: v.signature.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepFilesGlob
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepfilesglob-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct FetchStepFilesGlob {
    pub path: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_time: Option<UnionOrString<EventTimeSource>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<UnionOrString<SourceCaching>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<SourceOrdering>,
}

impl IntoDto for FetchStepFilesGlob {
    type Dto = dtos::FetchStepFilesGlob;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::FetchStepFilesGlob> for FetchStepFilesGlob {
    fn from(v: dtos::FetchStepFilesGlob) -> Self {
        Self {
            path: v.path,
            event_time: v.event_time.map(|v| v.into()),
            cache: v.cache.map(|v| v.into()),
            order: v.order.map(|v| v.into()),
        }
    }
}

impl From<FetchStepFilesGlob> for dtos::FetchStepFilesGlob {
    fn from(v: FetchStepFilesGlob) -> Self {
        Self {
            path: v.path,
            event_time: v.event_time.map(|v| v.into()),
            cache: v.cache.map(|v| v.into()),
            order: v.order.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepMqtt
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepmqtt-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct FetchStepMqtt {
    pub host: String,
    pub port: i32,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    pub topics: Vec<MqttTopicSubscription>,
}

impl IntoDto for FetchStepMqtt {
    type Dto = dtos::FetchStepMqtt;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::FetchStepMqtt> for FetchStepMqtt {
    fn from(v: dtos::FetchStepMqtt) -> Self {
        Self {
            host: v.host,
            port: v.port,
            username: v.username.map(|v| v),
            password: v.password.map(|v| v),
            topics: v.topics.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<FetchStepMqtt> for dtos::FetchStepMqtt {
    fn from(v: FetchStepMqtt) -> Self {
        Self {
            host: v.host,
            port: v.port,
            username: v.username.map(|v| v),
            password: v.password.map(|v| v),
            topics: v.topics.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStepUrl
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepurl-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct FetchStepUrl {
    pub url: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_time: Option<UnionOrString<EventTimeSource>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<UnionOrString<SourceCaching>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<Vec<RequestHeader>>,
}

impl IntoDto for FetchStepUrl {
    type Dto = dtos::FetchStepUrl;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::FetchStepUrl> for FetchStepUrl {
    fn from(v: dtos::FetchStepUrl) -> Self {
        Self {
            url: v.url,
            event_time: v.event_time.map(|v| v.into()),
            cache: v.cache.map(|v| v.into()),
            headers: v.headers.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

impl From<FetchStepUrl> for dtos::FetchStepUrl {
    fn from(v: FetchStepUrl) -> Self {
        Self {
            url: v.url,
            event_time: v.event_time.map(|v| v.into()),
            cache: v.cache.map(|v| v.into()),
            headers: v.headers.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Manifest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#manifest-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Manifest<ContentT> {
    pub kind: String,
    pub version: i32,
    pub content: ContentT,
}

impl<ContentT> IntoDto for Manifest<ContentT>
where
    ContentT: IntoDto,
    <ContentT as IntoDto>::Dto: From<ContentT>,
{
    type Dto = dtos::Manifest<ContentT::Dto>;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl<ContentTFrom, ContentTTo> From<dtos::Manifest<ContentTFrom>> for Manifest<ContentTTo>
where
    ContentTTo: From<ContentTFrom>,
{
    fn from(v: dtos::Manifest<ContentTFrom>) -> Self {
        Self {
            kind: v.kind,
            version: v.version,
            content: v.content.into(),
        }
    }
}

impl<ContentTFrom, ContentTTo> From<Manifest<ContentTFrom>> for dtos::Manifest<ContentTTo>
where
    ContentTTo: From<ContentTFrom>,
{
    fn from(v: Manifest<ContentTFrom>) -> Self {
        Self {
            kind: v.kind,
            version: v.version,
            content: v.content.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum MergeStrategy {
    #[serde(alias = "append")]
    Append(MergeStrategyAppend),
    #[serde(alias = "ledger")]
    Ledger(MergeStrategyLedger),
    #[serde(alias = "snapshot")]
    Snapshot(MergeStrategySnapshot),
    #[serde(alias = "changelogStream", alias = "changelogstream")]
    ChangelogStream(MergeStrategyChangelogStream),
    #[serde(alias = "upsertStream", alias = "upsertstream")]
    UpsertStream(MergeStrategyUpsertStream),
}

impl IntoDto for MergeStrategy {
    type Dto = dtos::MergeStrategy;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::MergeStrategy> for MergeStrategy {
    fn from(v: dtos::MergeStrategy) -> Self {
        match v {
            dtos::MergeStrategy::Append(v) => Self::Append(v.into()),
            dtos::MergeStrategy::Ledger(v) => Self::Ledger(v.into()),
            dtos::MergeStrategy::Snapshot(v) => Self::Snapshot(v.into()),
            dtos::MergeStrategy::ChangelogStream(v) => Self::ChangelogStream(v.into()),
            dtos::MergeStrategy::UpsertStream(v) => Self::UpsertStream(v.into()),
        }
    }
}

impl From<MergeStrategy> for dtos::MergeStrategy {
    fn from(v: MergeStrategy) -> Self {
        match v {
            MergeStrategy::Append(v) => Self::Append(v.into()),
            MergeStrategy::Ledger(v) => Self::Ledger(v.into()),
            MergeStrategy::Snapshot(v) => Self::Snapshot(v.into()),
            MergeStrategy::ChangelogStream(v) => Self::ChangelogStream(v.into()),
            MergeStrategy::UpsertStream(v) => Self::UpsertStream(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategyAppend
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategyappend-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MergeStrategyAppend {}

impl IntoDto for MergeStrategyAppend {
    type Dto = dtos::MergeStrategyAppend;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::MergeStrategyAppend> for MergeStrategyAppend {
    fn from(v: dtos::MergeStrategyAppend) -> Self {
        Self {}
    }
}

impl From<MergeStrategyAppend> for dtos::MergeStrategyAppend {
    fn from(v: MergeStrategyAppend) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategyChangelogStream
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategychangelogstream-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MergeStrategyChangelogStream {
    pub primary_key: Vec<String>,
}

impl IntoDto for MergeStrategyChangelogStream {
    type Dto = dtos::MergeStrategyChangelogStream;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::MergeStrategyChangelogStream> for MergeStrategyChangelogStream {
    fn from(v: dtos::MergeStrategyChangelogStream) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<MergeStrategyChangelogStream> for dtos::MergeStrategyChangelogStream {
    fn from(v: MergeStrategyChangelogStream) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategyLedger
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategyledger-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MergeStrategyLedger {
    pub primary_key: Vec<String>,
}

impl IntoDto for MergeStrategyLedger {
    type Dto = dtos::MergeStrategyLedger;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::MergeStrategyLedger> for MergeStrategyLedger {
    fn from(v: dtos::MergeStrategyLedger) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<MergeStrategyLedger> for dtos::MergeStrategyLedger {
    fn from(v: MergeStrategyLedger) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategySnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategysnapshot-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MergeStrategySnapshot {
    pub primary_key: Vec<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compare_columns: Option<Vec<String>>,
}

impl IntoDto for MergeStrategySnapshot {
    type Dto = dtos::MergeStrategySnapshot;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::MergeStrategySnapshot> for MergeStrategySnapshot {
    fn from(v: dtos::MergeStrategySnapshot) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
            compare_columns: v
                .compare_columns
                .map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

impl From<MergeStrategySnapshot> for dtos::MergeStrategySnapshot {
    fn from(v: MergeStrategySnapshot) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
            compare_columns: v
                .compare_columns
                .map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategyUpsertStream
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategyupsertstream-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MergeStrategyUpsertStream {
    pub primary_key: Vec<String>,
}

impl IntoDto for MergeStrategyUpsertStream {
    type Dto = dtos::MergeStrategyUpsertStream;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::MergeStrategyUpsertStream> for MergeStrategyUpsertStream {
    fn from(v: dtos::MergeStrategyUpsertStream) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<MergeStrategyUpsertStream> for dtos::MergeStrategyUpsertStream {
    fn from(v: MergeStrategyUpsertStream) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MetadataBlock {
    #[serde(with = "datetime_rfc3339")]
    pub system_time: DateTime<Utc>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_block_hash: Option<Multihash>,
    pub sequence_number: u64,
    pub event: MetadataEvent,
}

impl IntoDto for MetadataBlock {
    type Dto = dtos::MetadataBlock;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::MetadataBlock> for MetadataBlock {
    fn from(v: dtos::MetadataBlock) -> Self {
        Self {
            system_time: v.system_time,
            prev_block_hash: v.prev_block_hash.map(|v| v),
            sequence_number: v.sequence_number,
            event: v.event.into(),
        }
    }
}

impl From<MetadataBlock> for dtos::MetadataBlock {
    fn from(v: MetadataBlock) -> Self {
        Self {
            system_time: v.system_time,
            prev_block_hash: v.prev_block_hash.map(|v| v),
            sequence_number: v.sequence_number,
            event: v.event.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum MetadataEvent {
    #[serde(alias = "addData", alias = "adddata")]
    AddData(AddData),
    #[serde(alias = "executeTransform", alias = "executetransform")]
    ExecuteTransform(ExecuteTransform),
    #[serde(alias = "seed")]
    Seed(Seed),
    #[serde(alias = "setPollingSource", alias = "setpollingsource")]
    SetPollingSource(SetPollingSource),
    #[serde(alias = "setTransform", alias = "settransform")]
    SetTransform(SetTransform),
    #[serde(alias = "setVocab", alias = "setvocab")]
    SetVocab(SetVocab),
    #[serde(alias = "setAttachments", alias = "setattachments")]
    SetAttachments(SetAttachments),
    #[serde(alias = "setInfo", alias = "setinfo")]
    SetInfo(SetInfo),
    #[serde(alias = "setLicense", alias = "setlicense")]
    SetLicense(SetLicense),
    #[serde(alias = "setDataSchema", alias = "setdataschema")]
    SetDataSchema(SetDataSchema),
    #[serde(alias = "addPushSource", alias = "addpushsource")]
    AddPushSource(AddPushSource),
    #[serde(alias = "disablePushSource", alias = "disablepushsource")]
    DisablePushSource(DisablePushSource),
    #[serde(alias = "disablePollingSource", alias = "disablepollingsource")]
    DisablePollingSource(DisablePollingSource),
}

impl IntoDto for MetadataEvent {
    type Dto = dtos::MetadataEvent;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::MetadataEvent> for MetadataEvent {
    fn from(v: dtos::MetadataEvent) -> Self {
        match v {
            dtos::MetadataEvent::AddData(v) => Self::AddData(v.into()),
            dtos::MetadataEvent::ExecuteTransform(v) => Self::ExecuteTransform(v.into()),
            dtos::MetadataEvent::Seed(v) => Self::Seed(v.into()),
            dtos::MetadataEvent::SetPollingSource(v) => Self::SetPollingSource(v.into()),
            dtos::MetadataEvent::SetTransform(v) => Self::SetTransform(v.into()),
            dtos::MetadataEvent::SetVocab(v) => Self::SetVocab(v.into()),
            dtos::MetadataEvent::SetAttachments(v) => Self::SetAttachments(v.into()),
            dtos::MetadataEvent::SetInfo(v) => Self::SetInfo(v.into()),
            dtos::MetadataEvent::SetLicense(v) => Self::SetLicense(v.into()),
            dtos::MetadataEvent::SetDataSchema(v) => Self::SetDataSchema(v.into()),
            dtos::MetadataEvent::AddPushSource(v) => Self::AddPushSource(v.into()),
            dtos::MetadataEvent::DisablePushSource(v) => Self::DisablePushSource(v.into()),
            dtos::MetadataEvent::DisablePollingSource(v) => Self::DisablePollingSource(v.into()),
        }
    }
}

impl From<MetadataEvent> for dtos::MetadataEvent {
    fn from(v: MetadataEvent) -> Self {
        match v {
            MetadataEvent::AddData(v) => Self::AddData(v.into()),
            MetadataEvent::ExecuteTransform(v) => Self::ExecuteTransform(v.into()),
            MetadataEvent::Seed(v) => Self::Seed(v.into()),
            MetadataEvent::SetPollingSource(v) => Self::SetPollingSource(v.into()),
            MetadataEvent::SetTransform(v) => Self::SetTransform(v.into()),
            MetadataEvent::SetVocab(v) => Self::SetVocab(v.into()),
            MetadataEvent::SetAttachments(v) => Self::SetAttachments(v.into()),
            MetadataEvent::SetInfo(v) => Self::SetInfo(v.into()),
            MetadataEvent::SetLicense(v) => Self::SetLicense(v.into()),
            MetadataEvent::SetDataSchema(v) => Self::SetDataSchema(v.into()),
            MetadataEvent::AddPushSource(v) => Self::AddPushSource(v.into()),
            MetadataEvent::DisablePushSource(v) => Self::DisablePushSource(v.into()),
            MetadataEvent::DisablePollingSource(v) => Self::DisablePollingSource(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MqttQos
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqttqos-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum MqttQos {
    #[serde(alias = "atMostOnce", alias = "atmostonce")]
    AtMostOnce,
    #[serde(alias = "atLeastOnce", alias = "atleastonce")]
    AtLeastOnce,
    #[serde(alias = "exactlyOnce", alias = "exactlyonce")]
    ExactlyOnce,
}

impl IntoDto for MqttQos {
    type Dto = dtos::MqttQos;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::MqttQos> for MqttQos {
    fn from(v: dtos::MqttQos) -> Self {
        match v {
            dtos::MqttQos::AtMostOnce => Self::AtMostOnce,
            dtos::MqttQos::AtLeastOnce => Self::AtLeastOnce,
            dtos::MqttQos::ExactlyOnce => Self::ExactlyOnce,
        }
    }
}

impl From<MqttQos> for dtos::MqttQos {
    fn from(v: MqttQos) -> Self {
        match v {
            MqttQos::AtMostOnce => Self::AtMostOnce,
            MqttQos::AtLeastOnce => Self::AtLeastOnce,
            MqttQos::ExactlyOnce => Self::ExactlyOnce,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MqttTopicSubscription
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqtttopicsubscription-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MqttTopicSubscription {
    pub path: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qos: Option<MqttQos>,
}

impl IntoDto for MqttTopicSubscription {
    type Dto = dtos::MqttTopicSubscription;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::MqttTopicSubscription> for MqttTopicSubscription {
    fn from(v: dtos::MqttTopicSubscription) -> Self {
        Self {
            path: v.path,
            qos: v.qos.map(|v| v.into()),
        }
    }
}

impl From<MqttTopicSubscription> for dtos::MqttTopicSubscription {
    fn from(v: MqttTopicSubscription) -> Self {
        Self {
            path: v.path,
            qos: v.qos.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct OffsetInterval {
    pub start: u64,
    pub end: u64,
}

impl IntoDto for OffsetInterval {
    type Dto = dtos::OffsetInterval;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::OffsetInterval> for OffsetInterval {
    fn from(v: dtos::OffsetInterval) -> Self {
        Self {
            start: v.start,
            end: v.end,
        }
    }
}

impl From<OffsetInterval> for dtos::OffsetInterval {
    fn from(v: OffsetInterval) -> Self {
        Self {
            start: v.start,
            end: v.end,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PrepStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum PrepStep {
    #[serde(alias = "decompress")]
    Decompress(PrepStepDecompress),
    #[serde(alias = "pipe")]
    Pipe(PrepStepPipe),
}

impl IntoDto for PrepStep {
    type Dto = dtos::PrepStep;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::PrepStep> for PrepStep {
    fn from(v: dtos::PrepStep) -> Self {
        match v {
            dtos::PrepStep::Decompress(v) => Self::Decompress(v.into()),
            dtos::PrepStep::Pipe(v) => Self::Pipe(v.into()),
        }
    }
}

impl From<PrepStep> for dtos::PrepStep {
    fn from(v: PrepStep) -> Self {
        match v {
            PrepStep::Decompress(v) => Self::Decompress(v.into()),
            PrepStep::Pipe(v) => Self::Pipe(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PrepStepDecompress
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstepdecompress-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct PrepStepDecompress {
    pub format: CompressionFormat,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
}

impl IntoDto for PrepStepDecompress {
    type Dto = dtos::PrepStepDecompress;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::PrepStepDecompress> for PrepStepDecompress {
    fn from(v: dtos::PrepStepDecompress) -> Self {
        Self {
            format: v.format.into(),
            sub_path: v.sub_path.map(|v| v),
        }
    }
}

impl From<PrepStepDecompress> for dtos::PrepStepDecompress {
    fn from(v: PrepStepDecompress) -> Self {
        Self {
            format: v.format.into(),
            sub_path: v.sub_path.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PrepStepPipe
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepsteppipe-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct PrepStepPipe {
    pub command: Vec<String>,
}

impl IntoDto for PrepStepPipe {
    type Dto = dtos::PrepStepPipe;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::PrepStepPipe> for PrepStepPipe {
    fn from(v: dtos::PrepStepPipe) -> Self {
        Self {
            command: v.command.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<PrepStepPipe> for dtos::PrepStepPipe {
    fn from(v: PrepStepPipe) -> Self {
        Self {
            command: v.command.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryrequest-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct RawQueryRequest {
    pub input_data_paths: Vec<PathBuf>,
    pub transform: Transform,
    pub output_data_path: PathBuf,
}

impl IntoDto for RawQueryRequest {
    type Dto = dtos::RawQueryRequest;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::RawQueryRequest> for RawQueryRequest {
    fn from(v: dtos::RawQueryRequest) -> Self {
        Self {
            input_data_paths: v.input_data_paths.into_iter().map(Into::into).collect(),
            transform: v.transform.into(),
            output_data_path: v.output_data_path,
        }
    }
}

impl From<RawQueryRequest> for dtos::RawQueryRequest {
    fn from(v: RawQueryRequest) -> Self {
        Self {
            input_data_paths: v.input_data_paths.into_iter().map(Into::into).collect(),
            transform: v.transform.into(),
            output_data_path: v.output_data_path,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponse-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum RawQueryResponse {
    #[serde(alias = "progress")]
    Progress(RawQueryResponseProgress),
    #[serde(alias = "success")]
    Success(RawQueryResponseSuccess),
    #[serde(alias = "invalidQuery", alias = "invalidquery")]
    InvalidQuery(RawQueryResponseInvalidQuery),
    #[serde(alias = "internalError", alias = "internalerror")]
    InternalError(RawQueryResponseInternalError),
}

impl IntoDto for RawQueryResponse {
    type Dto = dtos::RawQueryResponse;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::RawQueryResponse> for RawQueryResponse {
    fn from(v: dtos::RawQueryResponse) -> Self {
        match v {
            dtos::RawQueryResponse::Progress(v) => Self::Progress(v.into()),
            dtos::RawQueryResponse::Success(v) => Self::Success(v.into()),
            dtos::RawQueryResponse::InvalidQuery(v) => Self::InvalidQuery(v.into()),
            dtos::RawQueryResponse::InternalError(v) => Self::InternalError(v.into()),
        }
    }
}

impl From<RawQueryResponse> for dtos::RawQueryResponse {
    fn from(v: RawQueryResponse) -> Self {
        match v {
            RawQueryResponse::Progress(v) => Self::Progress(v.into()),
            RawQueryResponse::Success(v) => Self::Success(v.into()),
            RawQueryResponse::InvalidQuery(v) => Self::InvalidQuery(v.into()),
            RawQueryResponse::InternalError(v) => Self::InternalError(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponseInternalError
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponseinternalerror-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct RawQueryResponseInternalError {
    pub message: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<String>,
}

impl IntoDto for RawQueryResponseInternalError {
    type Dto = dtos::RawQueryResponseInternalError;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::RawQueryResponseInternalError> for RawQueryResponseInternalError {
    fn from(v: dtos::RawQueryResponseInternalError) -> Self {
        Self {
            message: v.message,
            backtrace: v.backtrace.map(|v| v),
        }
    }
}

impl From<RawQueryResponseInternalError> for dtos::RawQueryResponseInternalError {
    fn from(v: RawQueryResponseInternalError) -> Self {
        Self {
            message: v.message,
            backtrace: v.backtrace.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponseInvalidQuery
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponseinvalidquery-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct RawQueryResponseInvalidQuery {
    pub message: String,
}

impl IntoDto for RawQueryResponseInvalidQuery {
    type Dto = dtos::RawQueryResponseInvalidQuery;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::RawQueryResponseInvalidQuery> for RawQueryResponseInvalidQuery {
    fn from(v: dtos::RawQueryResponseInvalidQuery) -> Self {
        Self { message: v.message }
    }
}

impl From<RawQueryResponseInvalidQuery> for dtos::RawQueryResponseInvalidQuery {
    fn from(v: RawQueryResponseInvalidQuery) -> Self {
        Self { message: v.message }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponseProgress
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponseprogress-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct RawQueryResponseProgress {}

impl IntoDto for RawQueryResponseProgress {
    type Dto = dtos::RawQueryResponseProgress;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::RawQueryResponseProgress> for RawQueryResponseProgress {
    fn from(v: dtos::RawQueryResponseProgress) -> Self {
        Self {}
    }
}

impl From<RawQueryResponseProgress> for dtos::RawQueryResponseProgress {
    fn from(v: RawQueryResponseProgress) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponseSuccess
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponsesuccess-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct RawQueryResponseSuccess {
    pub num_records: u64,
}

impl IntoDto for RawQueryResponseSuccess {
    type Dto = dtos::RawQueryResponseSuccess;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::RawQueryResponseSuccess> for RawQueryResponseSuccess {
    fn from(v: dtos::RawQueryResponseSuccess) -> Self {
        Self {
            num_records: v.num_records,
        }
    }
}

impl From<RawQueryResponseSuccess> for dtos::RawQueryResponseSuccess {
    fn from(v: RawQueryResponseSuccess) -> Self {
        Self {
            num_records: v.num_records,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum ReadStep {
    #[serde(alias = "csv")]
    Csv(ReadStepCsv),
    #[serde(alias = "geoJson", alias = "geojson")]
    GeoJson(ReadStepGeoJson),
    #[serde(alias = "esriShapefile", alias = "esrishapefile")]
    EsriShapefile(ReadStepEsriShapefile),
    #[serde(alias = "parquet")]
    Parquet(ReadStepParquet),
    #[serde(alias = "json")]
    Json(ReadStepJson),
    #[serde(alias = "ndJson", alias = "ndjson")]
    NdJson(ReadStepNdJson),
    #[serde(alias = "ndGeoJson", alias = "ndgeojson")]
    NdGeoJson(ReadStepNdGeoJson),
}

impl IntoDto for ReadStep {
    type Dto = dtos::ReadStep;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ReadStep> for ReadStep {
    fn from(v: dtos::ReadStep) -> Self {
        match v {
            dtos::ReadStep::Csv(v) => Self::Csv(v.into()),
            dtos::ReadStep::GeoJson(v) => Self::GeoJson(v.into()),
            dtos::ReadStep::EsriShapefile(v) => Self::EsriShapefile(v.into()),
            dtos::ReadStep::Parquet(v) => Self::Parquet(v.into()),
            dtos::ReadStep::Json(v) => Self::Json(v.into()),
            dtos::ReadStep::NdJson(v) => Self::NdJson(v.into()),
            dtos::ReadStep::NdGeoJson(v) => Self::NdGeoJson(v.into()),
        }
    }
}

impl From<ReadStep> for dtos::ReadStep {
    fn from(v: ReadStep) -> Self {
        match v {
            ReadStep::Csv(v) => Self::Csv(v.into()),
            ReadStep::GeoJson(v) => Self::GeoJson(v.into()),
            ReadStep::EsriShapefile(v) => Self::EsriShapefile(v.into()),
            ReadStep::Parquet(v) => Self::Parquet(v.into()),
            ReadStep::Json(v) => Self::Json(v.into()),
            ReadStep::NdJson(v) => Self::NdJson(v.into()),
            ReadStep::NdGeoJson(v) => Self::NdGeoJson(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepCsv
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepcsv-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ReadStepCsv {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ddl_schema: Option<Vec<String>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub separator: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub escape: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header: Option<bool>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub infer_schema: Option<bool>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_value: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_format: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_format: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<DataSchema>,
}

impl IntoDto for ReadStepCsv {
    type Dto = dtos::ReadStepCsv;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ReadStepCsv> for ReadStepCsv {
    fn from(v: dtos::ReadStepCsv) -> Self {
        Self {
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            separator: v.separator.map(|v| v),
            encoding: v.encoding.map(|v| v),
            quote: v.quote.map(|v| v),
            escape: v.escape.map(|v| v),
            header: v.header.map(|v| v),
            infer_schema: v.infer_schema.map(|v| v),
            null_value: v.null_value.map(|v| v),
            date_format: v.date_format.map(|v| v),
            timestamp_format: v.timestamp_format.map(|v| v),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

impl From<ReadStepCsv> for dtos::ReadStepCsv {
    fn from(v: ReadStepCsv) -> Self {
        Self {
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            separator: v.separator.map(|v| v),
            encoding: v.encoding.map(|v| v),
            quote: v.quote.map(|v| v),
            escape: v.escape.map(|v| v),
            header: v.header.map(|v| v),
            infer_schema: v.infer_schema.map(|v| v),
            null_value: v.null_value.map(|v| v),
            date_format: v.date_format.map(|v| v),
            timestamp_format: v.timestamp_format.map(|v| v),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepEsriShapefile
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepesrishapefile-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ReadStepEsriShapefile {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ddl_schema: Option<Vec<String>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<DataSchema>,
}

impl IntoDto for ReadStepEsriShapefile {
    type Dto = dtos::ReadStepEsriShapefile;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ReadStepEsriShapefile> for ReadStepEsriShapefile {
    fn from(v: dtos::ReadStepEsriShapefile) -> Self {
        Self {
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            sub_path: v.sub_path.map(|v| v),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

impl From<ReadStepEsriShapefile> for dtos::ReadStepEsriShapefile {
    fn from(v: ReadStepEsriShapefile) -> Self {
        Self {
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            sub_path: v.sub_path.map(|v| v),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepGeoJson
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepgeojson-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ReadStepGeoJson {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ddl_schema: Option<Vec<String>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<DataSchema>,
}

impl IntoDto for ReadStepGeoJson {
    type Dto = dtos::ReadStepGeoJson;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ReadStepGeoJson> for ReadStepGeoJson {
    fn from(v: dtos::ReadStepGeoJson) -> Self {
        Self {
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

impl From<ReadStepGeoJson> for dtos::ReadStepGeoJson {
    fn from(v: ReadStepGeoJson) -> Self {
        Self {
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepJson
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepjson-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ReadStepJson {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ddl_schema: Option<Vec<String>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_format: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_format: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<DataSchema>,
}

impl IntoDto for ReadStepJson {
    type Dto = dtos::ReadStepJson;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ReadStepJson> for ReadStepJson {
    fn from(v: dtos::ReadStepJson) -> Self {
        Self {
            sub_path: v.sub_path.map(|v| v),
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            date_format: v.date_format.map(|v| v),
            encoding: v.encoding.map(|v| v),
            timestamp_format: v.timestamp_format.map(|v| v),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

impl From<ReadStepJson> for dtos::ReadStepJson {
    fn from(v: ReadStepJson) -> Self {
        Self {
            sub_path: v.sub_path.map(|v| v),
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            date_format: v.date_format.map(|v| v),
            encoding: v.encoding.map(|v| v),
            timestamp_format: v.timestamp_format.map(|v| v),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepNdGeoJson
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepndgeojson-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ReadStepNdGeoJson {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ddl_schema: Option<Vec<String>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<DataSchema>,
}

impl IntoDto for ReadStepNdGeoJson {
    type Dto = dtos::ReadStepNdGeoJson;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ReadStepNdGeoJson> for ReadStepNdGeoJson {
    fn from(v: dtos::ReadStepNdGeoJson) -> Self {
        Self {
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

impl From<ReadStepNdGeoJson> for dtos::ReadStepNdGeoJson {
    fn from(v: ReadStepNdGeoJson) -> Self {
        Self {
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepNdJson
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepndjson-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ReadStepNdJson {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ddl_schema: Option<Vec<String>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_format: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_format: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<DataSchema>,
}

impl IntoDto for ReadStepNdJson {
    type Dto = dtos::ReadStepNdJson;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ReadStepNdJson> for ReadStepNdJson {
    fn from(v: dtos::ReadStepNdJson) -> Self {
        Self {
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            date_format: v.date_format.map(|v| v),
            encoding: v.encoding.map(|v| v),
            timestamp_format: v.timestamp_format.map(|v| v),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

impl From<ReadStepNdJson> for dtos::ReadStepNdJson {
    fn from(v: ReadStepNdJson) -> Self {
        Self {
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            date_format: v.date_format.map(|v| v),
            encoding: v.encoding.map(|v| v),
            timestamp_format: v.timestamp_format.map(|v| v),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStepParquet
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepparquet-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ReadStepParquet {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ddl_schema: Option<Vec<String>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<DataSchema>,
}

impl IntoDto for ReadStepParquet {
    type Dto = dtos::ReadStepParquet;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ReadStepParquet> for ReadStepParquet {
    fn from(v: dtos::ReadStepParquet) -> Self {
        Self {
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

impl From<ReadStepParquet> for dtos::ReadStepParquet {
    fn from(v: ReadStepParquet) -> Self {
        Self {
            ddl_schema: v
                .ddl_schema
                .map(|v| v.into_iter().map(Into::into).collect()),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RequestHeader
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#requestheader-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct RequestHeader {
    pub name: String,
    pub value: String,
}

impl IntoDto for RequestHeader {
    type Dto = dtos::RequestHeader;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::RequestHeader> for RequestHeader {
    fn from(v: dtos::RequestHeader) -> Self {
        Self {
            name: v.name,
            value: v.value,
        }
    }
}

impl From<RequestHeader> for dtos::RequestHeader {
    fn from(v: RequestHeader) -> Self {
        Self {
            name: v.name,
            value: v.value,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Resource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#resource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Resource<SpecT> {
    pub context: String,
    pub kind: String,
    pub header: ResourceHeader,
    pub spec: SpecT,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ResourceStatus>,
}

impl<SpecT> IntoDto for Resource<SpecT>
where
    SpecT: IntoDto,
    <SpecT as IntoDto>::Dto: From<SpecT>,
{
    type Dto = dtos::Resource<SpecT::Dto>;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl<SpecTFrom, SpecTTo> From<dtos::Resource<SpecTFrom>> for Resource<SpecTTo>
where
    SpecTTo: From<SpecTFrom>,
{
    fn from(v: dtos::Resource<SpecTFrom>) -> Self {
        Self {
            context: v.context,
            kind: v.kind,
            header: v.header.into(),
            spec: v.spec.into(),
            status: v.status.map(|v| v.into()),
        }
    }
}

impl<SpecTFrom, SpecTTo> From<Resource<SpecTFrom>> for dtos::Resource<SpecTTo>
where
    SpecTTo: From<SpecTFrom>,
{
    fn from(v: Resource<SpecTFrom>) -> Self {
        Self {
            context: v.context,
            kind: v.kind,
            header: v.header.into(),
            spec: v.spec.into(),
            status: v.status.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceAnnotations
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#resourceannotations-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceAnnotations {
    #[serde(flatten)]
    #[serde(with = "map_value_limited_precision")]
    pub entries: std::collections::BTreeMap<String, serde_json::Value>,
}

impl IntoDto for ResourceAnnotations {
    type Dto = dtos::ResourceAnnotations;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ResourceAnnotations> for ResourceAnnotations {
    fn from(v: dtos::ResourceAnnotations) -> Self {
        Self { entries: v.entries }
    }
}

impl From<ResourceAnnotations> for dtos::ResourceAnnotations {
    fn from(v: ResourceAnnotations) -> Self {
        Self { entries: v.entries }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceCondition
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#resourcecondition-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ResourceCondition {
    pub value: serde_json::Value,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "datetime_rfc3339_opt")]
    pub last_transition_time: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<u64>,
}

impl IntoDto for ResourceCondition {
    type Dto = dtos::ResourceCondition;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ResourceCondition> for ResourceCondition {
    fn from(v: dtos::ResourceCondition) -> Self {
        Self {
            value: v.value,
            reason: v.reason.map(|v| v),
            message: v.message.map(|v| v),
            last_transition_time: v.last_transition_time.map(|v| v),
            observed_generation: v.observed_generation.map(|v| v),
        }
    }
}

impl From<ResourceCondition> for dtos::ResourceCondition {
    fn from(v: ResourceCondition) -> Self {
        Self {
            value: v.value,
            reason: v.reason.map(|v| v),
            message: v.message.map(|v| v),
            last_transition_time: v.last_transition_time.map(|v| v),
            observed_generation: v.observed_generation.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceConditions
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#resourceconditions-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceConditions {
    #[serde(flatten)]
    pub entries: std::collections::BTreeMap<String, ResourceCondition>,
}

impl IntoDto for ResourceConditions {
    type Dto = dtos::ResourceConditions;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ResourceConditions> for ResourceConditions {
    fn from(v: dtos::ResourceConditions) -> Self {
        Self {
            entries: v.entries.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

impl From<ResourceConditions> for dtos::ResourceConditions {
    fn from(v: ResourceConditions) -> Self {
        Self {
            entries: v.entries.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceHeader
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#resourceheader-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ResourceHeader {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<StructOrString<AccountRef>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<ResourceLabels>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<ResourceAnnotations>,
}

impl IntoDto for ResourceHeader {
    type Dto = dtos::ResourceHeader;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ResourceHeader> for ResourceHeader {
    fn from(v: dtos::ResourceHeader) -> Self {
        Self {
            id: v.id.map(|v| v),
            name: v.name,
            account: v.account.map(|v| v.into()),
            labels: v.labels.map(|v| v.into()),
            annotations: v.annotations.map(|v| v.into()),
        }
    }
}

impl From<ResourceHeader> for dtos::ResourceHeader {
    fn from(v: ResourceHeader) -> Self {
        Self {
            id: v.id.map(|v| v),
            name: v.name,
            account: v.account.map(|v| v.into()),
            labels: v.labels.map(|v| v.into()),
            annotations: v.annotations.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceLabels
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#resourcelabels-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceLabels {
    #[serde(flatten)]
    #[serde(with = "map_value_limited_precision")]
    pub entries: std::collections::BTreeMap<String, serde_json::Value>,
}

impl IntoDto for ResourceLabels {
    type Dto = dtos::ResourceLabels;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ResourceLabels> for ResourceLabels {
    fn from(v: dtos::ResourceLabels) -> Self {
        Self { entries: v.entries }
    }
}

impl From<ResourceLabels> for dtos::ResourceLabels {
    fn from(v: ResourceLabels) -> Self {
        Self { entries: v.entries }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourcePhase
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#resourcephase-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ResourcePhase {
    #[serde(alias = "pending")]
    Pending,
    #[serde(alias = "reconciling")]
    Reconciling,
    #[serde(alias = "ready")]
    Ready,
    #[serde(alias = "degraded")]
    Degraded,
    #[serde(alias = "failed")]
    Failed,
}

impl IntoDto for ResourcePhase {
    type Dto = dtos::ResourcePhase;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ResourcePhase> for ResourcePhase {
    fn from(v: dtos::ResourcePhase) -> Self {
        match v {
            dtos::ResourcePhase::Pending => Self::Pending,
            dtos::ResourcePhase::Reconciling => Self::Reconciling,
            dtos::ResourcePhase::Ready => Self::Ready,
            dtos::ResourcePhase::Degraded => Self::Degraded,
            dtos::ResourcePhase::Failed => Self::Failed,
        }
    }
}

impl From<ResourcePhase> for dtos::ResourcePhase {
    fn from(v: ResourcePhase) -> Self {
        match v {
            ResourcePhase::Pending => Self::Pending,
            ResourcePhase::Reconciling => Self::Reconciling,
            ResourcePhase::Ready => Self::Ready,
            ResourcePhase::Degraded => Self::Degraded,
            ResourcePhase::Failed => Self::Failed,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceRef
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#resourceref-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRef {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<StructOrString<AccountRef>>,
}

impl IntoDto for ResourceRef {
    type Dto = dtos::ResourceRef;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ResourceRef> for ResourceRef {
    fn from(v: dtos::ResourceRef) -> Self {
        Self {
            id: v.id.map(|v| v),
            name: v.name.map(|v| v),
            account: v.account.map(|v| v.into()),
        }
    }
}

impl From<ResourceRef> for dtos::ResourceRef {
    fn from(v: ResourceRef) -> Self {
        Self {
            id: v.id.map(|v| v),
            name: v.name.map(|v| v),
            account: v.account.map(|v| v.into()),
        }
    }
}

impl From<dtos::ResourceRef> for StructOrString<ResourceRef> {
    fn from(v: dtos::ResourceRef) -> Self {
        Self(v.into())
    }
}
impl From<StructOrString<ResourceRef>> for dtos::ResourceRef {
    fn from(v: StructOrString<ResourceRef>) -> Self {
        v.0.into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceStatus
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#resourcestatus-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct ResourceStatus {
    pub phase: ResourcePhase,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<u64>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<ResourceConditions>,
}

impl IntoDto for ResourceStatus {
    type Dto = dtos::ResourceStatus;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::ResourceStatus> for ResourceStatus {
    fn from(v: dtos::ResourceStatus) -> Self {
        Self {
            phase: v.phase.into(),
            observed_generation: v.observed_generation.map(|v| v),
            conditions: v.conditions.map(|v| v.into()),
        }
    }
}

impl From<ResourceStatus> for dtos::ResourceStatus {
    fn from(v: ResourceStatus) -> Self {
        Self {
            phase: v.phase.into(),
            observed_generation: v.observed_generation.map(|v| v),
            conditions: v.conditions.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Secret
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#secret-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Secret {
    pub value: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
}

impl IntoDto for Secret {
    type Dto = dtos::Secret;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::Secret> for Secret {
    fn from(v: dtos::Secret) -> Self {
        Self {
            value: v.value,
            content_encoding: v.content_encoding.map(|v| v),
        }
    }
}

impl From<Secret> for dtos::Secret {
    fn from(v: Secret) -> Self {
        Self {
            value: v.value,
            content_encoding: v.content_encoding.map(|v| v),
        }
    }
}

impl From<dtos::Secret> for StructOrString<Secret> {
    fn from(v: dtos::Secret) -> Self {
        Self(v.into())
    }
}
impl From<StructOrString<Secret>> for dtos::Secret {
    fn from(v: StructOrString<Secret>) -> Self {
        v.0.into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SecretSet
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#secretset-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SecretSet {
    pub secrets: Secrets,
}

impl IntoDto for SecretSet {
    type Dto = dtos::SecretSet;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SecretSet> for SecretSet {
    fn from(v: dtos::SecretSet) -> Self {
        Self {
            secrets: v.secrets.into(),
        }
    }
}

impl From<SecretSet> for dtos::SecretSet {
    fn from(v: SecretSet) -> Self {
        Self {
            secrets: v.secrets.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Secrets
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#secrets-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct Secrets {
    #[serde(flatten)]
    pub entries: std::collections::BTreeMap<String, StructOrString<Secret>>,
}

impl IntoDto for Secrets {
    type Dto = dtos::Secrets;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::Secrets> for Secrets {
    fn from(v: dtos::Secrets) -> Self {
        Self {
            entries: v.entries.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

impl From<Secrets> for dtos::Secrets {
    fn from(v: Secrets) -> Self {
        Self {
            entries: v.entries.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Seed
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Seed {
    pub dataset_id: DatasetID,
    pub dataset_kind: DatasetKind,
}

impl IntoDto for Seed {
    type Dto = dtos::Seed;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::Seed> for Seed {
    fn from(v: dtos::Seed) -> Self {
        Self {
            dataset_id: v.dataset_id,
            dataset_kind: v.dataset_kind.into(),
        }
    }
}

impl From<Seed> for dtos::Seed {
    fn from(v: Seed) -> Self {
        Self {
            dataset_id: v.dataset_id,
            dataset_kind: v.dataset_kind.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetAttachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setattachments-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SetAttachments {
    pub attachments: Attachments,
}

impl IntoDto for SetAttachments {
    type Dto = dtos::SetAttachments;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SetAttachments> for SetAttachments {
    fn from(v: dtos::SetAttachments) -> Self {
        Self {
            attachments: v.attachments.into(),
        }
    }
}

impl From<SetAttachments> for dtos::SetAttachments {
    fn from(v: SetAttachments) -> Self {
        Self {
            attachments: v.attachments.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetDataSchema
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setdataschema-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SetDataSchema {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "base64_opt")]
    pub raw_arrow_schema: Option<Vec<u8>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<DataSchema>,
}

impl IntoDto for SetDataSchema {
    type Dto = dtos::SetDataSchema;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SetDataSchema> for SetDataSchema {
    fn from(v: dtos::SetDataSchema) -> Self {
        Self {
            raw_arrow_schema: v.raw_arrow_schema.map(|v| v),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

impl From<SetDataSchema> for dtos::SetDataSchema {
    fn from(v: SetDataSchema) -> Self {
        Self {
            raw_arrow_schema: v.raw_arrow_schema.map(|v| v),
            schema: v.schema.map(|v| v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetInfo
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setinfo-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SetInfo {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keywords: Option<Vec<String>>,
}

impl IntoDto for SetInfo {
    type Dto = dtos::SetInfo;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SetInfo> for SetInfo {
    fn from(v: dtos::SetInfo) -> Self {
        Self {
            description: v.description.map(|v| v),
            keywords: v.keywords.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

impl From<SetInfo> for dtos::SetInfo {
    fn from(v: SetInfo) -> Self {
        Self {
            description: v.description.map(|v| v),
            keywords: v.keywords.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetLicense
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setlicense-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SetLicense {
    pub short_name: String,
    pub name: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spdx_id: Option<String>,
    pub website_url: String,
}

impl IntoDto for SetLicense {
    type Dto = dtos::SetLicense;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SetLicense> for SetLicense {
    fn from(v: dtos::SetLicense) -> Self {
        Self {
            short_name: v.short_name,
            name: v.name,
            spdx_id: v.spdx_id.map(|v| v),
            website_url: v.website_url,
        }
    }
}

impl From<SetLicense> for dtos::SetLicense {
    fn from(v: SetLicense) -> Self {
        Self {
            short_name: v.short_name,
            name: v.name,
            spdx_id: v.spdx_id.map(|v| v),
            website_url: v.website_url,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetPollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SetPollingSource {
    pub fetch: FetchStep,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prepare: Option<Vec<PrepStep>>,
    pub read: ReadStep,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preprocess: Option<Transform>,
    pub merge: MergeStrategy,
}

impl IntoDto for SetPollingSource {
    type Dto = dtos::SetPollingSource;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SetPollingSource> for SetPollingSource {
    fn from(v: dtos::SetPollingSource) -> Self {
        Self {
            fetch: v.fetch.into(),
            prepare: v.prepare.map(|v| v.into_iter().map(Into::into).collect()),
            read: v.read.into(),
            preprocess: v.preprocess.map(|v| v.into()),
            merge: v.merge.into(),
        }
    }
}

impl From<SetPollingSource> for dtos::SetPollingSource {
    fn from(v: SetPollingSource) -> Self {
        Self {
            fetch: v.fetch.into(),
            prepare: v.prepare.map(|v| v.into_iter().map(Into::into).collect()),
            read: v.read.into(),
            preprocess: v.preprocess.map(|v| v.into()),
            merge: v.merge.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SetTransform {
    pub inputs: Vec<TransformInput>,
    pub transform: Transform,
}

impl IntoDto for SetTransform {
    type Dto = dtos::SetTransform;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SetTransform> for SetTransform {
    fn from(v: dtos::SetTransform) -> Self {
        Self {
            inputs: v.inputs.into_iter().map(Into::into).collect(),
            transform: v.transform.into(),
        }
    }
}

impl From<SetTransform> for dtos::SetTransform {
    fn from(v: SetTransform) -> Self {
        Self {
            inputs: v.inputs.into_iter().map(Into::into).collect(),
            transform: v.transform.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetVocab
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SetVocab {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_column: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_type_column: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_time_column: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_time_column: Option<String>,
}

impl IntoDto for SetVocab {
    type Dto = dtos::SetVocab;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SetVocab> for SetVocab {
    fn from(v: dtos::SetVocab) -> Self {
        Self {
            offset_column: v.offset_column.map(|v| v),
            operation_type_column: v.operation_type_column.map(|v| v),
            system_time_column: v.system_time_column.map(|v| v),
            event_time_column: v.event_time_column.map(|v| v),
        }
    }
}

impl From<SetVocab> for dtos::SetVocab {
    fn from(v: SetVocab) -> Self {
        Self {
            offset_column: v.offset_column.map(|v| v),
            operation_type_column: v.operation_type_column.map(|v| v),
            system_time_column: v.system_time_column.map(|v| v),
            event_time_column: v.event_time_column.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum SourceCaching {
    #[serde(alias = "forever")]
    Forever(SourceCachingForever),
}

impl From<dtos::SourceCaching> for UnionOrString<SourceCaching> {
    fn from(v: dtos::SourceCaching) -> Self {
        Self(v.into())
    }
}
impl From<UnionOrString<SourceCaching>> for dtos::SourceCaching {
    fn from(v: UnionOrString<SourceCaching>) -> Self {
        v.0.into()
    }
}

impl IntoDto for SourceCaching {
    type Dto = dtos::SourceCaching;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SourceCaching> for SourceCaching {
    fn from(v: dtos::SourceCaching) -> Self {
        match v {
            dtos::SourceCaching::Forever(v) => Self::Forever(v.into()),
        }
    }
}

impl From<SourceCaching> for dtos::SourceCaching {
    fn from(v: SourceCaching) -> Self {
        match v {
            SourceCaching::Forever(v) => Self::Forever(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceCachingForever
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecachingforever-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SourceCachingForever {}

impl IntoDto for SourceCachingForever {
    type Dto = dtos::SourceCachingForever;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SourceCachingForever> for SourceCachingForever {
    fn from(v: dtos::SourceCachingForever) -> Self {
        Self {}
    }
}

impl From<SourceCachingForever> for dtos::SourceCachingForever {
    fn from(v: SourceCachingForever) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceOrdering
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourceordering-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SourceOrdering {
    #[serde(alias = "byEventTime", alias = "byeventtime")]
    ByEventTime,
    #[serde(alias = "byName", alias = "byname")]
    ByName,
}

impl IntoDto for SourceOrdering {
    type Dto = dtos::SourceOrdering;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SourceOrdering> for SourceOrdering {
    fn from(v: dtos::SourceOrdering) -> Self {
        match v {
            dtos::SourceOrdering::ByEventTime => Self::ByEventTime,
            dtos::SourceOrdering::ByName => Self::ByName,
        }
    }
}

impl From<SourceOrdering> for dtos::SourceOrdering {
    fn from(v: SourceOrdering) -> Self {
        match v {
            SourceOrdering::ByEventTime => Self::ByEventTime,
            SourceOrdering::ByName => Self::ByName,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceState
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcestate-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SourceState {
    pub source_name: String,
    pub kind: String,
    pub value: String,
}

impl IntoDto for SourceState {
    type Dto = dtos::SourceState;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SourceState> for SourceState {
    fn from(v: dtos::SourceState) -> Self {
        Self {
            source_name: v.source_name,
            kind: v.kind,
            value: v.value,
        }
    }
}

impl From<SourceState> for dtos::SourceState {
    fn from(v: SourceState) -> Self {
        Self {
            source_name: v.source_name,
            kind: v.kind,
            value: v.value,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SqlQueryStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sqlquerystep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SqlQueryStep {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    pub query: String,
}

impl IntoDto for SqlQueryStep {
    type Dto = dtos::SqlQueryStep;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::SqlQueryStep> for SqlQueryStep {
    fn from(v: dtos::SqlQueryStep) -> Self {
        Self {
            alias: v.alias.map(|v| v),
            query: v.query,
        }
    }
}

impl From<SqlQueryStep> for dtos::SqlQueryStep {
    fn from(v: SqlQueryStep) -> Self {
        Self {
            alias: v.alias.map(|v| v),
            query: v.query,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TemporalTable {
    pub name: String,
    pub primary_key: Vec<String>,
}

impl IntoDto for TemporalTable {
    type Dto = dtos::TemporalTable;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::TemporalTable> for TemporalTable {
    fn from(v: dtos::TemporalTable) -> Self {
        Self {
            name: v.name,
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<TemporalTable> for dtos::TemporalTable {
    fn from(v: TemporalTable) -> Self {
        Self {
            name: v.name,
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TimeUnit
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#timeunit-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum TimeUnit {
    #[serde(alias = "second")]
    Second,
    #[serde(alias = "millisecond")]
    Millisecond,
    #[serde(alias = "microsecond")]
    Microsecond,
    #[serde(alias = "nanosecond")]
    Nanosecond,
}

impl IntoDto for TimeUnit {
    type Dto = dtos::TimeUnit;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::TimeUnit> for TimeUnit {
    fn from(v: dtos::TimeUnit) -> Self {
        match v {
            dtos::TimeUnit::Second => Self::Second,
            dtos::TimeUnit::Millisecond => Self::Millisecond,
            dtos::TimeUnit::Microsecond => Self::Microsecond,
            dtos::TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

impl From<TimeUnit> for dtos::TimeUnit {
    fn from(v: TimeUnit) -> Self {
        match v {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum Transform {
    #[serde(alias = "sql")]
    Sql(TransformSql),
}

impl IntoDto for Transform {
    type Dto = dtos::Transform;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::Transform> for Transform {
    fn from(v: dtos::Transform) -> Self {
        match v {
            dtos::Transform::Sql(v) => Self::Sql(v.into()),
        }
    }
}

impl From<Transform> for dtos::Transform {
    fn from(v: Transform) -> Self {
        match v {
            Transform::Sql(v) => Self::Sql(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformSql
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformsql-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TransformSql {
    pub engine: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queries: Option<Vec<SqlQueryStep>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temporal_tables: Option<Vec<TemporalTable>>,
}

impl IntoDto for TransformSql {
    type Dto = dtos::TransformSql;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::TransformSql> for TransformSql {
    fn from(v: dtos::TransformSql) -> Self {
        Self {
            engine: v.engine,
            version: v.version.map(|v| v),
            query: v.query.map(|v| v),
            queries: v.queries.map(|v| v.into_iter().map(Into::into).collect()),
            temporal_tables: v
                .temporal_tables
                .map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

impl From<TransformSql> for dtos::TransformSql {
    fn from(v: TransformSql) -> Self {
        Self {
            engine: v.engine,
            version: v.version.map(|v| v),
            query: v.query.map(|v| v),
            queries: v.queries.map(|v| v.into_iter().map(Into::into).collect()),
            temporal_tables: v
                .temporal_tables
                .map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TransformInput {
    pub dataset_ref: DatasetRef,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
}

impl IntoDto for TransformInput {
    type Dto = dtos::TransformInput;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::TransformInput> for TransformInput {
    fn from(v: dtos::TransformInput) -> Self {
        Self {
            dataset_ref: v.dataset_ref,
            alias: v.alias.map(|v| v),
        }
    }
}

impl From<TransformInput> for dtos::TransformInput {
    fn from(v: TransformInput) -> Self {
        Self {
            dataset_ref: v.dataset_ref,
            alias: v.alias.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequest-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TransformRequest {
    pub dataset_id: DatasetID,
    pub dataset_alias: DatasetAlias,
    #[serde(with = "datetime_rfc3339")]
    pub system_time: DateTime<Utc>,
    pub vocab: DatasetVocabulary,
    pub transform: Transform,
    pub query_inputs: Vec<TransformRequestInput>,
    pub next_offset: u64,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_checkpoint_path: Option<PathBuf>,
    pub new_checkpoint_path: PathBuf,
    pub new_data_path: PathBuf,
}

impl IntoDto for TransformRequest {
    type Dto = dtos::TransformRequest;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::TransformRequest> for TransformRequest {
    fn from(v: dtos::TransformRequest) -> Self {
        Self {
            dataset_id: v.dataset_id,
            dataset_alias: v.dataset_alias,
            system_time: v.system_time,
            vocab: v.vocab.into(),
            transform: v.transform.into(),
            query_inputs: v.query_inputs.into_iter().map(Into::into).collect(),
            next_offset: v.next_offset,
            prev_checkpoint_path: v.prev_checkpoint_path.map(|v| v),
            new_checkpoint_path: v.new_checkpoint_path,
            new_data_path: v.new_data_path,
        }
    }
}

impl From<TransformRequest> for dtos::TransformRequest {
    fn from(v: TransformRequest) -> Self {
        Self {
            dataset_id: v.dataset_id,
            dataset_alias: v.dataset_alias,
            system_time: v.system_time,
            vocab: v.vocab.into(),
            transform: v.transform.into(),
            query_inputs: v.query_inputs.into_iter().map(Into::into).collect(),
            next_offset: v.next_offset,
            prev_checkpoint_path: v.prev_checkpoint_path.map(|v| v),
            new_checkpoint_path: v.new_checkpoint_path,
            new_data_path: v.new_data_path,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformRequestInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequestinput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TransformRequestInput {
    pub dataset_id: DatasetID,
    pub dataset_alias: DatasetAlias,
    pub query_alias: String,
    pub vocab: DatasetVocabulary,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_interval: Option<OffsetInterval>,
    pub data_paths: Vec<PathBuf>,
    pub schema_file: PathBuf,
    pub explicit_watermarks: Vec<Watermark>,
}

impl IntoDto for TransformRequestInput {
    type Dto = dtos::TransformRequestInput;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::TransformRequestInput> for TransformRequestInput {
    fn from(v: dtos::TransformRequestInput) -> Self {
        Self {
            dataset_id: v.dataset_id,
            dataset_alias: v.dataset_alias,
            query_alias: v.query_alias,
            vocab: v.vocab.into(),
            offset_interval: v.offset_interval.map(|v| v.into()),
            data_paths: v.data_paths.into_iter().map(Into::into).collect(),
            schema_file: v.schema_file,
            explicit_watermarks: v.explicit_watermarks.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<TransformRequestInput> for dtos::TransformRequestInput {
    fn from(v: TransformRequestInput) -> Self {
        Self {
            dataset_id: v.dataset_id,
            dataset_alias: v.dataset_alias,
            query_alias: v.query_alias,
            vocab: v.vocab.into(),
            offset_interval: v.offset_interval.map(|v| v.into()),
            data_paths: v.data_paths.into_iter().map(Into::into).collect(),
            schema_file: v.schema_file,
            explicit_watermarks: v.explicit_watermarks.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponse-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind")]
pub enum TransformResponse {
    #[serde(alias = "progress")]
    Progress(TransformResponseProgress),
    #[serde(alias = "success")]
    Success(TransformResponseSuccess),
    #[serde(alias = "invalidQuery", alias = "invalidquery")]
    InvalidQuery(TransformResponseInvalidQuery),
    #[serde(alias = "internalError", alias = "internalerror")]
    InternalError(TransformResponseInternalError),
}

impl IntoDto for TransformResponse {
    type Dto = dtos::TransformResponse;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::TransformResponse> for TransformResponse {
    fn from(v: dtos::TransformResponse) -> Self {
        match v {
            dtos::TransformResponse::Progress(v) => Self::Progress(v.into()),
            dtos::TransformResponse::Success(v) => Self::Success(v.into()),
            dtos::TransformResponse::InvalidQuery(v) => Self::InvalidQuery(v.into()),
            dtos::TransformResponse::InternalError(v) => Self::InternalError(v.into()),
        }
    }
}

impl From<TransformResponse> for dtos::TransformResponse {
    fn from(v: TransformResponse) -> Self {
        match v {
            TransformResponse::Progress(v) => Self::Progress(v.into()),
            TransformResponse::Success(v) => Self::Success(v.into()),
            TransformResponse::InvalidQuery(v) => Self::InvalidQuery(v.into()),
            TransformResponse::InternalError(v) => Self::InternalError(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponseInternalError
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponseinternalerror-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TransformResponseInternalError {
    pub message: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<String>,
}

impl IntoDto for TransformResponseInternalError {
    type Dto = dtos::TransformResponseInternalError;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::TransformResponseInternalError> for TransformResponseInternalError {
    fn from(v: dtos::TransformResponseInternalError) -> Self {
        Self {
            message: v.message,
            backtrace: v.backtrace.map(|v| v),
        }
    }
}

impl From<TransformResponseInternalError> for dtos::TransformResponseInternalError {
    fn from(v: TransformResponseInternalError) -> Self {
        Self {
            message: v.message,
            backtrace: v.backtrace.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponseInvalidQuery
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponseinvalidquery-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TransformResponseInvalidQuery {
    pub message: String,
}

impl IntoDto for TransformResponseInvalidQuery {
    type Dto = dtos::TransformResponseInvalidQuery;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::TransformResponseInvalidQuery> for TransformResponseInvalidQuery {
    fn from(v: dtos::TransformResponseInvalidQuery) -> Self {
        Self { message: v.message }
    }
}

impl From<TransformResponseInvalidQuery> for dtos::TransformResponseInvalidQuery {
    fn from(v: TransformResponseInvalidQuery) -> Self {
        Self { message: v.message }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponseProgress
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponseprogress-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TransformResponseProgress {}

impl IntoDto for TransformResponseProgress {
    type Dto = dtos::TransformResponseProgress;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::TransformResponseProgress> for TransformResponseProgress {
    fn from(v: dtos::TransformResponseProgress) -> Self {
        Self {}
    }
}

impl From<TransformResponseProgress> for dtos::TransformResponseProgress {
    fn from(v: TransformResponseProgress) -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponseSuccess
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponsesuccess-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TransformResponseSuccess {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_offset_interval: Option<OffsetInterval>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "datetime_rfc3339_opt")]
    pub new_watermark: Option<DateTime<Utc>>,
}

impl IntoDto for TransformResponseSuccess {
    type Dto = dtos::TransformResponseSuccess;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::TransformResponseSuccess> for TransformResponseSuccess {
    fn from(v: dtos::TransformResponseSuccess) -> Self {
        Self {
            new_offset_interval: v.new_offset_interval.map(|v| v.into()),
            new_watermark: v.new_watermark.map(|v| v),
        }
    }
}

impl From<TransformResponseSuccess> for dtos::TransformResponseSuccess {
    fn from(v: TransformResponseSuccess) -> Self {
        Self {
            new_offset_interval: v.new_offset_interval.map(|v| v.into()),
            new_watermark: v.new_watermark.map(|v| v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Variable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#variable-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Variable {
    pub value: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
}

impl IntoDto for Variable {
    type Dto = dtos::Variable;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::Variable> for Variable {
    fn from(v: dtos::Variable) -> Self {
        Self {
            value: v.value,
            content_encoding: v.content_encoding.map(|v| v),
        }
    }
}

impl From<Variable> for dtos::Variable {
    fn from(v: Variable) -> Self {
        Self {
            value: v.value,
            content_encoding: v.content_encoding.map(|v| v),
        }
    }
}

impl From<dtos::Variable> for StructOrString<Variable> {
    fn from(v: dtos::Variable) -> Self {
        Self(v.into())
    }
}
impl From<StructOrString<Variable>> for dtos::Variable {
    fn from(v: StructOrString<Variable>) -> Self {
        v.0.into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// VariableSet
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#variableset-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct VariableSet {
    pub variables: Variables,
}

impl IntoDto for VariableSet {
    type Dto = dtos::VariableSet;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::VariableSet> for VariableSet {
    fn from(v: dtos::VariableSet) -> Self {
        Self {
            variables: v.variables.into(),
        }
    }
}

impl From<VariableSet> for dtos::VariableSet {
    fn from(v: VariableSet) -> Self {
        Self {
            variables: v.variables.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Variables
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#variables-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct Variables {
    #[serde(flatten)]
    pub entries: std::collections::BTreeMap<String, StructOrString<Variable>>,
}

impl IntoDto for Variables {
    type Dto = dtos::Variables;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::Variables> for Variables {
    fn from(v: dtos::Variables) -> Self {
        Self {
            entries: v.entries.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

impl From<Variables> for dtos::Variables {
    fn from(v: Variables) -> Self {
        Self {
            entries: v.entries.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Watermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Watermark {
    #[serde(with = "datetime_rfc3339")]
    pub system_time: DateTime<Utc>,
    #[serde(with = "datetime_rfc3339")]
    pub event_time: DateTime<Utc>,
}

impl IntoDto for Watermark {
    type Dto = dtos::Watermark;
    fn into_dto(self) -> Self::Dto {
        self.into()
    }
}

impl From<dtos::Watermark> for Watermark {
    fn from(v: dtos::Watermark) -> Self {
        Self {
            system_time: v.system_time,
            event_time: v.event_time,
        }
    }
}

impl From<Watermark> for dtos::Watermark {
    fn from(v: Watermark) -> Self {
        Self {
            system_time: v.system_time,
            event_time: v.event_time,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

implement_serde_as!(dtos::AccountRef, AccountRef);
implement_serde_as!(dtos::AddData, AddData);
implement_serde_as!(dtos::AddPushSource, AddPushSource);
implement_serde_as!(dtos::AttachmentEmbedded, AttachmentEmbedded);
implement_serde_as!(dtos::Attachments, Attachments);
implement_serde_as!(dtos::AttachmentsEmbedded, AttachmentsEmbedded);
implement_serde_as!(dtos::Checkpoint, Checkpoint);
implement_serde_as!(dtos::CompressionFormat, CompressionFormat);
implement_serde_as!(dtos::DataField, DataField);
implement_serde_as!(dtos::DataSchema, DataSchema);
implement_serde_as!(dtos::DataSlice, DataSlice);
implement_serde_as!(dtos::DataType, DataType);
implement_serde_as!(dtos::DataTypeBinary, DataTypeBinary);
implement_serde_as!(dtos::DataTypeBool, DataTypeBool);
implement_serde_as!(dtos::DataTypeDate, DataTypeDate);
implement_serde_as!(dtos::DataTypeDecimal, DataTypeDecimal);
implement_serde_as!(dtos::DataTypeDuration, DataTypeDuration);
implement_serde_as!(dtos::DataTypeFloat16, DataTypeFloat16);
implement_serde_as!(dtos::DataTypeFloat32, DataTypeFloat32);
implement_serde_as!(dtos::DataTypeFloat64, DataTypeFloat64);
implement_serde_as!(dtos::DataTypeInt16, DataTypeInt16);
implement_serde_as!(dtos::DataTypeInt32, DataTypeInt32);
implement_serde_as!(dtos::DataTypeInt64, DataTypeInt64);
implement_serde_as!(dtos::DataTypeInt8, DataTypeInt8);
implement_serde_as!(dtos::DataTypeList, DataTypeList);
implement_serde_as!(dtos::DataTypeMap, DataTypeMap);
implement_serde_as!(dtos::DataTypeNull, DataTypeNull);
implement_serde_as!(dtos::DataTypeOption, DataTypeOption);
implement_serde_as!(dtos::DataTypeString, DataTypeString);
implement_serde_as!(dtos::DataTypeStruct, DataTypeStruct);
implement_serde_as!(dtos::DataTypeTime, DataTypeTime);
implement_serde_as!(dtos::DataTypeTimestamp, DataTypeTimestamp);
implement_serde_as!(dtos::DataTypeUInt16, DataTypeUInt16);
implement_serde_as!(dtos::DataTypeUInt32, DataTypeUInt32);
implement_serde_as!(dtos::DataTypeUInt64, DataTypeUInt64);
implement_serde_as!(dtos::DataTypeUInt8, DataTypeUInt8);
implement_serde_as!(dtos::DatasetKind, DatasetKind);
implement_serde_as!(dtos::DatasetSnapshot, DatasetSnapshot);
implement_serde_as!(dtos::DatasetVocabulary, DatasetVocabulary);
implement_serde_as!(dtos::DisablePollingSource, DisablePollingSource);
implement_serde_as!(dtos::DisablePushSource, DisablePushSource);
implement_serde_as!(dtos::EnvVar, EnvVar);
implement_serde_as!(dtos::EventTimeSource, EventTimeSource);
implement_serde_as!(
    dtos::EventTimeSourceFromMetadata,
    EventTimeSourceFromMetadata
);
implement_serde_as!(dtos::EventTimeSourceFromPath, EventTimeSourceFromPath);
implement_serde_as!(
    dtos::EventTimeSourceFromSystemTime,
    EventTimeSourceFromSystemTime
);
implement_serde_as!(dtos::ExecuteTransform, ExecuteTransform);
implement_serde_as!(dtos::ExecuteTransformInput, ExecuteTransformInput);
implement_serde_as!(dtos::ExtraAttributes, ExtraAttributes);
implement_serde_as!(dtos::FetchStep, FetchStep);
implement_serde_as!(dtos::FetchStepContainer, FetchStepContainer);
implement_serde_as!(dtos::FetchStepEthereumLogs, FetchStepEthereumLogs);
implement_serde_as!(dtos::FetchStepFilesGlob, FetchStepFilesGlob);
implement_serde_as!(dtos::FetchStepMqtt, FetchStepMqtt);
implement_serde_as!(dtos::FetchStepUrl, FetchStepUrl);
implement_serde_as!(dtos::MergeStrategy, MergeStrategy);
implement_serde_as!(dtos::MergeStrategyAppend, MergeStrategyAppend);
implement_serde_as!(
    dtos::MergeStrategyChangelogStream,
    MergeStrategyChangelogStream
);
implement_serde_as!(dtos::MergeStrategyLedger, MergeStrategyLedger);
implement_serde_as!(dtos::MergeStrategySnapshot, MergeStrategySnapshot);
implement_serde_as!(dtos::MergeStrategyUpsertStream, MergeStrategyUpsertStream);
implement_serde_as!(dtos::MetadataBlock, MetadataBlock);
implement_serde_as!(dtos::MetadataEvent, MetadataEvent);
implement_serde_as!(dtos::MqttQos, MqttQos);
implement_serde_as!(dtos::MqttTopicSubscription, MqttTopicSubscription);
implement_serde_as!(dtos::OffsetInterval, OffsetInterval);
implement_serde_as!(dtos::PrepStep, PrepStep);
implement_serde_as!(dtos::PrepStepDecompress, PrepStepDecompress);
implement_serde_as!(dtos::PrepStepPipe, PrepStepPipe);
implement_serde_as!(dtos::RawQueryRequest, RawQueryRequest);
implement_serde_as!(dtos::RawQueryResponse, RawQueryResponse);
implement_serde_as!(
    dtos::RawQueryResponseInternalError,
    RawQueryResponseInternalError
);
implement_serde_as!(
    dtos::RawQueryResponseInvalidQuery,
    RawQueryResponseInvalidQuery
);
implement_serde_as!(dtos::RawQueryResponseProgress, RawQueryResponseProgress);
implement_serde_as!(dtos::RawQueryResponseSuccess, RawQueryResponseSuccess);
implement_serde_as!(dtos::ReadStep, ReadStep);
implement_serde_as!(dtos::ReadStepCsv, ReadStepCsv);
implement_serde_as!(dtos::ReadStepEsriShapefile, ReadStepEsriShapefile);
implement_serde_as!(dtos::ReadStepGeoJson, ReadStepGeoJson);
implement_serde_as!(dtos::ReadStepJson, ReadStepJson);
implement_serde_as!(dtos::ReadStepNdGeoJson, ReadStepNdGeoJson);
implement_serde_as!(dtos::ReadStepNdJson, ReadStepNdJson);
implement_serde_as!(dtos::ReadStepParquet, ReadStepParquet);
implement_serde_as!(dtos::RequestHeader, RequestHeader);
implement_serde_as!(dtos::ResourceAnnotations, ResourceAnnotations);
implement_serde_as!(dtos::ResourceCondition, ResourceCondition);
implement_serde_as!(dtos::ResourceConditions, ResourceConditions);
implement_serde_as!(dtos::ResourceHeader, ResourceHeader);
implement_serde_as!(dtos::ResourceLabels, ResourceLabels);
implement_serde_as!(dtos::ResourcePhase, ResourcePhase);
implement_serde_as!(dtos::ResourceRef, ResourceRef);
implement_serde_as!(dtos::ResourceStatus, ResourceStatus);
implement_serde_as!(dtos::Secret, Secret);
implement_serde_as!(dtos::SecretSet, SecretSet);
implement_serde_as!(dtos::Secrets, Secrets);
implement_serde_as!(dtos::Seed, Seed);
implement_serde_as!(dtos::SetAttachments, SetAttachments);
implement_serde_as!(dtos::SetDataSchema, SetDataSchema);
implement_serde_as!(dtos::SetInfo, SetInfo);
implement_serde_as!(dtos::SetLicense, SetLicense);
implement_serde_as!(dtos::SetPollingSource, SetPollingSource);
implement_serde_as!(dtos::SetTransform, SetTransform);
implement_serde_as!(dtos::SetVocab, SetVocab);
implement_serde_as!(dtos::SourceCaching, SourceCaching);
implement_serde_as!(dtos::SourceCachingForever, SourceCachingForever);
implement_serde_as!(dtos::SourceOrdering, SourceOrdering);
implement_serde_as!(dtos::SourceState, SourceState);
implement_serde_as!(dtos::SqlQueryStep, SqlQueryStep);
implement_serde_as!(dtos::TemporalTable, TemporalTable);
implement_serde_as!(dtos::TimeUnit, TimeUnit);
implement_serde_as!(dtos::Transform, Transform);
implement_serde_as!(dtos::TransformSql, TransformSql);
implement_serde_as!(dtos::TransformInput, TransformInput);
implement_serde_as!(dtos::TransformRequest, TransformRequest);
implement_serde_as!(dtos::TransformRequestInput, TransformRequestInput);
implement_serde_as!(dtos::TransformResponse, TransformResponse);
implement_serde_as!(
    dtos::TransformResponseInternalError,
    TransformResponseInternalError
);
implement_serde_as!(
    dtos::TransformResponseInvalidQuery,
    TransformResponseInvalidQuery
);
implement_serde_as!(dtos::TransformResponseProgress, TransformResponseProgress);
implement_serde_as!(dtos::TransformResponseSuccess, TransformResponseSuccess);
implement_serde_as!(dtos::Variable, Variable);
implement_serde_as!(dtos::VariableSet, VariableSet);
implement_serde_as!(dtos::Variables, Variables);
implement_serde_as!(dtos::Watermark, Watermark);
