// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{ExtraAttribute, IntoExtraAttribute};
use crate::data::*;
use crate::dtos::dataset::DatasetVocabulary;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSchema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataSchema {
    pub fn builder() -> DataSchemaBuilder {
        DataSchemaBuilder::new()
    }

    pub fn new_empty() -> Self {
        Self {
            fields: Vec::new(),
            extra: None,
        }
    }

    pub fn new(fields: Vec<DataField>) -> Self {
        Self {
            fields,
            extra: None,
        }
    }

    pub fn new_with_attrs(fields: Vec<DataField>, extra: ExtraAttributes) -> Self {
        Self {
            fields,
            extra: extra.map_empty(),
        }
    }

    pub fn field_by_name(&self, name: &str) -> Option<&DataField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Removes fields that match their default values
    pub fn normalize(self) -> Self {
        Self {
            fields: self.fields.into_iter().map(DataField::normalize).collect(),
            ..self
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSchemaBuilder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct DataSchemaBuilder {
    fields: Vec<DataField>,
    extra: ExtraAttributes,
}

impl DataSchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn with_changelog_system_fields(
        mut self,
        vocab: DatasetVocabulary,
        custom_event_time_type: Option<DataType>,
    ) -> Self {
        assert_eq!(self.fields.len(), 0);

        self.fields.extend([
            DataField::i64(vocab.offset_column()),
            DataField::i32(vocab.operation_type_column()),
            DataField::timestamp_millis_utc(vocab.system_time_column()),
            DataField::new(
                vocab.event_time_column(),
                custom_event_time_type.unwrap_or(DataType::timestamp_millis_utc()),
            ),
        ]);

        self
    }

    pub fn push(mut self, field: DataField) -> Self {
        self.fields.push(field);
        self
    }

    pub fn extend<I>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = DataField>,
    {
        self.fields.extend(fields);
        self
    }

    /// Serializes custom attribute and merges with existing
    pub fn extra<T: IntoExtraAttribute>(mut self, attr: T) -> Self {
        self.extra.insert(attr);
        self
    }

    /// Merges JSON object with existing attributes
    pub fn extra_json(mut self, attrs: serde_json::Value) -> Self {
        self.extra.insert_json(attrs);
        self
    }

    pub fn build(self) -> Result<DataSchema, InvalidSchema> {
        // TODO: Add validation
        // - duplicate fields
        // - system columns
        // - field types sanity
        Ok(DataSchema::new_with_attrs(self.fields, self.extra))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataField
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataField {
    pub fn new(name: impl Into<String>, data_type: impl Into<DataType>) -> Self {
        Self {
            name: name.into(),
            r#type: data_type.into(),
            extra: None,
        }
    }

    pub fn is_optional(&self) -> bool {
        matches!(&self.r#type, DataType::Option(_))
    }

    pub fn normalize(self) -> Self {
        Self {
            r#type: self.r#type.normalize(),
            ..self
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Builder interface
impl DataField {
    pub fn binary(name: impl Into<String>) -> Self {
        Self::new(name, DataType::binary())
    }
    pub fn bool(name: impl Into<String>) -> Self {
        Self::new(name, DataType::bool())
    }
    pub fn date(name: impl Into<String>) -> Self {
        Self::new(name, DataType::date())
    }
    pub fn decimal(name: impl Into<String>, precision: u32, scale: i32) -> Self {
        Self::new(name, DataType::decimal(precision, scale))
    }
    pub fn duration_millis(name: impl Into<String>) -> Self {
        Self::new(name, DataType::duration_millis())
    }
    pub fn i8(name: impl Into<String>) -> Self {
        Self::new(name, DataType::i8())
    }
    pub fn i16(name: impl Into<String>) -> Self {
        Self::new(name, DataType::i16())
    }
    pub fn i32(name: impl Into<String>) -> Self {
        Self::new(name, DataType::i32())
    }
    pub fn i64(name: impl Into<String>) -> Self {
        Self::new(name, DataType::i64())
    }
    pub fn u8(name: impl Into<String>) -> Self {
        Self::new(name, DataType::u8())
    }
    pub fn u16(name: impl Into<String>) -> Self {
        Self::new(name, DataType::u16())
    }
    pub fn u32(name: impl Into<String>) -> Self {
        Self::new(name, DataType::u32())
    }
    pub fn u64(name: impl Into<String>) -> Self {
        Self::new(name, DataType::u64())
    }
    pub fn f16(name: impl Into<String>) -> Self {
        Self::new(name, DataType::f16())
    }
    pub fn f32(name: impl Into<String>) -> Self {
        Self::new(name, DataType::f32())
    }
    pub fn f64(name: impl Into<String>) -> Self {
        Self::new(name, DataType::f64())
    }
    pub fn list(name: impl Into<String>, item_type: DataType) -> Self {
        Self::new(name, DataType::list(item_type))
    }
    pub fn map(name: impl Into<String>, key_type: DataType, value_type: DataType) -> Self {
        Self::new(name, DataType::map(key_type, value_type))
    }
    pub fn null(name: impl Into<String>) -> Self {
        Self::new(name, DataType::null())
    }
    pub fn time_millis(name: impl Into<String>) -> Self {
        Self::new(name, DataType::time_millis())
    }
    pub fn timestamp_millis_utc(name: impl Into<String>) -> Self {
        Self::new(name, DataType::timestamp_millis_utc())
    }
    pub fn structure(name: impl Into<String>, fields: Vec<DataField>) -> Self {
        Self::new(name, DataType::structure(fields))
    }
    pub fn string(name: impl Into<String>) -> Self {
        Self::new(name, DataType::string())
    }

    pub fn optional(self) -> Self {
        Self {
            r#type: self.r#type.optional(),
            ..self
        }
    }

    /// Transforms Option<T> into T, leaving already non-optional types as-is
    pub fn required(self) -> Self {
        Self {
            r#type: self.r#type.required(),
            ..self
        }
    }

    /// Serializes custom attribute and merges with existing
    pub fn extra<T: IntoExtraAttribute>(self, attr: T) -> Self {
        let mut extra = self.extra.unwrap_or_default();
        extra.insert(attr);
        Self {
            extra: extra.map_empty(),
            ..self
        }
    }

    /// Merges JSON object with existing attributes
    pub fn extra_json(self, attrs: serde_json::Value) -> Self {
        let mut extra = self.extra.unwrap_or_default();
        extra.insert_json(attrs);
        Self {
            extra: extra.map_empty(),
            ..self
        }
    }

    pub fn get_extra<T>(&self) -> Result<Option<T>, serde_json::Error>
    where
        T: ExtraAttribute,
    {
        if let Some(extra) = self.extra.as_ref() {
            extra.get()
        } else {
            Ok(None)
        }
    }

    pub fn description(self, description: impl Into<String>) -> Self {
        self.extra(crate::data::ext::AttrDescription::new(description))
    }

    pub fn type_ext(self, typ: impl Into<crate::data::ext::DataTypeExt>) -> Self {
        self.extra(crate::data::ext::AttrType::new(typ.into()))
    }

    pub fn encoding(self, enc: impl Into<ArrowEncoding>) -> Self {
        let enc = enc.into();
        let mut extra = self.extra.unwrap_or_default();
        extra.insert(enc);
        Self {
            extra: extra.map_empty(),
            ..self
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataType
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataType {
    pub fn normalize(self) -> Self {
        match self {
            DataType::Binary(v) => DataType::Binary(v.normalize()),
            DataType::Bool(v) => DataType::Bool(v.normalize()),
            DataType::Date(v) => DataType::Date(v.normalize()),
            DataType::Decimal(v) => DataType::Decimal(v.normalize()),
            DataType::Duration(v) => DataType::Duration(v.normalize()),
            DataType::Float16(v) => DataType::Float16(v.normalize()),
            DataType::Float32(v) => DataType::Float32(v.normalize()),
            DataType::Float64(v) => DataType::Float64(v.normalize()),
            DataType::Int8(v) => DataType::Int8(v.normalize()),
            DataType::Int16(v) => DataType::Int16(v.normalize()),
            DataType::Int32(v) => DataType::Int32(v.normalize()),
            DataType::Int64(v) => DataType::Int64(v.normalize()),
            DataType::UInt8(v) => DataType::UInt8(v.normalize()),
            DataType::UInt16(v) => DataType::UInt16(v.normalize()),
            DataType::UInt32(v) => DataType::UInt32(v.normalize()),
            DataType::UInt64(v) => DataType::UInt64(v.normalize()),
            DataType::List(v) => DataType::List(v.normalize()),
            DataType::Map(v) => DataType::Map(v.normalize()),
            DataType::Null(v) => DataType::Null(v.normalize()),
            DataType::Option(v) => DataType::Option(v.normalize()),
            DataType::Struct(v) => DataType::Struct(v.normalize()),
            DataType::Time(v) => DataType::Time(v.normalize()),
            DataType::Timestamp(v) => DataType::Timestamp(v.normalize()),
            DataType::String(v) => DataType::String(v.normalize()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Builder interface
impl DataType {
    pub fn binary() -> Self {
        Self::Binary(DataTypeBinary { fixed_length: None })
    }
    pub fn bool() -> Self {
        Self::Bool(DataTypeBool {})
    }
    pub fn date() -> Self {
        Self::Date(DataTypeDate {})
    }
    pub fn decimal(precision: u32, scale: i32) -> Self {
        Self::Decimal(DataTypeDecimal { precision, scale })
    }
    pub fn duration_millis() -> Self {
        debug_assert_eq!(DataTypeDuration::default_unit(), TimeUnit::Millisecond);
        Self::Duration(DataTypeDuration { unit: None })
    }
    pub fn i8() -> Self {
        Self::Int8(DataTypeInt8 {})
    }
    pub fn i16() -> Self {
        Self::Int16(DataTypeInt16 {})
    }
    pub fn i32() -> Self {
        Self::Int32(DataTypeInt32 {})
    }
    pub fn i64() -> Self {
        Self::Int64(DataTypeInt64 {})
    }
    pub fn u8() -> Self {
        Self::UInt8(DataTypeUInt8 {})
    }
    pub fn u16() -> Self {
        Self::UInt16(DataTypeUInt16 {})
    }
    pub fn u32() -> Self {
        Self::UInt32(DataTypeUInt32 {})
    }
    pub fn u64() -> Self {
        Self::UInt64(DataTypeUInt64 {})
    }
    pub fn f16() -> Self {
        Self::Float16(DataTypeFloat16 {})
    }
    pub fn f32() -> Self {
        Self::Float32(DataTypeFloat32 {})
    }
    pub fn f64() -> Self {
        Self::Float64(DataTypeFloat64 {})
    }
    pub fn list(item_type: DataType) -> Self {
        Self::List(DataTypeList {
            fixed_length: None,
            item_type: Box::new(item_type),
        })
    }
    pub fn map(key_type: DataType, value_type: DataType) -> Self {
        Self::Map(DataTypeMap {
            key_type: Box::new(key_type),
            value_type: Box::new(value_type),
            keys_sorted: None,
        })
    }
    pub fn null() -> Self {
        Self::Null(DataTypeNull {})
    }
    pub fn time_millis() -> Self {
        Self::Time(DataTypeTime {
            unit: Some(TimeUnit::Millisecond),
        })
    }
    pub fn timestamp_millis_utc() -> Self {
        debug_assert_eq!(DataTypeTimestamp::default_unit(), TimeUnit::Millisecond);
        debug_assert_eq!(DataTypeTimestamp::default_timezone(), "UTC");

        Self::Timestamp(DataTypeTimestamp {
            unit: None,
            timezone: None,
        })
    }
    pub fn structure(fields: Vec<DataField>) -> Self {
        Self::Struct(DataTypeStruct { fields })
    }
    pub fn string() -> Self {
        Self::String(DataTypeString {})
    }

    pub fn optional(self) -> Self {
        Self::Option(DataTypeOption {
            inner: Box::new(self),
        })
    }

    /// Transforms Option<T> into T, leaving already non-optional types as-is
    pub fn required(self) -> Self {
        match self {
            DataType::Option(t) => *t.inner,
            _ => self,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataTypeBinary {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeBool {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeDate {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeDecimal {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeDuration {
    pub fn normalize(self) -> Self {
        Self {
            unit: if self.unit == Some(Self::default_unit()) {
                None
            } else {
                self.unit
            },
        }
    }
}
impl DataTypeFloat16 {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeFloat32 {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeFloat64 {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeInt8 {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeInt16 {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeInt32 {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeInt64 {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeUInt8 {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeUInt16 {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeUInt32 {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeUInt64 {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeList {
    pub fn normalize(self) -> Self {
        Self {
            item_type: Box::new(self.item_type.normalize()),
            ..self
        }
    }
}
impl DataTypeMap {
    pub fn normalize(self) -> Self {
        Self {
            key_type: Box::new(self.key_type.normalize()),
            value_type: Box::new(self.value_type.normalize()),
            ..self
        }
    }
}
impl DataTypeNull {
    pub fn normalize(self) -> Self {
        self
    }
}
impl DataTypeOption {
    pub fn normalize(self) -> Self {
        Self {
            inner: Box::new(self.inner.normalize()),
        }
    }
}
impl DataTypeStruct {
    pub fn normalize(self) -> Self {
        Self {
            fields: self.fields.into_iter().map(DataField::normalize).collect(),
        }
    }
}
impl DataTypeTime {
    pub fn normalize(self) -> Self {
        Self {
            unit: if self.unit == Some(Self::default_unit()) {
                None
            } else {
                self.unit
            },
        }
    }
}
impl DataTypeTimestamp {
    pub fn normalize(self) -> Self {
        Self {
            unit: if self.unit == Some(Self::default_unit()) {
                None
            } else {
                self.unit
            },
            timezone: if self.timezone.as_deref() == Some(Self::default_timezone()) {
                None
            } else {
                self.timezone
            },
        }
    }
}
impl DataTypeString {
    pub fn normalize(self) -> Self {
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum InvalidSchema {
    #[error(transparent)]
    Malformed(#[from] MalformedSchema),

    #[error(transparent)]
    Unsupported(#[from] UnsupportedSchema),
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct MalformedSchema {
    message: String,
}

impl MalformedSchema {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct UnsupportedSchema {
    message: String,
}

impl UnsupportedSchema {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
