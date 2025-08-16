// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::data_encoding::ArrowEncoding;
use super::dtos_generated::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSchema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataSchema {
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

    pub fn extra(self, attrs: serde_json::Value) -> Self {
        Self {
            extra: Some(ExtraAttributes::new_from_json(attrs).unwrap()),
            ..self
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataField
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataField {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            r#type: data_type,
            extra: None,
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
    pub fn extra(self, attrs: serde_json::Value) -> Self {
        let mut extra = self.extra.unwrap_or_default();
        extra.merge(attrs);
        Self {
            extra: extra.map_empty(),
            ..self
        }
    }
    pub fn encoding(self, enc: impl Into<ArrowEncoding>) -> Self {
        let enc = enc.into();
        let mut extra = self.extra.unwrap_or_default();
        extra.merge_serialized(&enc);
        Self {
            extra: extra.map_empty(),
            ..self
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataType
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
        Self::Duration(DataTypeDuration {
            unit: TimeUnit::Millisecond,
        })
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
            unit: TimeUnit::Millisecond,
        })
    }
    pub fn timestamp_millis_utc() -> Self {
        Self::Timestamp(DataTypeTimestamp {
            unit: TimeUnit::Millisecond,
            timezone: Some("UTC".into()),
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
