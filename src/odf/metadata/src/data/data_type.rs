// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::data::*;

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
    pub fn option(inner: DataType) -> Self {
        Self::Option(DataTypeOption {
            inner: Box::new(inner),
        })
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
        if let DataType::Option(t) = self {
            *t.inner
        } else {
            self
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

crate::impl_parse_error!(DataType);

crate::impl_try_from_str!(DataType);

impl std::str::FromStr for DataType {
    type Err = multiformats::ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let err = || multiformats::ParseError::new(s);

        // Shorthand: "Timestamp?"
        if let Some(inner) = s.strip_suffix('?') {
            return Ok(Self::option(inner.parse()?));
        }

        // Generic form: "Option<Timestamp>"
        if let Some(open) = s.find('<') {
            let (name, rest) = s.split_at(open);
            let inner = rest[1..].strip_suffix('>').ok_or_else(err)?;
            if name.trim().eq_ignore_ascii_case("option") {
                return Ok(Self::option(inner.parse()?));
            }
            return Err(err());
        }

        #[expect(clippy::match_same_arms, reason = "readability")]
        match s {
            "Binary" | "binary" => Ok(Self::Binary(DataTypeBinary::default())),
            "Bool" | "bool" => Ok(Self::Bool(DataTypeBool::default())),
            "Date" | "date" => Ok(Self::Date(DataTypeDate::default())),
            "Decimal" | "decimal" => Err(err()),
            "Duration" | "duration" => Ok(Self::Duration(DataTypeDuration::default())),
            "Float16" | "float16" => Ok(Self::Float16(DataTypeFloat16::default())),
            "Float32" | "float32" => Ok(Self::Float32(DataTypeFloat32::default())),
            "Float64" | "float64" => Ok(Self::Float64(DataTypeFloat64::default())),
            "Int8" | "int8" => Ok(Self::Int8(DataTypeInt8::default())),
            "Int16" | "int16" => Ok(Self::Int16(DataTypeInt16::default())),
            "Int32" | "int32" => Ok(Self::Int32(DataTypeInt32::default())),
            "Int64" | "int64" => Ok(Self::Int64(DataTypeInt64::default())),
            "UInt8" | "uint8" => Ok(Self::UInt8(DataTypeUInt8::default())),
            "UInt16" | "uint16" => Ok(Self::UInt16(DataTypeUInt16::default())),
            "UInt32" | "uint32" => Ok(Self::UInt32(DataTypeUInt32::default())),
            "UInt64" | "uint64" => Ok(Self::UInt64(DataTypeUInt64::default())),
            "List" | "list" => Err(err()),
            "Map" | "map" => Err(err()),
            "Null" | "null" => Ok(Self::Null(DataTypeNull::default())),
            "Option" | "option" => Err(err()),
            "Struct" | "struct" => Err(err()),
            "Time" | "time" => Ok(Self::Time(DataTypeTime::default())),
            "Timestamp" | "timestamp" => Ok(Self::Timestamp(DataTypeTimestamp::default())),
            "String" | "string" => Ok(Self::String(DataTypeString::default())),
            _ => Err(err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
