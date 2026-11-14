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
