// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::arrow_encoding::ArrowEncoding;
use crate::dtos::*;
#[cfg(feature = "arrow")]
use crate::UnsupportedSchema;
use crate::{ArrowBufferEncoding, ArrowDateEncoding};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSchema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataSchema {
    #[cfg(feature = "arrow")]
    pub fn new_from_arrow(value: &arrow::datatypes::Schema) -> Result<Self, UnsupportedSchema> {
        let mut fields = Vec::with_capacity(value.fields.len());
        for field in &value.fields {
            fields.push(DataField::new_from_arrow(field)?);
        }

        Ok(Self {
            fields,
            extra: None,
        })
    }

    #[cfg(feature = "arrow")]
    pub fn to_arrow(
        &self,
        settings: &ToArrowSettings,
    ) -> Result<arrow::datatypes::Schema, UnsupportedSchema> {
        let fields: Vec<_> = self
            .fields
            .iter()
            .map(|f| f.to_arrow(settings))
            .collect::<Result<_, _>>()?;

        Ok(arrow::datatypes::Schema::new(fields))
    }

    /// Strips all *known* encoding-related attributes.
    pub fn strip_encoding(self) -> Self {
        let fields = self
            .fields
            .into_iter()
            .map(DataField::strip_encoding)
            .collect();

        Self {
            fields,
            extra: self.extra,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ToArrowSettings {
    // Which buffer encoding to use if not explicitly specified
    pub default_buffer_encoding: Option<ArrowBufferEncoding>,

    // How to represent Date if not explicitly specified
    pub default_date_encoding: Option<ArrowDateEncoding>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataField
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataField {
    #[cfg(feature = "arrow")]
    fn new_from_arrow(value: &arrow::datatypes::Field) -> Result<Self, UnsupportedSchema> {
        let (r#type, encoding) = DataType::new_from_arrow(value.data_type(), value.is_nullable())?;

        let mut extra = ExtraAttributes::new();
        if let Some(encoding) = encoding {
            extra.insert(encoding);
        }

        Ok(Self {
            name: value.name().clone(),
            r#type,
            extra: extra.map_empty(),
        })
    }

    #[cfg(feature = "arrow")]
    pub fn to_arrow(
        &self,
        settings: &ToArrowSettings,
    ) -> Result<arrow::datatypes::Field, UnsupportedSchema> {
        let empty = ExtraAttributes::new();
        let extra = self.extra.as_ref().unwrap_or(&empty);
        let encoding = extra
            .get::<ArrowEncoding>()
            .map_err(|e| UnsupportedSchema::new(format!("Invalid encoding: {e}")))?;

        let (data_type, nullable) = self.r#type.to_arrow(encoding.as_ref(), settings)?;

        Ok(arrow::datatypes::Field::new(
            &self.name, data_type, nullable,
        ))
    }

    pub fn strip_encoding(self) -> Self {
        Self {
            name: self.name,
            r#type: self.r#type.strip_encoding(),
            extra: self.extra.and_then(|e| {
                e.retain(|k, _| !ArrowEncoding::KEYS.contains(&k.as_str()))
                    .map_empty()
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataType
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataType {
    /// Translates Arrow schema to ODF schema as closely as possible, emitting
    /// both logical types and encoding hints about how data in Arrow is layed
    /// out. Remember that how data is layed out in Arrow in-memory is not
    /// always the same to how it's stored on disk or how it will be read back
    /// from disk as different optimizations may be applied along the way.
    ///
    /// The `new_from_arrow` / `to_arrow` methods must be symmetric. The goal is
    /// to achieve ability to convert back and forth between ODF schema and
    /// Arrow without information loss. Since [`DataType`] in ODF schema
    /// represents *logical* types - all information related to encoding and
    /// physical layout of data is pushed into `extra` attributes (see
    /// [`ArrowEncoding`]).
    #[cfg(feature = "arrow")]
    fn new_from_arrow(
        value: &arrow::datatypes::DataType,
        nullable: bool,
    ) -> Result<(Self, Option<ArrowEncoding>), UnsupportedSchema> {
        use arrow::datatypes::DataType as ArrowDataType;

        use crate::{ArrowDateEncoding, ArrowDateUnit, ArrowDecimalEncoding};

        let (odf_type, encoding) = match value {
            ArrowDataType::Null => (DataType::Null(DataTypeNull {}), None),
            ArrowDataType::Boolean => (DataType::Bool(DataTypeBool {}), None),
            ArrowDataType::Int8 => (DataType::Int8(DataTypeInt8 {}), None),
            ArrowDataType::Int16 => (DataType::Int16(DataTypeInt16 {}), None),
            ArrowDataType::Int32 => (DataType::Int32(DataTypeInt32 {}), None),
            ArrowDataType::Int64 => (DataType::Int64(DataTypeInt64 {}), None),
            ArrowDataType::UInt8 => (DataType::UInt8(DataTypeUInt8 {}), None),
            ArrowDataType::UInt16 => (DataType::UInt16(DataTypeUInt16 {}), None),
            ArrowDataType::UInt32 => (DataType::UInt32(DataTypeUInt32 {}), None),
            ArrowDataType::UInt64 => (DataType::UInt64(DataTypeUInt64 {}), None),
            ArrowDataType::Float16 => (DataType::Float16(DataTypeFloat16 {}), None),
            ArrowDataType::Float32 => (DataType::Float32(DataTypeFloat32 {}), None),
            ArrowDataType::Float64 => (DataType::Float64(DataTypeFloat64 {}), None),
            ArrowDataType::Timestamp(time_unit, tz) => (
                DataType::Timestamp(DataTypeTimestamp {
                    unit: (*time_unit).into(),
                    timezone: tz.as_ref().map(std::string::ToString::to_string),
                }),
                None,
            ),
            ArrowDataType::Date32 => (DataType::Date(DataTypeDate {}), None),
            ArrowDataType::Date64 => (
                DataType::Date(DataTypeDate {}),
                Some(ArrowEncoding {
                    date: Some(ArrowDateEncoding {
                        unit: ArrowDateUnit::Millisecond,
                    }),
                    ..Default::default()
                }),
            ),
            ArrowDataType::Time32(time_unit) | ArrowDataType::Time64(time_unit) => (
                DataType::Time(DataTypeTime {
                    unit: (*time_unit).into(),
                }),
                None,
            ),
            ArrowDataType::Duration(time_unit) => (
                DataType::Duration(DataTypeDuration {
                    unit: (*time_unit).into(),
                }),
                None,
            ),
            ArrowDataType::Interval(_) => {
                return Err(UnsupportedSchema::new(
                    "Interval types are not yet supported in ODF schema",
                ));
            }
            ArrowDataType::Binary => (
                DataType::Binary(DataTypeBinary { fixed_length: None }),
                Some(
                    ArrowBufferEncoding::Contiguous {
                        offset_bit_width: Some(32),
                    }
                    .into(),
                ),
            ),
            ArrowDataType::LargeBinary => (
                DataType::Binary(DataTypeBinary { fixed_length: None }),
                Some(
                    ArrowBufferEncoding::Contiguous {
                        offset_bit_width: Some(64),
                    }
                    .into(),
                ),
            ),
            ArrowDataType::BinaryView => (
                DataType::Binary(DataTypeBinary { fixed_length: None }),
                Some(
                    ArrowBufferEncoding::View {
                        offset_bit_width: Some(32),
                    }
                    .into(),
                ),
            ),
            ArrowDataType::FixedSizeBinary(fixed_length) => (
                DataType::Binary(DataTypeBinary {
                    fixed_length: Some(u64::try_from(*fixed_length).unwrap()),
                }),
                None,
            ),
            ArrowDataType::Utf8 => (
                DataType::String(DataTypeString {}),
                Some(
                    ArrowBufferEncoding::Contiguous {
                        offset_bit_width: Some(32),
                    }
                    .into(),
                ),
            ),
            ArrowDataType::LargeUtf8 => (
                DataType::String(DataTypeString {}),
                Some(
                    ArrowBufferEncoding::Contiguous {
                        offset_bit_width: Some(64),
                    }
                    .into(),
                ),
            ),
            ArrowDataType::Utf8View => (
                DataType::String(DataTypeString {}),
                Some(
                    ArrowBufferEncoding::View {
                        offset_bit_width: Some(32),
                    }
                    .into(),
                ),
            ),
            ArrowDataType::List(field) => {
                let (item_type, encoding) =
                    DataType::new_from_arrow(field.data_type(), field.is_nullable())?;

                if let Some(encoding) = encoding {
                    return Err(UnsupportedSchema::new(format!(
                        "List item types with custom encoding {encoding:?} are not supported"
                    )));
                }

                (
                    DataType::List(DataTypeList {
                        item_type: Box::new(item_type),
                        fixed_length: None,
                    }),
                    None,
                )
            }
            ArrowDataType::ListView(field) => {
                let (item_type, encoding) =
                    DataType::new_from_arrow(field.data_type(), field.is_nullable())?;

                if let Some(encoding) = encoding {
                    return Err(UnsupportedSchema::new(format!(
                        "List item types with custom encoding {encoding:?} are not supported"
                    )));
                }

                (
                    DataType::List(DataTypeList {
                        item_type: Box::new(item_type),
                        fixed_length: None,
                    }),
                    Some(
                        ArrowBufferEncoding::View {
                            offset_bit_width: Some(32),
                        }
                        .into(),
                    ),
                )
            }
            ArrowDataType::FixedSizeList(field, fixed_length) => {
                let (item_type, encoding) =
                    DataType::new_from_arrow(field.data_type(), field.is_nullable())?;

                if let Some(encoding) = encoding {
                    return Err(UnsupportedSchema::new(format!(
                        "List item types with custom encoding {encoding:?} are not supported"
                    )));
                }

                (
                    DataType::List(DataTypeList {
                        item_type: Box::new(item_type),
                        fixed_length: Some(u64::try_from(*fixed_length).unwrap()),
                    }),
                    None,
                )
            }
            ArrowDataType::LargeList(field) => {
                let (item_type, encoding) =
                    DataType::new_from_arrow(field.data_type(), field.is_nullable())?;

                if let Some(encoding) = encoding {
                    return Err(UnsupportedSchema::new(format!(
                        "List item types with custom encoding {encoding:?} are not supported"
                    )));
                }

                (
                    DataType::List(DataTypeList {
                        item_type: Box::new(item_type),
                        fixed_length: None,
                    }),
                    Some(
                        ArrowBufferEncoding::Contiguous {
                            offset_bit_width: Some(64),
                        }
                        .into(),
                    ),
                )
            }
            ArrowDataType::LargeListView(field) => {
                let (item_type, encoding) =
                    DataType::new_from_arrow(field.data_type(), field.is_nullable())?;

                if let Some(encoding) = encoding {
                    return Err(UnsupportedSchema::new(format!(
                        "List item types with custom encoding {encoding:?} are not supported"
                    )));
                }

                (
                    DataType::List(DataTypeList {
                        item_type: Box::new(item_type),
                        fixed_length: None,
                    }),
                    Some(
                        ArrowBufferEncoding::View {
                            offset_bit_width: Some(64),
                        }
                        .into(),
                    ),
                )
            }
            ArrowDataType::Struct(fields) => (
                DataType::Struct(DataTypeStruct {
                    fields: fields
                        .iter()
                        .map(|f| DataField::new_from_arrow(f))
                        .collect::<Result<_, _>>()?,
                }),
                None,
            ),
            ArrowDataType::Union(_, _) => {
                return Err(UnsupportedSchema::new(
                    "Union types are not yet supported in ODF schema",
                ));
            }
            ArrowDataType::Dictionary(_, _) => {
                return Err(UnsupportedSchema::new(
                    "Dictionary types are not yet supported in ODF schema",
                ));
            }
            ArrowDataType::Decimal128(precision, scale) => (
                DataType::Decimal(DataTypeDecimal {
                    precision: (*precision).into(),
                    scale: (*scale).into(),
                }),
                Some(ArrowDecimalEncoding { bit_width: 128 }.into()),
            ),
            ArrowDataType::Decimal256(precision, scale) => (
                DataType::Decimal(DataTypeDecimal {
                    precision: (*precision).into(),
                    scale: (*scale).into(),
                }),
                Some(ArrowDecimalEncoding { bit_width: 256 }.into()),
            ),
            ArrowDataType::Map(field, keys_sorted) => {
                let ArrowDataType::Struct(inner_fields) = field.data_type() else {
                    panic!("Invalid representation of an Arrow Map type: {field:#?}");
                };

                assert!(
                    inner_fields.len() == 2,
                    "Invalid representation of an Arrow Map type: {field:#?}"
                );

                let (key_type, key_encoding) = DataType::new_from_arrow(
                    inner_fields[0].data_type(),
                    inner_fields[0].is_nullable(),
                )?;
                let (value_type, value_encoding) = DataType::new_from_arrow(
                    inner_fields[1].data_type(),
                    inner_fields[1].is_nullable(),
                )?;

                if let Some(key_encoding) = key_encoding {
                    return Err(UnsupportedSchema::new(format!(
                        "Map key types with custom encoding {key_encoding:?} are not supported"
                    )));
                }

                if let Some(value_encoding) = value_encoding {
                    return Err(UnsupportedSchema::new(format!(
                        "Map value types with custom encoding {value_encoding:?} are not supported"
                    )));
                }

                (
                    DataType::Map(DataTypeMap {
                        key_type: Box::new(key_type),
                        value_type: Box::new(value_type),
                        keys_sorted: Some(*keys_sorted),
                    }),
                    None,
                )
            }
            ArrowDataType::RunEndEncoded(run_ends, values) => {
                let run_ends_bit_width = match run_ends.data_type() {
                    ArrowDataType::Int32 => 32,
                    ArrowDataType::Int64 => 64,
                    t => {
                        return Err(UnsupportedSchema::new(format!(
                            "Expected Int32/Int64 type for run ends but got: {t:?}"
                        )))
                    }
                };

                let (value_type, value_encoding) =
                    DataType::new_from_arrow(values.data_type(), values.is_nullable())?;

                if let Some(value_encoding) = value_encoding {
                    return Err(UnsupportedSchema::new(format!(
                        "REE encoding of types with another layer of custom encoding \
                         {value_encoding:?} is not supported"
                    )));
                }

                (
                    value_type,
                    Some(
                        ArrowBufferEncoding::RunEnd {
                            run_ends_bit_width: Some(run_ends_bit_width),
                        }
                        .into(),
                    ),
                )
            }
        };

        if !nullable {
            Ok((odf_type, encoding))
        } else {
            Ok((
                DataType::Option(DataTypeOption {
                    inner: Box::new(odf_type),
                }),
                encoding,
            ))
        }
    }

    #[cfg(feature = "arrow")]
    pub fn to_arrow(
        &self,
        encoding: Option<&ArrowEncoding>,
        settings: &ToArrowSettings,
    ) -> Result<(arrow::datatypes::DataType, bool), UnsupportedSchema> {
        use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};

        use crate::{ArrowBufferEncoding, ArrowDateEncoding, ArrowDateUnit, ArrowDecimalEncoding};

        if let DataType::Option(DataTypeOption { inner }) = self {
            let (arrow_type, nullable) = inner.to_arrow(encoding, settings)?;
            assert!(!nullable, "Nested Option types are not supported: {self:?}");
            return Ok((arrow_type, true));
        }

        // TODO: Avoid cloning
        let encoding = if encoding.is_some() {
            encoding.cloned()
        } else {
            match self {
                DataType::Binary(_) | DataType::String(_) | DataType::List(_) => {
                    Some(ArrowEncoding {
                        buffer: settings.default_buffer_encoding.clone(),
                        date: None,
                        decimal: None,
                    })
                }
                DataType::Date(_) => Some(ArrowEncoding {
                    buffer: None,
                    date: settings.default_date_encoding.clone(),
                    decimal: None,
                }),
                DataType::Bool(_)
                | DataType::Decimal(_)
                | DataType::Duration(_)
                | DataType::Float16(_)
                | DataType::Float32(_)
                | DataType::Float64(_)
                | DataType::Int8(_)
                | DataType::Int16(_)
                | DataType::Int32(_)
                | DataType::Int64(_)
                | DataType::UInt8(_)
                | DataType::UInt16(_)
                | DataType::UInt32(_)
                | DataType::UInt64(_)
                | DataType::Map(_)
                | DataType::Null(_)
                | DataType::Option(_)
                | DataType::Struct(_)
                | DataType::Time(_)
                | DataType::Timestamp(_) => None,
            }
        };

        let unsupported = || {
            Err(UnsupportedSchema::new(format!(
                "Unsupported type and encoding combination: {self:?} {encoding:?}"
            )))
        };

        let arrow_type = match &encoding {
            None
            | Some(ArrowEncoding {
                buffer: None,
                date: None,
                decimal: None,
            }) => match self {
                DataType::Binary(DataTypeBinary { fixed_length: None }) => ArrowDataType::Binary,
                DataType::Binary(DataTypeBinary {
                    fixed_length: Some(fixed_length),
                }) => ArrowDataType::FixedSizeBinary(i32::try_from(*fixed_length).unwrap()),
                DataType::Bool(DataTypeBool {}) => ArrowDataType::Boolean,
                DataType::Date(DataTypeDate {}) => ArrowDataType::Date32,
                DataType::Decimal(DataTypeDecimal { precision, scale }) => {
                    if *precision <= ArrowDecimalEncoding::MAX_PRECISION_128 {
                        ArrowDataType::Decimal128(
                            u8::try_from(*precision).unwrap(),
                            i8::try_from(*scale).unwrap(),
                        )
                    } else if *precision <= ArrowDecimalEncoding::MAX_PRECISION_256 {
                        ArrowDataType::Decimal256(
                            u8::try_from(*precision).unwrap(),
                            i8::try_from(*scale).unwrap(),
                        )
                    } else {
                        return Err(UnsupportedSchema::new(format!(
                            "Arrow's largest decimal is 256-bit which is too small to represent \
                             precision of {precision} (maximum is {})",
                            ArrowDecimalEncoding::MAX_PRECISION_256,
                        )));
                    }
                }
                DataType::Duration(DataTypeDuration { unit }) => {
                    ArrowDataType::Duration((*unit).into())
                }
                DataType::Float16(DataTypeFloat16 {}) => ArrowDataType::Float16,
                DataType::Float32(DataTypeFloat32 {}) => ArrowDataType::Float32,
                DataType::Float64(DataTypeFloat64 {}) => ArrowDataType::Float64,
                DataType::Int8(DataTypeInt8 {}) => ArrowDataType::Int8,
                DataType::Int16(DataTypeInt16 {}) => ArrowDataType::Int16,
                DataType::Int32(DataTypeInt32 {}) => ArrowDataType::Int32,
                DataType::Int64(DataTypeInt64 {}) => ArrowDataType::Int64,
                DataType::UInt8(DataTypeUInt8 {}) => ArrowDataType::UInt8,
                DataType::UInt16(DataTypeUInt16 {}) => ArrowDataType::UInt16,
                DataType::UInt32(DataTypeUInt32 {}) => ArrowDataType::UInt32,
                DataType::UInt64(DataTypeUInt64 {}) => ArrowDataType::UInt64,
                DataType::List(DataTypeList {
                    item_type,
                    fixed_length: None,
                }) => {
                    let (item_type_arrow, nullable) = item_type.to_arrow(None, settings)?;
                    ArrowDataType::List(
                        ArrowField::new_list_field(item_type_arrow, nullable).into(),
                    )
                }
                DataType::List(DataTypeList {
                    item_type,
                    fixed_length: Some(fixed_length),
                }) => {
                    let (item_type_arrow, nullable) = item_type.to_arrow(None, settings)?;
                    ArrowDataType::FixedSizeList(
                        ArrowField::new_list_field(item_type_arrow, nullable).into(),
                        i32::try_from(*fixed_length).unwrap(),
                    )
                }
                DataType::Map(DataTypeMap {
                    key_type,
                    value_type,
                    keys_sorted,
                }) => {
                    let (key_type_arrow, key_type_nullable) = key_type.to_arrow(None, settings)?;
                    let (value_type_arrow, value_type_nullable) =
                        value_type.to_arrow(None, settings)?;

                    let keys = ArrowField::new("keys", key_type_arrow, key_type_nullable);
                    let values = ArrowField::new("values", value_type_arrow, value_type_nullable);

                    ArrowDataType::Map(
                        ArrowField::new(
                            "entries",
                            ArrowDataType::Struct(arrow::datatypes::Fields::from([
                                keys.into(),
                                values.into(),
                            ])),
                            false, // The inner map field is always non-nullable (arrow-rs#1697),
                        )
                        .into(),
                        keys_sorted.unwrap_or(false),
                    )
                }
                DataType::Null(DataTypeNull {}) => ArrowDataType::Null,
                DataType::Struct(DataTypeStruct { fields }) => {
                    let fields: Vec<_> = fields
                        .iter()
                        .map(|f| f.to_arrow(settings))
                        .collect::<Result<_, _>>()?;
                    ArrowDataType::Struct(arrow::datatypes::Fields::from(fields))
                }
                DataType::Time(DataTypeTime {
                    unit: unit @ (TimeUnit::Second | TimeUnit::Millisecond),
                }) => ArrowDataType::Time32((*unit).into()),
                DataType::Time(DataTypeTime {
                    unit: unit @ (TimeUnit::Microsecond | TimeUnit::Nanosecond),
                }) => ArrowDataType::Time64((*unit).into()),
                DataType::Timestamp(DataTypeTimestamp { unit, timezone }) => {
                    ArrowDataType::Timestamp(
                        (*unit).into(),
                        timezone.as_ref().map(|s| s.as_str().into()),
                    )
                }
                DataType::String(DataTypeString {}) => ArrowDataType::Utf8,
                DataType::Option(_) => unreachable!(),
            },
            Some(ArrowEncoding {
                buffer: None,
                date: Some(ArrowDateEncoding { unit }),
                decimal: None,
            }) => match self {
                DataType::Date(DataTypeDate {}) => match unit {
                    ArrowDateUnit::Day => ArrowDataType::Date32,
                    ArrowDateUnit::Millisecond => ArrowDataType::Date64,
                },
                DataType::Binary(_)
                | DataType::Bool(_)
                | DataType::Decimal(_)
                | DataType::Duration(_)
                | DataType::Float16(_)
                | DataType::Float32(_)
                | DataType::Float64(_)
                | DataType::Int8(_)
                | DataType::Int16(_)
                | DataType::Int32(_)
                | DataType::Int64(_)
                | DataType::UInt8(_)
                | DataType::UInt16(_)
                | DataType::UInt32(_)
                | DataType::UInt64(_)
                | DataType::List(_)
                | DataType::Map(_)
                | DataType::Null(_)
                | DataType::Option(_)
                | DataType::Struct(_)
                | DataType::Time(_)
                | DataType::Timestamp(_)
                | DataType::String(_) => return unsupported(),
            },
            Some(ArrowEncoding {
                buffer: None,
                date: None,
                decimal: Some(ArrowDecimalEncoding { bit_width }),
            }) => match self {
                DataType::Decimal(DataTypeDecimal { precision, scale }) => match bit_width {
                    128 => ArrowDataType::Decimal128(
                        u8::try_from(*precision).unwrap(),
                        i8::try_from(*scale).unwrap(),
                    ),
                    256 => ArrowDataType::Decimal256(
                        u8::try_from(*precision).unwrap(),
                        i8::try_from(*scale).unwrap(),
                    ),
                    _ => return unsupported(),
                },
                DataType::Binary(_)
                | DataType::Bool(_)
                | DataType::Date(_)
                | DataType::Duration(_)
                | DataType::Float16(_)
                | DataType::Float32(_)
                | DataType::Float64(_)
                | DataType::Int8(_)
                | DataType::Int16(_)
                | DataType::Int32(_)
                | DataType::Int64(_)
                | DataType::UInt8(_)
                | DataType::UInt16(_)
                | DataType::UInt32(_)
                | DataType::UInt64(_)
                | DataType::List(_)
                | DataType::Map(_)
                | DataType::Null(_)
                | DataType::Option(_)
                | DataType::Struct(_)
                | DataType::Time(_)
                | DataType::Timestamp(_)
                | DataType::String(_) => return unsupported(),
            },
            Some(ArrowEncoding {
                buffer: Some(buffer),
                date: None,
                decimal: None,
            }) => match self {
                DataType::Binary(DataTypeBinary { fixed_length: None }) => match buffer {
                    ArrowBufferEncoding::Contiguous {
                        offset_bit_width: None | Some(32),
                    } => ArrowDataType::Binary,
                    ArrowBufferEncoding::Contiguous {
                        offset_bit_width: Some(64),
                    } => ArrowDataType::LargeBinary,
                    ArrowBufferEncoding::View {
                        offset_bit_width: None | Some(32),
                    } => ArrowDataType::BinaryView,
                    _ => return unsupported(),
                },
                DataType::List(DataTypeList {
                    item_type,
                    fixed_length: None,
                }) => {
                    let (item_type_arrow, nullable) = item_type.to_arrow(None, settings)?;
                    let list_field = ArrowField::new_list_field(item_type_arrow, nullable);
                    match buffer {
                        ArrowBufferEncoding::Contiguous {
                            offset_bit_width: None | Some(32),
                        } => ArrowDataType::List(list_field.into()),
                        ArrowBufferEncoding::Contiguous {
                            offset_bit_width: Some(64),
                        } => ArrowDataType::LargeList(list_field.into()),
                        ArrowBufferEncoding::View {
                            offset_bit_width: None | Some(32),
                        } => ArrowDataType::ListView(list_field.into()),
                        ArrowBufferEncoding::View {
                            offset_bit_width: Some(64),
                        } => ArrowDataType::LargeListView(list_field.into()),
                        _ => return unsupported(),
                    }
                }
                DataType::String(DataTypeString {}) => match buffer {
                    ArrowBufferEncoding::Contiguous {
                        offset_bit_width: None | Some(32),
                    } => ArrowDataType::Utf8,
                    ArrowBufferEncoding::Contiguous {
                        offset_bit_width: Some(64),
                    } => ArrowDataType::LargeUtf8,
                    ArrowBufferEncoding::View {
                        offset_bit_width: None | Some(32),
                    } => ArrowDataType::Utf8View,
                    _ => return unsupported(),
                },
                DataType::Binary(_)
                | DataType::Bool(_)
                | DataType::Date(_)
                | DataType::Decimal(_)
                | DataType::Duration(_)
                | DataType::Float16(_)
                | DataType::Float32(_)
                | DataType::Float64(_)
                | DataType::Int8(_)
                | DataType::Int16(_)
                | DataType::Int32(_)
                | DataType::Int64(_)
                | DataType::UInt8(_)
                | DataType::UInt16(_)
                | DataType::UInt32(_)
                | DataType::UInt64(_)
                | DataType::List(_)
                | DataType::Map(_)
                | DataType::Null(_)
                | DataType::Struct(_)
                | DataType::Time(_)
                | DataType::Timestamp(_) => return unsupported(),
                DataType::Option(_) => unreachable!(),
            },
            Some(_) => return unsupported(),
        };

        Ok((arrow_type, false))
    }

    pub fn strip_encoding(self) -> Self {
        match self {
            DataType::Option(DataTypeOption { inner }) => DataType::Option(DataTypeOption {
                inner: Box::new(inner.strip_encoding()),
            }),
            DataType::Struct(DataTypeStruct { fields }) => DataType::Struct(DataTypeStruct {
                fields: fields.into_iter().map(DataField::strip_encoding).collect(),
            }),
            DataType::Binary(_)
            | DataType::Bool(_)
            | DataType::Date(_)
            | DataType::Decimal(_)
            | DataType::Duration(_)
            | DataType::Float16(_)
            | DataType::Float32(_)
            | DataType::Float64(_)
            | DataType::Int8(_)
            | DataType::Int16(_)
            | DataType::Int32(_)
            | DataType::Int64(_)
            | DataType::UInt8(_)
            | DataType::UInt16(_)
            | DataType::UInt32(_)
            | DataType::UInt64(_)
            | DataType::List(_)
            | DataType::Map(_)
            | DataType::Null(_)
            | DataType::Time(_)
            | DataType::Timestamp(_)
            | DataType::String(_) => self,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TimeUnit
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<TimeUnit> for arrow::datatypes::TimeUnit {
    fn from(value: TimeUnit) -> Self {
        match value {
            TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
            TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
            TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
        }
    }
}

impl From<arrow::datatypes::TimeUnit> for TimeUnit {
    fn from(value: arrow::datatypes::TimeUnit) -> Self {
        match value {
            arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
            arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
            arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
            arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use serde_json::json;

    use crate::*;

    #[test]
    fn test_field_from_to_arrow() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("utf8", ArrowDataType::Utf8, false),
            ArrowField::new("utf8_large", ArrowDataType::LargeUtf8, false),
            ArrowField::new("utf8_view", ArrowDataType::Utf8View, false),
            ArrowField::new("date32", ArrowDataType::Date32, false),
            ArrowField::new("date64", ArrowDataType::Date64, false),
            ArrowField::new("decimal128", ArrowDataType::Decimal128(20, 5), false),
            ArrowField::new("decimal256", ArrowDataType::Decimal256(20, 5), false),
        ]);

        let odf_schema_actual = DataSchema::new_from_arrow(&arrow_schema).unwrap();
        pretty_assertions::assert_eq!(
            DataSchema {
                fields: vec![
                    DataField {
                        name: "utf8".into(),
                        r#type: DataType::String(DataTypeString {}),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                "arrow.apache.org/bufferEncoding": {
                                    "kind": "Contiguous",
                                    "offsetBitWidth": 32,
                                }
                            }))
                            .unwrap()
                        )
                    },
                    DataField {
                        name: "utf8_large".into(),
                        r#type: DataType::String(DataTypeString {}),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                "arrow.apache.org/bufferEncoding": {
                                    "kind": "Contiguous",
                                    "offsetBitWidth": 64,
                                }
                            }))
                            .unwrap()
                        )
                    },
                    DataField {
                        name: "utf8_view".into(),
                        r#type: DataType::String(DataTypeString {}),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                "arrow.apache.org/bufferEncoding": {
                                    "kind": "View",
                                    "offsetBitWidth": 32,
                                }
                            }))
                            .unwrap()
                        )
                    },
                    DataField {
                        name: "date32".into(),
                        r#type: DataType::Date(DataTypeDate {}),
                        extra: None,
                    },
                    DataField {
                        name: "date64".into(),
                        r#type: DataType::Date(DataTypeDate {}),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                    "arrow.apache.org/dateEncoding":  {
                                        "unit": "Millisecond",
                                    },

                            }))
                            .unwrap()
                        ),
                    },
                    DataField {
                        name: "decimal128".into(),
                        r#type: DataType::Decimal(DataTypeDecimal {
                            precision: 20,
                            scale: 5,
                        },),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                    "arrow.apache.org/decimalEncoding":  {
                                        "bitWidth": 128,
                                    },

                            }))
                            .unwrap()
                        ),
                    },
                    DataField {
                        name: "decimal256".into(),
                        r#type: DataType::Decimal(DataTypeDecimal {
                            precision: 20,
                            scale: 5,
                        },),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                    "arrow.apache.org/decimalEncoding": {
                                        "bitWidth": 256,
                                    },

                            }))
                            .unwrap()
                        ),
                    },
                ],
                extra: None,
            },
            odf_schema_actual,
        );

        let arrow_schema_actual = odf_schema_actual
            .to_arrow(&ToArrowSettings::default())
            .unwrap();
        pretty_assertions::assert_eq!(arrow_schema_actual, arrow_schema);
    }

    #[test]
    fn test_schema_strip_encoding() {
        let odf_schema_orig = DataSchema::new_with_attrs(
            vec![
                DataField::string("utf8")
                    .encoding(ArrowBufferEncoding::Contiguous {
                        offset_bit_width: Some(32),
                    })
                    .extra_json(json!({"foo.com/foo": "a"})),
                DataField::string("utf8_view")
                    .encoding(ArrowBufferEncoding::View {
                        offset_bit_width: Some(32),
                    })
                    .extra_json(json!({"foo.com/foo": "b"})),
                DataField::date("date")
                    .encoding(ArrowDateEncoding {
                        unit: ArrowDateUnit::Day,
                    })
                    .extra_json(json!({"foo.com/foo": "c"})),
                DataField::date("date_millis")
                    .encoding(ArrowDateEncoding {
                        unit: ArrowDateUnit::Millisecond,
                    })
                    .extra_json(json!({"foo.com/foo": "d"})),
                DataField::decimal("decimal", 10, 5)
                    .encoding(ArrowDecimalEncoding { bit_width: 256 })
                    .extra_json(json!({"foo.com/foo": "e"})),
            ],
            ExtraAttributes::new_from_json(json!({
                "foo.com/bar": "x",
            }))
            .unwrap(),
        );

        pretty_assertions::assert_eq!(
            DataSchema {
                fields: vec![
                    DataField {
                        name: "utf8".into(),
                        r#type: DataType::String(DataTypeString {}),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                "foo.com/foo": "a",
                            }))
                            .unwrap(),
                        ),
                    },
                    DataField {
                        name: "utf8_view".into(),
                        r#type: DataType::String(DataTypeString {}),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                "foo.com/foo": "b",
                            }))
                            .unwrap(),
                        ),
                    },
                    DataField {
                        name: "date".into(),
                        r#type: DataType::Date(DataTypeDate {}),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                "foo.com/foo": "c",
                            }))
                            .unwrap(),
                        ),
                    },
                    DataField {
                        name: "date_millis".into(),
                        r#type: DataType::Date(DataTypeDate {}),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                "foo.com/foo": "d",
                            }))
                            .unwrap(),
                        ),
                    },
                    DataField {
                        name: "decimal".into(),
                        r#type: DataType::Decimal(DataTypeDecimal {
                            precision: 10,
                            scale: 5
                        }),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                "foo.com/foo": "e",
                            }))
                            .unwrap(),
                        ),
                    }
                ],
                extra: Some(
                    ExtraAttributes::new_from_json(json!({
                        "foo.com/bar": "x",
                    }))
                    .unwrap(),
                ),
            },
            odf_schema_orig.strip_encoding(),
        );
    }

    #[test]
    fn test_schema_to_arrow_default_view_ecoding() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("utf8", ArrowDataType::Utf8, false),
            ArrowField::new("binary", ArrowDataType::Binary, false),
        ]);

        let odf_schema = DataSchema::new_from_arrow(&arrow_schema).unwrap();
        pretty_assertions::assert_eq!(
            DataSchema::new(vec![
                DataField::string("utf8").encoding(ArrowBufferEncoding::Contiguous {
                    offset_bit_width: Some(32)
                }),
                DataField::binary("binary").encoding(ArrowBufferEncoding::Contiguous {
                    offset_bit_width: Some(32)
                })
            ]),
            odf_schema,
        );

        pretty_assertions::assert_eq!(
            odf_schema.to_arrow(&ToArrowSettings::default()).unwrap(),
            arrow_schema
        );

        // After stripping encoding to carry only logical types we should see View
        // encoding used by default, as it's much faster when reading from Parquet
        pretty_assertions::assert_eq!(
            odf_schema
                .strip_encoding()
                .to_arrow(&ToArrowSettings {
                    default_buffer_encoding: Some(ArrowBufferEncoding::View {
                        offset_bit_width: Some(32)
                    }),
                    default_date_encoding: None,
                })
                .unwrap(),
            ArrowSchema::new(vec![
                ArrowField::new("utf8", ArrowDataType::Utf8View, false),
                ArrowField::new("binary", ArrowDataType::BinaryView, false),
            ])
        );
    }
}
