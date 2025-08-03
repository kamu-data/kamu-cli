// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::data_encoding::DataEncoding;
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

    #[cfg(feature = "arrow")]
    pub fn new_from_arrow(value: &arrow::datatypes::Schema) -> Self {
        let mut fields = Vec::with_capacity(value.fields.len());
        for field in &value.fields {
            fields.push(DataField::new_from_arrow(field));
        }

        Self {
            fields,
            extra: None,
        }
    }

    #[cfg(feature = "arrow")]
    pub fn to_arrow(&self) -> arrow::datatypes::Schema {
        let mut fields = Vec::new();
        for field in &self.fields {
            fields.push(field.to_arrow());
        }
        arrow::datatypes::Schema::new(fields)
    }

    /// Strips all *known* encoding-related metadata from the schema fields,
    /// leaving bare logical types
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

    #[cfg(feature = "arrow")]
    fn new_from_arrow(value: &arrow::datatypes::Field) -> Self {
        let (r#type, encoding) = DataType::new_from_arrow(value.data_type(), value.is_nullable());

        let extra = encoding.as_ref().and_then(DataEncoding::to_extra);

        Self {
            name: value.name().clone(),
            r#type,
            extra,
        }
    }

    #[cfg(feature = "arrow")]
    pub fn to_arrow(&self) -> arrow::datatypes::Field {
        let encoding = self.extra.as_ref().and_then(DataEncoding::new_from_extra);
        let (data_type, nullable) = self.r#type.to_arrow(encoding.as_ref());
        arrow::datatypes::Field::new(&self.name, data_type, nullable)
    }

    pub fn strip_encoding(self) -> Self {
        Self {
            name: self.name,
            r#type: self.r#type.strip_encoding(),
            extra: self.extra.and_then(|e| {
                e.retain(|k, _| !DataEncoding::ALL_KEYS.contains(&k.as_str()))
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
    /// [`DataEncoding`]).
    #[cfg(feature = "arrow")]
    fn new_from_arrow(
        value: &arrow::datatypes::DataType,
        nullable: bool,
    ) -> (Self, Option<DataEncoding>) {
        use arrow::datatypes::DataType as ArrowDataType;

        let (odf_type, encoding) = match value {
            ArrowDataType::Null => (DataType::Null(DataTypeNull {}), None),
            ArrowDataType::Boolean => (DataType::Bool(DataTypeBool {}), None),
            ArrowDataType::Int8 => (
                DataType::Int(DataTypeInt {
                    bit_width: 8,
                    signed: true,
                }),
                None,
            ),
            ArrowDataType::Int16 => (
                DataType::Int(DataTypeInt {
                    bit_width: 16,
                    signed: true,
                }),
                None,
            ),
            ArrowDataType::Int32 => (
                DataType::Int(DataTypeInt {
                    bit_width: 32,
                    signed: true,
                }),
                None,
            ),
            ArrowDataType::Int64 => (
                DataType::Int(DataTypeInt {
                    bit_width: 64,
                    signed: true,
                }),
                None,
            ),
            ArrowDataType::UInt8 => (
                DataType::Int(DataTypeInt {
                    bit_width: 8,
                    signed: false,
                }),
                None,
            ),
            ArrowDataType::UInt16 => (
                DataType::Int(DataTypeInt {
                    bit_width: 16,
                    signed: false,
                }),
                None,
            ),
            ArrowDataType::UInt32 => (
                DataType::Int(DataTypeInt {
                    bit_width: 32,
                    signed: false,
                }),
                None,
            ),
            ArrowDataType::UInt64 => (
                DataType::Int(DataTypeInt {
                    bit_width: 64,
                    signed: false,
                }),
                None,
            ),
            ArrowDataType::Float16 => (DataType::Float(DataTypeFloat { bit_width: 16 }), None),
            ArrowDataType::Float32 => (DataType::Float(DataTypeFloat { bit_width: 32 }), None),
            ArrowDataType::Float64 => (DataType::Float(DataTypeFloat { bit_width: 64 }), None),
            ArrowDataType::Timestamp(time_unit, tz) => (
                DataType::Timestamp(DataTypeTimestamp {
                    unit: (*time_unit).into(),
                    timezone: tz.as_ref().map(std::string::ToString::to_string),
                }),
                None,
            ),
            ArrowDataType::Date32 => (
                DataType::Date(DataTypeDate {
                    unit: DateUnit::Day,
                    bit_width: 32,
                }),
                None,
            ),
            ArrowDataType::Date64 => (
                DataType::Date(DataTypeDate {
                    unit: DateUnit::Millisecond,
                    bit_width: 64,
                }),
                None,
            ),
            ArrowDataType::Time32(time_unit) => (
                DataType::Time(DataTypeTime {
                    unit: (*time_unit).into(),
                    bit_width: 32,
                }),
                None,
            ),
            ArrowDataType::Time64(time_unit) => (
                DataType::Time(DataTypeTime {
                    unit: (*time_unit).into(),
                    bit_width: 64,
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
                unimplemented!("Interval types are not yet supported in ODF schema")
            }
            ArrowDataType::Binary => (
                DataType::Binary(DataTypeBinary { fixed_length: None }),
                Some(DataEncoding::ArrowContiguousBuffer {
                    offset_bit_width: 32,
                }),
            ),
            ArrowDataType::LargeBinary => (
                DataType::Binary(DataTypeBinary { fixed_length: None }),
                Some(DataEncoding::ArrowContiguousBuffer {
                    offset_bit_width: 64,
                }),
            ),
            ArrowDataType::BinaryView => (
                DataType::Binary(DataTypeBinary { fixed_length: None }),
                Some(DataEncoding::ArrowViewBuffer {
                    offset_bit_width: 32,
                }),
            ),
            ArrowDataType::FixedSizeBinary(fixed_length) => (
                DataType::Binary(DataTypeBinary {
                    fixed_length: Some(u64::try_from(*fixed_length).unwrap()),
                }),
                None,
            ),
            ArrowDataType::Utf8 => (
                DataType::String(DataTypeString {}),
                Some(DataEncoding::ArrowContiguousBuffer {
                    offset_bit_width: 32,
                }),
            ),
            ArrowDataType::LargeUtf8 => (
                DataType::String(DataTypeString {}),
                Some(DataEncoding::ArrowContiguousBuffer {
                    offset_bit_width: 64,
                }),
            ),
            ArrowDataType::Utf8View => (
                DataType::String(DataTypeString {}),
                Some(DataEncoding::ArrowViewBuffer {
                    offset_bit_width: 32,
                }),
            ),
            ArrowDataType::List(field) => {
                let (item_type, encoding) =
                    DataType::new_from_arrow(field.data_type(), field.is_nullable());

                if let Some(encoding) = encoding {
                    unimplemented!(
                        "List item types with custom encoding {encoding:?} are not supported"
                    );
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
                    DataType::new_from_arrow(field.data_type(), field.is_nullable());

                if let Some(encoding) = encoding {
                    unimplemented!(
                        "List item types with custom encoding {encoding:?} are not supported"
                    );
                }

                (
                    DataType::List(DataTypeList {
                        item_type: Box::new(item_type),
                        fixed_length: None,
                    }),
                    Some(DataEncoding::ArrowViewBuffer {
                        offset_bit_width: 32,
                    }),
                )
            }
            ArrowDataType::FixedSizeList(field, fixed_length) => {
                let (item_type, encoding) =
                    DataType::new_from_arrow(field.data_type(), field.is_nullable());

                if let Some(encoding) = encoding {
                    unimplemented!(
                        "List item types with custom encoding {encoding:?} are not supported"
                    );
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
                    DataType::new_from_arrow(field.data_type(), field.is_nullable());

                if let Some(encoding) = encoding {
                    unimplemented!(
                        "List item types with custom encoding {encoding:?} are not supported"
                    );
                }

                (
                    DataType::List(DataTypeList {
                        item_type: Box::new(item_type),
                        fixed_length: None,
                    }),
                    Some(DataEncoding::ArrowContiguousBuffer {
                        offset_bit_width: 64,
                    }),
                )
            }
            ArrowDataType::LargeListView(field) => {
                let (item_type, encoding) =
                    DataType::new_from_arrow(field.data_type(), field.is_nullable());

                if let Some(encoding) = encoding {
                    unimplemented!(
                        "List item types with custom encoding {encoding:?} are not supported"
                    );
                }

                (
                    DataType::List(DataTypeList {
                        item_type: Box::new(item_type),
                        fixed_length: None,
                    }),
                    Some(DataEncoding::ArrowViewBuffer {
                        offset_bit_width: 64,
                    }),
                )
            }
            ArrowDataType::Struct(fields) => (
                DataType::Struct(DataTypeStruct {
                    children: fields
                        .iter()
                        .map(|f| DataField::new_from_arrow(f))
                        .collect(),
                }),
                None,
            ),
            ArrowDataType::Union(_, _) => {
                unimplemented!("Union types are not yet supported in ODF schema")
            }
            ArrowDataType::Dictionary(_, _) => {
                unimplemented!("Dictionary types are not yet supported in ODF schema")
            }
            ArrowDataType::Decimal128(precision, scale) => (
                DataType::Decimal(DataTypeDecimal {
                    precision: (*precision).into(),
                    scale: (*scale).into(),
                    bit_width: 128,
                }),
                None,
            ),
            ArrowDataType::Decimal256(precision, scale) => (
                DataType::Decimal(DataTypeDecimal {
                    precision: (*precision).into(),
                    scale: (*scale).into(),
                    bit_width: 256,
                }),
                None,
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
                );
                let (value_type, value_encoding) = DataType::new_from_arrow(
                    inner_fields[1].data_type(),
                    inner_fields[1].is_nullable(),
                );

                if let Some(key_encoding) = key_encoding {
                    unimplemented!(
                        "Map key types with custom encoding {key_encoding:?} are not supported"
                    );
                }

                if let Some(value_encoding) = value_encoding {
                    unimplemented!(
                        "Map value types with custom encoding {value_encoding:?} are not supported"
                    );
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
                let (run_ends_type, run_ends_encoding) =
                    DataType::new_from_arrow(run_ends.data_type(), run_ends.is_nullable());

                assert_eq!(
                    run_ends_encoding, None,
                    "Unexpected custom encoding on run ends type"
                );

                let run_ends_bit_width = match run_ends_type {
                    DataType::Int(DataTypeInt {
                        bit_width: 32,
                        signed: true,
                    }) => 32,
                    DataType::Int(DataTypeInt {
                        bit_width: 64,
                        signed: true,
                    }) => 64,
                    t => panic!("Expected signed Int type for run ends but got: {t:?}"),
                };

                let (value_type, value_encoding) =
                    DataType::new_from_arrow(values.data_type(), values.is_nullable());

                if let Some(value_encoding) = value_encoding {
                    unimplemented!(
                        "REE encoding of types with another layer of custom encoding \
                         {value_encoding:?} is not supported"
                    );
                }

                (
                    value_type,
                    Some(DataEncoding::ArrowRunEnd { run_ends_bit_width }),
                )
            }
        };

        if !nullable {
            (odf_type, encoding)
        } else {
            (
                DataType::Option(DataTypeOption {
                    inner: Box::new(odf_type),
                }),
                encoding,
            )
        }
    }

    #[cfg(feature = "arrow")]
    pub fn to_arrow(&self, encoding: Option<&DataEncoding>) -> (arrow::datatypes::DataType, bool) {
        use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};

        if let DataType::Option(DataTypeOption { inner }) = self {
            let (arrow_type, nullable) = inner.to_arrow(encoding);
            assert!(!nullable, "Nested Option types are not supported: {self:?}");
            return (arrow_type, true);
        }

        let unsupported =
            || panic!("Unsupported type / encoding combination: {self:?} {encoding:?}");

        let arrow_type = match encoding {
            // TODO: If None is to be considered a "defaut" and not as absence of encoding - would
            // our decisions change here?
            None => match self {
                DataType::Binary(DataTypeBinary { fixed_length: None }) => ArrowDataType::Binary,
                DataType::Binary(DataTypeBinary {
                    fixed_length: Some(fixed_length),
                }) => ArrowDataType::FixedSizeBinary(i32::try_from(*fixed_length).unwrap()),
                DataType::Bool(DataTypeBool {}) => ArrowDataType::Boolean,
                DataType::Date(DataTypeDate {
                    unit: DateUnit::Day,
                    bit_width: 32,
                }) => ArrowDataType::Date32,
                DataType::Date(DataTypeDate {
                    unit: DateUnit::Millisecond,
                    bit_width: 64,
                }) => ArrowDataType::Date64,
                DataType::Date(_) => unsupported(),
                DataType::Decimal(DataTypeDecimal {
                    precision,
                    scale,
                    bit_width: 128,
                }) => ArrowDataType::Decimal128(
                    u8::try_from(*precision).unwrap(),
                    i8::try_from(*scale).unwrap(),
                ),
                DataType::Decimal(DataTypeDecimal {
                    precision,
                    scale,
                    bit_width: 256,
                }) => ArrowDataType::Decimal256(
                    u8::try_from(*precision).unwrap(),
                    i8::try_from(*scale).unwrap(),
                ),
                DataType::Decimal(DataTypeDecimal {
                    precision: _,
                    scale: _,
                    bit_width,
                }) => panic!("Unsupported Decimal bit width {bit_width}"),
                DataType::Duration(DataTypeDuration { unit }) => {
                    ArrowDataType::Duration((*unit).into())
                }
                DataType::Float(DataTypeFloat { bit_width: 16 }) => ArrowDataType::Float16,
                DataType::Float(DataTypeFloat { bit_width: 32 }) => ArrowDataType::Float32,
                DataType::Float(DataTypeFloat { bit_width: 64 }) => ArrowDataType::Float64,
                DataType::Float(DataTypeFloat { bit_width }) => {
                    panic!("Unsupported Float bit width {bit_width}")
                }
                DataType::Int(DataTypeInt {
                    bit_width: 8,
                    signed: true,
                }) => ArrowDataType::Int8,
                DataType::Int(DataTypeInt {
                    bit_width: 8,
                    signed: false,
                }) => ArrowDataType::UInt8,
                DataType::Int(DataTypeInt {
                    bit_width: 16,
                    signed: true,
                }) => ArrowDataType::Int16,
                DataType::Int(DataTypeInt {
                    bit_width: 16,
                    signed: false,
                }) => ArrowDataType::UInt16,
                DataType::Int(DataTypeInt {
                    bit_width: 32,
                    signed: true,
                }) => ArrowDataType::Int32,
                DataType::Int(DataTypeInt {
                    bit_width: 32,
                    signed: false,
                }) => ArrowDataType::UInt32,
                DataType::Int(DataTypeInt {
                    bit_width: 64,
                    signed: true,
                }) => ArrowDataType::Int64,
                DataType::Int(DataTypeInt {
                    bit_width: 64,
                    signed: false,
                }) => ArrowDataType::UInt64,
                DataType::Int(DataTypeInt {
                    bit_width,
                    signed: _,
                }) => panic!("Unsupported Int bit width {bit_width}"),
                DataType::List(DataTypeList {
                    item_type,
                    fixed_length: None,
                }) => {
                    let (item_type_arrow, nullable) = item_type.to_arrow(None);
                    ArrowDataType::List(
                        ArrowField::new_list_field(item_type_arrow, nullable).into(),
                    )
                }
                DataType::List(DataTypeList {
                    item_type,
                    fixed_length: Some(fixed_length),
                }) => {
                    let (item_type_arrow, nullable) = item_type.to_arrow(None);
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
                    let (key_type_arrow, key_type_nullable) = key_type.to_arrow(None);
                    let (value_type_arrow, value_type_nullable) = value_type.to_arrow(None);

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
                DataType::Struct(DataTypeStruct { children }) => {
                    let fields: Vec<_> = children.iter().map(DataField::to_arrow).collect();
                    ArrowDataType::Struct(arrow::datatypes::Fields::from(fields))
                }
                DataType::Time(DataTypeTime {
                    unit,
                    bit_width: 32,
                }) => ArrowDataType::Time32((*unit).into()),
                DataType::Time(DataTypeTime {
                    unit,
                    bit_width: 64,
                }) => ArrowDataType::Time64((*unit).into()),
                DataType::Time(DataTypeTime { unit: _, bit_width }) => {
                    panic!("Unsupported Time bit width {bit_width}")
                }
                DataType::Timestamp(DataTypeTimestamp { unit, timezone }) => {
                    ArrowDataType::Timestamp(
                        (*unit).into(),
                        timezone.as_ref().map(|s| s.as_str().into()),
                    )
                }
                DataType::String(DataTypeString {}) => ArrowDataType::Utf8,
                DataType::Option(_) => unreachable!(),
            },
            Some(DataEncoding::ArrowContiguousBuffer {
                offset_bit_width: 32,
            }) => match self {
                DataType::Binary(DataTypeBinary { fixed_length: None }) => ArrowDataType::Binary,

                DataType::List(DataTypeList {
                    item_type,
                    fixed_length: None,
                }) => {
                    let (item_type_arrow, nullable) = item_type.to_arrow(None);
                    ArrowDataType::List(
                        ArrowField::new_list_field(item_type_arrow, nullable).into(),
                    )
                }
                DataType::String(DataTypeString {}) => ArrowDataType::Utf8,
                DataType::Binary(_)
                | DataType::Bool(_)
                | DataType::Date(_)
                | DataType::Decimal(_)
                | DataType::Duration(_)
                | DataType::Float(_)
                | DataType::Int(_)
                | DataType::List(_)
                | DataType::Map(_)
                | DataType::Null(_)
                | DataType::Struct(_)
                | DataType::Time(_)
                | DataType::Timestamp(_) => unsupported(),
                DataType::Option(_) => unreachable!(),
            },
            Some(DataEncoding::ArrowContiguousBuffer {
                offset_bit_width: 64,
            }) => match self {
                DataType::Binary(DataTypeBinary { fixed_length: None }) => {
                    ArrowDataType::LargeBinary
                }

                DataType::List(DataTypeList {
                    item_type,
                    fixed_length: None,
                }) => {
                    let (item_type_arrow, nullable) = item_type.to_arrow(None);
                    ArrowDataType::LargeList(
                        ArrowField::new_list_field(item_type_arrow, nullable).into(),
                    )
                }
                DataType::String(DataTypeString {}) => ArrowDataType::LargeUtf8,
                DataType::Binary(_)
                | DataType::Bool(_)
                | DataType::Date(_)
                | DataType::Decimal(_)
                | DataType::Duration(_)
                | DataType::Float(_)
                | DataType::Int(_)
                | DataType::List(_)
                | DataType::Map(_)
                | DataType::Null(_)
                | DataType::Struct(_)
                | DataType::Time(_)
                | DataType::Timestamp(_) => unsupported(),
                DataType::Option(_) => unreachable!(),
            },
            Some(DataEncoding::ArrowViewBuffer {
                offset_bit_width: 32,
            }) => match self {
                DataType::Binary(DataTypeBinary { fixed_length: None }) => {
                    ArrowDataType::BinaryView
                }
                DataType::List(DataTypeList {
                    item_type,
                    fixed_length: None,
                }) => {
                    let (item_type_arrow, nullable) = item_type.to_arrow(None);
                    ArrowDataType::ListView(
                        ArrowField::new_list_field(item_type_arrow, nullable).into(),
                    )
                }
                DataType::String(DataTypeString {}) => ArrowDataType::Utf8View,
                DataType::Binary(_)
                | DataType::Bool(_)
                | DataType::Date(_)
                | DataType::Decimal(_)
                | DataType::Duration(_)
                | DataType::Float(_)
                | DataType::Int(_)
                | DataType::List(_)
                | DataType::Map(_)
                | DataType::Null(_)
                | DataType::Struct(_)
                | DataType::Time(_)
                | DataType::Timestamp(_) => unsupported(),
                DataType::Option(_) => unreachable!(),
            },
            Some(DataEncoding::ArrowViewBuffer {
                offset_bit_width: 64,
            }) => match self {
                DataType::List(DataTypeList {
                    item_type,
                    fixed_length: None,
                }) => {
                    let (item_type_arrow, nullable) = item_type.to_arrow(None);
                    ArrowDataType::LargeListView(
                        ArrowField::new_list_field(item_type_arrow, nullable).into(),
                    )
                }
                DataType::Binary(_)
                | DataType::Bool(_)
                | DataType::Date(_)
                | DataType::Decimal(_)
                | DataType::Duration(_)
                | DataType::Float(_)
                | DataType::Int(_)
                | DataType::List(_)
                | DataType::Map(_)
                | DataType::Null(_)
                | DataType::String(_)
                | DataType::Struct(_)
                | DataType::Time(_)
                | DataType::Timestamp(_) => unsupported(),
                DataType::Option(_) => unreachable!(),
            },
            Some(
                DataEncoding::ArrowContiguousBuffer {
                    offset_bit_width: _,
                }
                | DataEncoding::ArrowViewBuffer {
                    offset_bit_width: _,
                }
                | DataEncoding::ArrowRunEnd {
                    run_ends_bit_width: _,
                },
            ) => unsupported(),
        };

        (arrow_type, false)
    }

    pub fn strip_encoding(self) -> Self {
        match self {
            DataType::Option(DataTypeOption { inner }) => DataType::Option(DataTypeOption {
                inner: Box::new(inner.strip_encoding()),
            }),
            DataType::Struct(DataTypeStruct { children }) => DataType::Struct(DataTypeStruct {
                children: children
                    .into_iter()
                    .map(DataField::strip_encoding)
                    .collect(),
            }),
            DataType::Binary(_)
            | DataType::Bool(_)
            | DataType::Date(_)
            | DataType::Decimal(_)
            | DataType::Duration(_)
            | DataType::Float(_)
            | DataType::Int(_)
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
    fn test_flied_from_to_arrow() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("utf8", ArrowDataType::Utf8, false),
            ArrowField::new("utf8_large", ArrowDataType::LargeUtf8, false),
            ArrowField::new("utf8_view", ArrowDataType::Utf8View, false),
        ]);

        let odf_schema_actual = DataSchema::new_from_arrow(&arrow_schema);
        pretty_assertions::assert_eq!(
            DataSchema {
                fields: vec![
                    DataField {
                        name: "utf8".into(),
                        r#type: DataType::String(DataTypeString {}),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                "arrow.apache.org/encoding": "contiguous",
                                "arrow.apache.org/offsetBitWidth": "32",
                            }))
                            .unwrap()
                        )
                    },
                    DataField {
                        name: "utf8_large".into(),
                        r#type: DataType::String(DataTypeString {}),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                "arrow.apache.org/encoding": "contiguous",
                                "arrow.apache.org/offsetBitWidth": "64",
                            }))
                            .unwrap()
                        )
                    },
                    DataField {
                        name: "utf8_view".into(),
                        r#type: DataType::String(DataTypeString {}),
                        extra: Some(
                            ExtraAttributes::new_from_json(json!({
                                "arrow.apache.org/encoding": "view",
                                "arrow.apache.org/offsetBitWidth": "32",
                            }))
                            .unwrap()
                        )
                    }
                ],
                extra: None,
            },
            odf_schema_actual,
        );

        let arrow_schema_actual = odf_schema_actual.to_arrow();
        pretty_assertions::assert_eq!(arrow_schema_actual, arrow_schema);
    }

    #[test]
    fn test_schema_strip_encoding() {
        let odf_schema_orig = DataSchema {
            fields: vec![DataField {
                name: "foo".into(),
                r#type: DataType::String(DataTypeString {}),
                extra: Some(
                    ExtraAttributes::new_from_json(json!({
                        "arrow.apache.org/encoding": "contiguous",
                        "arrow.apache.org/offsetBitWidth": "32",
                        "foo.com/foo": "bar",
                    }))
                    .unwrap(),
                ),
            }],
            extra: Some(
                ExtraAttributes::new_from_json(json!({
                    "foo.com/bar": "baz",
                }))
                .unwrap(),
            ),
        };

        pretty_assertions::assert_eq!(
            DataSchema {
                fields: vec![DataField {
                    name: "foo".into(),
                    r#type: DataType::String(DataTypeString {}),
                    extra: Some(
                        ExtraAttributes::new_from_json(json!({
                            "foo.com/foo": "bar",
                        }))
                        .unwrap(),
                    ),
                }],
                extra: Some(
                    ExtraAttributes::new_from_json(json!({
                        "foo.com/bar": "baz",
                    }))
                    .unwrap(),
                ),
            },
            odf_schema_orig.strip_encoding(),
        );
    }
}
