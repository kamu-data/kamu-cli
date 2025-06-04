// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;

use datafusion::arrow::datatypes::Schema;
use datafusion::parquet::basic::{ConvertedType, LogicalType, TimeUnit, Type as PhysicalType};
use datafusion::parquet::schema::types::Type;
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Prints schema in a style of `parquet-schema` output
pub fn write_schema_parquet(output: &mut dyn Write, schema: &Type) -> Result<(), std::io::Error> {
    datafusion::parquet::schema::printer::print_schema(output, schema);
    Ok(())
}

/// Same as [`write_schema_parquet`] but outputs into a String
pub fn format_schema_parquet(schema: &Type) -> String {
    let mut buf = Vec::new();
    write_schema_parquet(&mut buf, schema).unwrap();
    String::from_utf8(buf).unwrap()
}

/// Similar to [`write_schema_parquet`], but uses JSON format that does not
/// require a custom parser
pub fn write_schema_parquet_json(
    output: &mut dyn Write,
    schema: &Type,
) -> Result<(), std::io::Error> {
    let mut writer = ParquetJsonSchemaWriter::new(output);
    writer.write(schema)?;
    Ok(())
}

pub fn write_schema_arrow_json(
    output: &mut dyn Write,
    schema: &Schema,
) -> Result<(), std::io::Error> {
    serde_json::to_writer(output, schema)?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Prints arrow schema to output
/// TODO: It currently converts schema to Parquet, but we will avoid this in the
/// future to preserve most descriptive types.
pub fn write_schema_arrow(output: &mut dyn Write, schema: &Schema) -> Result<(), std::io::Error> {
    let parquet_schema = crate::schema::convert::arrow_schema_to_parquet_schema(schema);
    write_schema_parquet(output, &parquet_schema)
}

/// Same as [`write_schema_arrow`] but outputs into a String
pub fn format_schema_arrow(schema: &Schema) -> String {
    let mut buf = Vec::new();
    write_schema_arrow(&mut buf, schema).unwrap();
    String::from_utf8(buf).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
struct ParquetJsonSchemaWriter<'a> {
    output: &'a mut dyn Write,
}

impl<'a> ParquetJsonSchemaWriter<'a> {
    pub fn new(output: &'a mut dyn Write) -> Self {
        Self { output }
    }

    pub fn write(&mut self, tp: &Type) -> Result<(), std::io::Error> {
        match *tp {
            Type::PrimitiveType {
                ref basic_info,
                physical_type,
                type_length,
                scale,
                precision,
            } => {
                write!(
                    self.output,
                    r#"{{"name": {}, "repetition": "{}""#,
                    JsonEscapedString(basic_info.name()),
                    basic_info.repetition()
                )?;

                match physical_type {
                    PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                        write!(self.output, r#", "type": "{physical_type}({type_length})""#)?;
                    }
                    _ => write!(self.output, r#", "type": "{physical_type}""#)?,
                }

                // Also print logical type if it is available
                // If there is a logical type, do not print converted type
                let logical_type_str = Self::format_logical_and_converted(
                    basic_info.logical_type().as_ref(),
                    basic_info.converted_type(),
                    precision,
                    scale,
                );

                if !logical_type_str.is_empty() {
                    write!(self.output, r#", "logicalType": "{logical_type_str}""#)?;
                }

                write!(self.output, "}}")?;
            }
            Type::GroupType {
                ref basic_info,
                ref fields,
            } => {
                write!(
                    self.output,
                    r#"{{"name": {}, "type": "struct""#,
                    JsonEscapedString(basic_info.name())
                )?;

                if basic_info.has_repetition() {
                    write!(
                        self.output,
                        r#", "repetition": "{}""#,
                        basic_info.repetition()
                    )?;
                }

                write!(self.output, r#", "fields": ["#,)?;

                for (i, field) in fields.iter().enumerate() {
                    self.write(field)?;
                    if i != fields.len() - 1 {
                        write!(self.output, ", ")?;
                    }
                }

                write!(self.output, "]}}")?;
            }
        }
        Ok(())
    }

    fn format_logical_and_converted(
        logical_type: Option<&LogicalType>,
        converted_type: ConvertedType,
        precision: i32,
        scale: i32,
    ) -> String {
        match logical_type {
            Some(logical_type) => match logical_type {
                LogicalType::Float16 => "FLOAT16".to_string(),
                LogicalType::Integer {
                    bit_width,
                    is_signed,
                } => {
                    format!("INTEGER({bit_width},{is_signed})")
                }
                LogicalType::Decimal { precision, scale } => {
                    format!("DECIMAL({precision},{scale})")
                }
                LogicalType::Timestamp {
                    is_adjusted_to_u_t_c,
                    unit,
                } => {
                    format!(
                        "TIMESTAMP({},{})",
                        Self::print_timeunit(unit),
                        is_adjusted_to_u_t_c
                    )
                }
                LogicalType::Time {
                    is_adjusted_to_u_t_c,
                    unit,
                } => {
                    format!(
                        "TIME({},{})",
                        Self::print_timeunit(unit),
                        is_adjusted_to_u_t_c
                    )
                }
                LogicalType::Date => "DATE".to_string(),
                LogicalType::Bson => "BSON".to_string(),
                LogicalType::Json => "JSON".to_string(),
                LogicalType::String => "STRING".to_string(),
                LogicalType::Uuid => "UUID".to_string(),
                LogicalType::Enum => "ENUM".to_string(),
                LogicalType::List => "LIST".to_string(),
                LogicalType::Map => "MAP".to_string(),
                LogicalType::Unknown => "UNKNOWN".to_string(),
            },
            None => {
                // Also print converted type if it is available
                match converted_type {
                    ConvertedType::NONE => String::new(),
                    decimal @ ConvertedType::DECIMAL => {
                        // For decimal type we should print precision and scale if they
                        // are > 0, e.g. DECIMAL(9, 2) -
                        // DECIMAL(9) - DECIMAL
                        let precision_scale = match (precision, scale) {
                            (p, s) if p > 0 && s > 0 => {
                                format!("({p},{s})")
                            }
                            (p, 0) if p > 0 => format!("({p})"),
                            _ => String::new(),
                        };
                        format!("{decimal}{precision_scale}")
                    }
                    other_converted_type => {
                        format!("{other_converted_type}")
                    }
                }
            }
        }
    }

    fn print_timeunit(unit: &TimeUnit) -> &str {
        match unit {
            TimeUnit::MILLIS(_) => "MILLIS",
            TimeUnit::MICROS(_) => "MICROS",
            TimeUnit::NANOS(_) => "NANOS",
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct JsonEscapedString<'a>(&'a str);

impl std::fmt::Display for JsonEscapedString<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use serde::Serializer;

        // TODO: PERF: Find a way to avoid allocation and write directly into
        // formatter's buffer
        let mut buf = Vec::new();
        let mut serializer = serde_json::Serializer::new(&mut buf);
        serializer.serialize_str(self.0).unwrap();
        write!(f, "{}", std::str::from_utf8(&buf).unwrap())?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
