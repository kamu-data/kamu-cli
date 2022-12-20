// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;
use std::sync::Arc;

use datafusion::common::DFSchema;
use datafusion::parquet::basic::Type as PhysicalType;
use datafusion::parquet::basic::{ConvertedType, LogicalType, TimeUnit};
use datafusion::parquet::schema::types::Type;

/// Prints schema in a style of `parquet-schema` output
pub fn write_schema_parquet(output: &mut dyn Write, schema: &Type) -> Result<(), std::io::Error> {
    datafusion::parquet::schema::printer::print_schema(output, schema);
    Ok(())
}

/// Similar to [`print_schema_parquet`], but uses JSON format that does not require a custom parser
pub fn write_schema_parquet_json(
    output: &mut dyn Write,
    schema: &Type,
) -> Result<(), std::io::Error> {
    let mut writer = ParquetJsonSchemaWriter::new(output);
    writer.write(schema)?;
    Ok(())
}

pub fn dataframe_schema_to_parquet_schema(df_schema: &DFSchema) -> Arc<Type> {
    let arrow_schema: datafusion::arrow::datatypes::Schema = df_schema.into();
    let parquet_schema =
        datafusion::parquet::arrow::arrow_to_parquet_schema(&arrow_schema).unwrap();
    parquet_schema.root_schema_ptr()
}

/////////////////////////////////////////////////////////////////////////////////////////

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
                    r#"{{"name": "{}", "repetition": "{}""#,
                    basic_info.name(),
                    basic_info.repetition()
                )?;

                match physical_type {
                    PhysicalType::FIXED_LEN_BYTE_ARRAY => write!(
                        self.output,
                        r#", "type": "{}({})""#,
                        physical_type, type_length
                    )?,
                    _ => write!(self.output, r#", "type": "{}""#, physical_type)?,
                };

                // Also print logical type if it is available
                // If there is a logical type, do not print converted type
                let logical_type_str = Self::format_logical_and_converted(
                    &basic_info.logical_type(),
                    basic_info.converted_type(),
                    precision,
                    scale,
                );

                if !logical_type_str.is_empty() {
                    write!(self.output, r#", "logicalType": "{}""#, logical_type_str)?;
                }

                write!(self.output, "}}")?;
            }
            Type::GroupType {
                ref basic_info,
                ref fields,
            } => {
                write!(
                    self.output,
                    r#"{{"name": "{}", "type": "struct""#,
                    basic_info.name()
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
        logical_type: &Option<LogicalType>,
        converted_type: ConvertedType,
        precision: i32,
        scale: i32,
    ) -> String {
        match logical_type {
            Some(logical_type) => match logical_type {
                LogicalType::Integer {
                    bit_width,
                    is_signed,
                } => {
                    format!("INTEGER({},{})", bit_width, is_signed)
                }
                LogicalType::Decimal { precision, scale } => {
                    format!("DECIMAL({},{})", precision, scale)
                }
                LogicalType::Timestamp {
                    is_adjusted_to_u_t_c,
                    unit,
                } => {
                    format!(
                        "TIMESTAMP({},{})",
                        Self::print_timeunit(&unit),
                        is_adjusted_to_u_t_c
                    )
                }
                LogicalType::Time {
                    is_adjusted_to_u_t_c,
                    unit,
                } => {
                    format!(
                        "TIME({},{})",
                        Self::print_timeunit(&unit),
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
                    ConvertedType::NONE => format!(""),
                    decimal @ ConvertedType::DECIMAL => {
                        // For decimal type we should print precision and scale if they
                        // are > 0, e.g. DECIMAL(9, 2) -
                        // DECIMAL(9) - DECIMAL
                        let precision_scale = match (precision, scale) {
                            (p, s) if p > 0 && s > 0 => {
                                format!("({},{})", p, s)
                            }
                            (p, 0) if p > 0 => format!("({})", p),
                            _ => format!(""),
                        };
                        format!("{}{}", decimal, precision_scale)
                    }
                    other_converted_type => {
                        format!("{}", other_converted_type)
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

/////////////////////////////////////////////////////////////////////////////////////////

trait ParquetSchemaFormatter {
    fn begin_group();
    fn primitive();
    fn end_group();
}

/////////////////////////////////////////////////////////////////////////////////////////

struct ParquetStyleSchemaFormatter;

impl ParquetSchemaFormatter for ParquetStyleSchemaFormatter {
    fn begin_group() {
        todo!()
    }

    fn primitive() {
        todo!()
    }

    fn end_group() {
        todo!()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct ParquetJsonSchemaFormatter;

impl ParquetSchemaFormatter for ParquetJsonSchemaFormatter {
    fn begin_group() {
        todo!()
    }

    fn primitive() {
        todo!()
    }

    fn end_group() {
        todo!()
    }
}
