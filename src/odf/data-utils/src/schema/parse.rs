// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Parses data type declarations into ODF schema.
///
/// Expects input is in form of CREATE TABLE parameters, e.g.:
///
///   "a STRING, b INT NOT NULL, c TIMESTAMP"
///
/// `force_utc_time` parameter is related to:
///     <https://github.com/apache/arrow-datafusion/issues/686>
///
/// `DataFusion` currently has inconsistent timezone handling behavior. It will
/// ensure that all times are parsed and adjusted if necessary to UTC timezone.
pub fn parse_ddl_to_odf_schema(
    ddl: &str,
) -> Result<odf_metadata::schema::DataSchema, odf_metadata::UnsupportedSchema> {
    use datafusion::sql::sqlparser::ast::{
        ColumnOption,
        ColumnOptionDef,
        DataType as SqlDataType,
        Statement,
    };
    use odf_metadata::schema::*;

    let sql = format!("create table x ({ddl})");
    let Statement::CreateTable(create_table) = Parser::parse_sql(&GenericDialect {}, &sql)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    else {
        unreachable!();
    };

    let mut fields = Vec::new();

    for col in create_table.columns {
        let mut data_type = match col.data_type {
            SqlDataType::Bool | SqlDataType::Boolean => DataType::bool().optional(),
            SqlDataType::Signed
            | SqlDataType::SignedInteger
            | SqlDataType::Int(_)
            | SqlDataType::Integer(_)
            | SqlDataType::Int32 => DataType::i32().optional(),
            SqlDataType::IntegerUnsigned(_)
            | SqlDataType::IntUnsigned(_)
            | SqlDataType::Unsigned
            | SqlDataType::UnsignedInteger
            | SqlDataType::UInt32 => DataType::u32().optional(),
            SqlDataType::BigInt(_) => DataType::i64().optional(),
            SqlDataType::BigIntUnsigned(_) => DataType::u64().optional(),
            SqlDataType::Float(_) | SqlDataType::Float32 => DataType::f32().optional(),
            SqlDataType::Double(_) => DataType::f64().optional(),
            SqlDataType::Date => DataType::date(),
            SqlDataType::Timestamp(_, _) => DataType::timestamp_millis_utc().optional(),
            SqlDataType::String(_)
            | SqlDataType::Varchar(_)
            | SqlDataType::Text
            | SqlDataType::TinyText
            | SqlDataType::MediumText
            | SqlDataType::LongText
            | SqlDataType::FixedString(_) => DataType::string().optional(),
            SqlDataType::Table(_)
            | SqlDataType::Character(_)
            | SqlDataType::Char(_)
            | SqlDataType::CharacterVarying(_)
            | SqlDataType::CharVarying(_)
            | SqlDataType::Nvarchar(_)
            | SqlDataType::Uuid
            | SqlDataType::CharacterLargeObject(_)
            | SqlDataType::CharLargeObject(_)
            | SqlDataType::Clob(_)
            | SqlDataType::Binary(_)
            | SqlDataType::Varbinary(_)
            | SqlDataType::Blob(_)
            | SqlDataType::TinyBlob
            | SqlDataType::MediumBlob
            | SqlDataType::LongBlob
            | SqlDataType::Bytes(_)
            | SqlDataType::Numeric(_)
            | SqlDataType::Decimal(_)
            | SqlDataType::BigNumeric(_)
            | SqlDataType::BigDecimal(_)
            | SqlDataType::Dec(_)
            | SqlDataType::TinyInt(_)
            | SqlDataType::TinyIntUnsigned(_)
            | SqlDataType::UTinyInt
            | SqlDataType::Int2(_)
            | SqlDataType::Int2Unsigned(_)
            | SqlDataType::SmallInt(_)
            | SqlDataType::SmallIntUnsigned(_)
            | SqlDataType::USmallInt
            | SqlDataType::MediumInt(_)
            | SqlDataType::MediumIntUnsigned(_)
            | SqlDataType::Int4(_)
            | SqlDataType::Int8(_)
            | SqlDataType::Int16
            | SqlDataType::Int64
            | SqlDataType::Int128
            | SqlDataType::Int256
            | SqlDataType::Int4Unsigned(_)
            | SqlDataType::UInt8
            | SqlDataType::UInt16
            | SqlDataType::UInt64
            | SqlDataType::UInt128
            | SqlDataType::UInt256
            | SqlDataType::Int8Unsigned(_)
            | SqlDataType::Float4
            | SqlDataType::Float64
            | SqlDataType::Real
            | SqlDataType::Float8
            | SqlDataType::DoublePrecision
            | SqlDataType::Date32
            | SqlDataType::Time(_, _)
            | SqlDataType::Datetime(_)
            | SqlDataType::Datetime64(_, _)
            | SqlDataType::Interval
            | SqlDataType::JSON
            | SqlDataType::JSONB
            | SqlDataType::Regclass
            | SqlDataType::Bytea
            | SqlDataType::Bit(_)
            | SqlDataType::BitVarying(_)
            | SqlDataType::VarBit(_)
            | SqlDataType::Custom(_, _)
            | SqlDataType::Array(_)
            | SqlDataType::Map(_, _)
            | SqlDataType::Tuple(_)
            | SqlDataType::Nested(_)
            | SqlDataType::Enum(_, _)
            | SqlDataType::Set(_)
            | SqlDataType::Struct(_, _)
            | SqlDataType::Union(_)
            | SqlDataType::Nullable(_)
            | SqlDataType::LowCardinality(_)
            | SqlDataType::Unspecified
            | SqlDataType::Trigger
            | SqlDataType::AnyType
            | SqlDataType::GeometricType(_)
            | SqlDataType::NamedTable { .. }
            | SqlDataType::HugeInt
            | SqlDataType::UHugeInt
            | SqlDataType::UBigInt
            | SqlDataType::TimestampNtz
            | SqlDataType::TsVector
            | SqlDataType::TsQuery => {
                return Err(UnsupportedSchema::new(format!(
                    "Unsupported SQL type when converting to ODF schema: {}",
                    col.data_type
                )));
            }
        };

        for opt in col.options {
            data_type = match opt {
                ColumnOptionDef {
                    name: None,
                    option: ColumnOption::NotNull,
                } => data_type.required(),
                _ => {
                    return Err(UnsupportedSchema::new(format!(
                        "Unsupported SQL column option: {opt:?}"
                    )));
                }
            }
        }

        fields.push(DataField::new(col.name.value, data_type));
    }

    Ok(DataSchema::new(fields))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Parses data type declarations into a `DataFusion` schema.
///
/// Expects input is in form of CREATE TABLE parameters, e.g.:
///
///   "a STRING, b INT NOT NULL, c TIMESTAMP"
///
/// `force_utc_time` parameter is related to:
///     <https://github.com/apache/arrow-datafusion/issues/686>
///
/// `DataFusion` currently has inconsistent timezone handling behavior. It will
/// ensure that all times are parsed and adjusted if necessary to UTC timezone.
pub async fn parse_ddl_to_datafusion_schema(
    ctx: &SessionContext,
    ddl: &str,
    force_utc_time: bool,
) -> Result<DFSchema, DataFusionError> {
    // TODO: SEC: should we worry about SQL injections?
    let sql = format!("create table x ({ddl})");

    if let Err(ParserError::ParserError(err)) = Parser::parse_sql(&GenericDialect {}, &sql)
        && let Some(err_index) = extract_column_index(&err)
        && let Some(invalid_field) = extract_problematic_field(&sql, err_index)
    {
        return Err(DataFusionError::SQL(
            Box::new(ParserError::ParserError(format!(
                "Argument '{invalid_field}' is invalid or a reserved keyword"
            ))),
            None,
        ));
    }

    let plan = ctx.state().create_logical_plan(&sql).await?;

    // Pull the schema out of the plan while avoiding cloning it
    let schema = Arc::clone(plan.schema());
    drop(plan);
    let schema = Arc::into_inner(schema).unwrap();

    let schema = if force_utc_time {
        do_force_utc_time(schema)
    } else {
        schema
    };

    Ok(schema)
}

/// Parses data type declarations into an Arrow schema.
///
/// Expects input is in form of CREATE TABLE parameters, e.g.:
///
///   "a STRING, b INT NOT NULL, c TIMESTAMP"
///
/// `force_utc_time` parameter is related to:
///     <https://github.com/apache/arrow-datafusion/issues/686>
///
/// `DataFusion` currently has inconsistent timezone handling behavior. It will
/// ensure that all times are parsed and adjusted if necessary to UTC timezone.
pub async fn parse_ddl_to_arrow_schema(
    ctx: &SessionContext,
    ddl: &str,
    force_utc_time: bool,
) -> Result<Schema, DataFusionError> {
    let df_schema = parse_ddl_to_datafusion_schema(ctx, ddl, force_utc_time).await?;
    Ok(df_schema.into())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TZ coercion
//
// This code should go away once this issue is resolved:
// <https://github.com/apache/arrow-datafusion/issues/686>
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn do_force_utc_time(schema: DFSchema) -> DFSchema {
    if !force_utc_time_applies(&schema) {
        return schema;
    }

    let mut fields = Vec::new();
    let metadata = schema.metadata().clone();

    let utc = Arc::from("UTC");

    for (i, field) in schema.fields().iter().enumerate() {
        let (qualifier, _) = schema.qualified_field(i);
        fields.push((qualifier.cloned(), force_utc_time_rec(field, &utc)));
    }

    DFSchema::new_with_metadata(fields, metadata).unwrap()
}

fn force_utc_time_rec(field: &Arc<Field>, tz: &Arc<str>) -> Arc<Field> {
    match field.data_type() {
        DataType::Timestamp(unit, None) => {
            let data_type = DataType::Timestamp(*unit, Some(tz.clone()));
            Arc::new(Field::new(field.name(), data_type, field.is_nullable()))
        }
        DataType::Struct(fields) => {
            let fields: Vec<_> = fields
                .iter()
                .map(|fr| force_utc_time_field_rec(fr.as_ref().clone(), tz))
                .collect();
            let data_type = DataType::Struct(fields.into());
            Arc::new(Field::new(field.name(), data_type, field.is_nullable()))
        }
        _ => field.clone(),
    }
}

fn force_utc_time_field_rec(field: Field, tz: &Arc<str>) -> Field {
    match field.data_type().clone() {
        DataType::Timestamp(unit, None) => {
            field.with_data_type(DataType::Timestamp(unit, Some(tz.clone())))
        }
        DataType::Struct(fields) => {
            let struct_fields: Vec<_> = fields
                .iter()
                .map(|fr| force_utc_time_field_rec(fr.as_ref().clone(), tz))
                .collect();
            field.with_data_type(DataType::Struct(struct_fields.into()))
        }
        _ => field,
    }
}

fn force_utc_time_applies(schema: &DFSchema) -> bool {
    for field in schema.fields() {
        if force_utc_time_applies_rec(field.data_type()) {
            return true;
        }
    }
    false
}

fn force_utc_time_applies_rec(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, None) => true,
        DataType::Struct(fields) => {
            for f in fields {
                if force_utc_time_applies_rec(f.data_type()) {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

fn extract_column_index(error_message: &str) -> Option<usize> {
    let column_marker = "Column: ";
    let index = error_message.find(column_marker)?;

    error_message[index + column_marker.len()..]
        .split_whitespace()
        .next()?
        .parse::<usize>()
        .ok()
}

fn extract_problematic_field(sql: &str, error_index: usize) -> Option<String> {
    // Step 1: Get the substring up to the error index
    let truncated = &sql[..error_index - 1];

    // Step 2: Find the last ',' or '(' before the error index
    let last_separator = truncated.rfind([',', '('])?;

    // Step 3: Extract the column definition
    let column_definition = truncated[(last_separator + 1)..].trim();

    Some(column_definition.to_string())
}
