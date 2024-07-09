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
