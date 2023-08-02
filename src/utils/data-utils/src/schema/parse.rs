// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::Schema;
use datafusion::common::DFSchemaRef;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;

pub async fn parse_ddl_to_datafusion_schema(
    ctx: &SessionContext,
    ddl_schema: &Vec<String>,
) -> Result<DFSchemaRef, DataFusionError> {
    // TODO: SEC: should we worry about SQL injections?
    let sql = format!("create table x ({})", ddl_schema.join(","));
    let plan = ctx.state().create_logical_plan(&sql).await?;
    Ok(plan.schema().clone())
}

pub async fn parse_ddl_to_arrow_schema(
    ctx: &SessionContext,
    ddl_schema: &Vec<String>,
) -> Result<Schema, DataFusionError> {
    // TODO: SEC: should we worry about SQL injections?
    let sql = format!("create table x ({})", ddl_schema.join(","));
    let plan = ctx.state().create_logical_plan(&sql).await?;
    let schema: Schema = plan.schema().as_ref().clone().into();
    Ok(schema)
}
