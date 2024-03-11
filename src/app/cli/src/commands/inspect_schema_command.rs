// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::parquet::schema::types::Type;
use kamu::domain::*;
use opendatafabric::*;

use super::{CLIError, Command};

pub struct InspectSchemaCommand {
    query_svc: Arc<dyn QueryService>,
    dataset_ref: DatasetRef,
    output_format: Option<String>,
}

impl InspectSchemaCommand {
    pub fn new(
        query_svc: Arc<dyn QueryService>,
        dataset_ref: DatasetRef,
        output_format: Option<&str>,
    ) -> Self {
        Self {
            query_svc,
            dataset_ref,
            output_format: output_format.map(ToOwned::to_owned),
        }
    }

    fn print_schema_ddl(&self, schema: &Type) {
        match schema {
            Type::GroupType { fields, .. } => {
                for field in fields {
                    self.print_schema_ddl_rec(field, 0);
                }
            }
            _ => unreachable!(),
        }
    }

    // TODO: This produces DDL that doesn't match the SQL types we use elsewhere
    fn print_schema_ddl_rec(&self, t: &Type, depth: i32) {
        use datafusion::parquet::basic::{ConvertedType, Type as BasicType};

        self.indent(depth);

        match t {
            Type::PrimitiveType {
                basic_info,
                physical_type,
                scale,
                precision,
                ..
            } => {
                print!("{} ", console::style(basic_info.name()).bold());
                let typ = if *precision > 0 {
                    format!("DECIMAL({precision}, {scale})")
                } else if basic_info.converted_type() == ConvertedType::UTF8 {
                    "STRING".to_string()
                } else if basic_info.converted_type() != ConvertedType::NONE {
                    format!("{:?}", basic_info.converted_type())
                } else if *physical_type == BasicType::INT96 {
                    "TIMESTAMP".to_string()
                } else {
                    format!("{physical_type:?}")
                };
                print!("{}", console::style(typ).cyan());
            }
            Type::GroupType { basic_info, fields } => {
                print!("{} ", console::style(basic_info.name()).bold());
                println!("{}", console::style("STRUCT<").cyan());
                for field in fields {
                    self.print_schema_ddl_rec(field, depth + 1);
                }
                self.indent(depth);
                print!("{}", console::style(">").cyan());
            }
        }
        println!("{}", console::style(",").dim());
    }

    fn indent(&self, depth: i32) {
        for _ in 0..depth {
            print!("  ");
        }
    }

    fn print_schema_parquet(&self, schema: &Type) -> Result<(), CLIError> {
        kamu_data_utils::schema::format::write_schema_parquet(&mut std::io::stdout(), schema)?;
        Ok(())
    }

    fn query_errors(e: QueryError) -> CLIError {
        match e {
            QueryError::DatasetNotFound(e) => CLIError::usage_error_from(e),
            QueryError::DatasetSchemaNotAvailable(_) => unreachable!(),
            e @ (QueryError::DataFusionError(_) | QueryError::Access(_)) => CLIError::failure(e),
            e @ QueryError::Internal(_) => CLIError::critical(e),
        }
    }

    async fn get_parquet_schema(&mut self) -> Result<Type, CLIError> {
        Ok(self
            .query_svc
            .get_schema_parquet(&self.dataset_ref)
            .await
            .map_err(Self::query_errors)?
            .unwrap())
    }
}

#[async_trait::async_trait(? Send)]
impl Command for InspectSchemaCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        match self.output_format.as_deref() {
            None | Some("ddl") => {
                let schema = self.get_parquet_schema().await?;
                self.print_schema_ddl(&schema)
            }
            Some("parquet-json") => {
                let schema = self.get_parquet_schema().await?;
                self.print_schema_parquet(&schema)?
            }
            Some("arrow-json") => {
                let arrow = self
                    .query_svc
                    .get_schema(&self.dataset_ref)
                    .await
                    .map_err(Self::query_errors)?
                    .unwrap();
                let schema = SetDataSchema::from(arrow).schema_as_arrow().unwrap();
                kamu_data_utils::schema::format::write_schema_arrow_json(
                    &mut std::io::stdout(),
                    &schema,
                )?;
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}
