// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::parquet::schema::types::Type;
use internal_error::*;
use kamu::domain::*;

use super::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum SchemaOutputFormat {
    ArrowJson,
    Ddl,
    OdfJson,
    OdfYaml,
    Parquet,
    ParquetJson,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct InspectSchemaCommand {
    query_svc: Arc<dyn QueryService>,

    #[dill::component(explicit)]
    dataset_ref: odf::DatasetRef,

    #[dill::component(explicit)]
    output_format: Option<SchemaOutputFormat>,

    #[dill::component(explicit)]
    from_data_file: bool,
}

impl InspectSchemaCommand {
    fn print_schema_unavailable(&self) {
        eprintln!(
            "{}: Dataset schema is not yet available: {}",
            console::style("Warning").yellow(),
            self.dataset_ref.alias().unwrap(),
        );
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

    fn query_errors(e: QueryError) -> CLIError {
        match e {
            QueryError::DatasetNotFound(e) => CLIError::usage_error_from(e),
            QueryError::DatasetBlockNotFound(_)
            | QueryError::BadQuery(_)
            | QueryError::Access(_) => CLIError::failure(e),
            QueryError::Internal(_) => CLIError::critical(e),
        }
    }

    async fn get_odf_schema(&self) -> Result<Option<Arc<odf::schema::DataSchema>>, CLIError> {
        if !self.from_data_file {
            self.query_svc
                .get_schema(&self.dataset_ref)
                .await
                .map_err(Self::query_errors)
        } else {
            let Some(arrow_schema) = Box::pin(self.get_arrow_schema()).await? else {
                return Ok(None);
            };
            let odf_schema = odf::schema::DataSchema::new_from_arrow(&arrow_schema).int_err()?;
            Ok(Some(Arc::new(odf_schema)))
        }
    }

    async fn get_arrow_schema(&self) -> Result<Option<SchemaRef>, CLIError> {
        if !self.from_data_file {
            let Some(odf_schema) = Box::pin(self.get_odf_schema()).await? else {
                return Ok(None);
            };
            let arrow_schema = odf_schema
                .to_arrow(&odf::metadata::ToArrowSettings::default())
                .int_err()?;
            Ok(Some(Arc::new(arrow_schema)))
        } else {
            let schema = self
                .query_svc
                .get_last_data_chunk_schema_arrow(&self.dataset_ref)
                .await
                .map_err(Self::query_errors)?;

            Ok(schema)
        }
    }

    async fn get_parquet_schema(&self) -> Result<Option<Arc<Type>>, CLIError> {
        if !self.from_data_file {
            let arrow_schema = Box::pin(self.get_arrow_schema()).await?;
            Ok(arrow_schema
                .as_deref()
                .map(odf::utils::schema::convert::arrow_schema_to_parquet_schema))
        } else {
            let schema = self
                .query_svc
                .get_last_data_chunk_schema_parquet(&self.dataset_ref)
                .await
                .map_err(Self::query_errors)?;

            Ok(schema.map(Arc::new))
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for InspectSchemaCommand {
    async fn run(&self) -> Result<(), CLIError> {
        match self.output_format {
            None | Some(SchemaOutputFormat::Ddl) => {
                if let Some(schema) = self.get_parquet_schema().await? {
                    self.print_schema_ddl(&schema);
                } else {
                    self.print_schema_unavailable();
                }
            }
            Some(SchemaOutputFormat::Parquet) => {
                if let Some(schema) = self.get_parquet_schema().await? {
                    odf::utils::schema::format::write_schema_parquet(
                        &mut std::io::stdout(),
                        &schema,
                    )?;
                } else {
                    self.print_schema_unavailable();
                }
            }
            Some(SchemaOutputFormat::ParquetJson) => {
                if let Some(schema) = self.get_parquet_schema().await? {
                    odf::utils::schema::format::write_schema_parquet_json(
                        &mut std::io::stdout(),
                        &schema,
                    )?;
                } else {
                    self.print_schema_unavailable();
                }
            }
            Some(SchemaOutputFormat::ArrowJson) => {
                if let Some(schema) = self.get_arrow_schema().await? {
                    odf::utils::schema::format::write_schema_arrow_json(
                        &mut std::io::stdout(),
                        schema.as_ref(),
                    )?;
                } else {
                    self.print_schema_unavailable();
                }
            }
            Some(SchemaOutputFormat::OdfJson) => {
                if let Some(schema) = self.get_odf_schema().await? {
                    odf::utils::schema::format::write_schema_odf_json(
                        &mut std::io::stdout(),
                        &schema,
                    )
                    .int_err()?;
                } else {
                    self.print_schema_unavailable();
                }
            }
            Some(SchemaOutputFormat::OdfYaml) => {
                if let Some(schema) = self.get_odf_schema().await? {
                    odf::utils::schema::format::write_schema_odf_yaml(
                        &mut std::io::stdout(),
                        &schema,
                    )
                    .int_err()?;
                } else {
                    self.print_schema_unavailable();
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
