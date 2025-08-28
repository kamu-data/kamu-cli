// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::IsTerminal;
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
    output_format: SchemaOutputFormat,

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

    // TODO: Refactor to some kind of a special YAML serializer that can color
    // values depending on context
    /// Writes schema as a colored YAML for extra readability
    fn print_schema_odf_colored_yaml(&self, schema: &odf::schema::DataSchema) {
        self.print_schema_odf_colored_yaml_fields(&schema.fields, 0);

        if let Some(extra) = &schema.extra {
            self.print_schema_odf_colored_yaml_extra(extra, 0);
        }
    }

    fn print_schema_odf_colored_yaml_fields(
        &self,
        fields: &Vec<odf::schema::DataField>,
        depth: i32,
    ) {
        self.indent(depth);
        println!("{}", console::style("fields:").dim());

        for f in fields {
            self.indent(depth);
            println!(
                "- {} {}",
                console::style("name:").dim(),
                console::style(&f.name).bold()
            );

            self.indent(depth + 1);
            println!("{}", console::style("type:").dim());
            self.print_schema_odf_colored_yaml_type(&f.r#type, depth + 2);

            if let Some(extra) = &f.extra {
                self.print_schema_odf_colored_yaml_extra(extra, depth + 2);
            }
        }
    }

    fn print_schema_odf_colored_yaml_type(&self, t: &odf::schema::DataType, depth: i32) {
        use odf::schema::*;

        self.indent(depth);
        print!("{}", console::style("kind: ").dim());

        match t {
            DataType::Binary(DataTypeBinary { fixed_length }) => {
                println!("{}", console::style("Binary").cyan());

                if let Some(fixed_length) = fixed_length {
                    self.indent(depth);
                    println!(
                        "{} {}",
                        console::style("fixedLength:").dim(),
                        console::style(fixed_length).cyan()
                    );
                }
            }
            DataType::Bool(DataTypeBool {}) => println!("{}", console::style("Bool").cyan()),
            DataType::Date(DataTypeDate {}) => println!("{}", console::style("Date").cyan()),
            DataType::Decimal(DataTypeDecimal { precision, scale }) => {
                println!("{}", console::style("Decimal").cyan());

                self.indent(depth);
                println!(
                    "{} {}",
                    console::style("precision:").dim(),
                    console::style(precision).cyan()
                );
                self.indent(depth);
                println!(
                    "{} {}",
                    console::style("scale:").dim(),
                    console::style(scale).cyan()
                );
            }
            DataType::Duration(DataTypeDuration { unit }) => {
                println!("{}", console::style("Duration").cyan());

                self.indent(depth);
                println!(
                    "{} {}",
                    console::style("unit:").dim(),
                    console::style(format!("{unit:?}")).cyan()
                );
            }
            DataType::Float16(DataTypeFloat16 {}) => {
                println!("{}", console::style("Float16").cyan());
            }
            DataType::Float32(DataTypeFloat32 {}) => {
                println!("{}", console::style("Float32").cyan());
            }
            DataType::Float64(DataTypeFloat64 {}) => {
                println!("{}", console::style("Float64").cyan());
            }
            DataType::Int8(DataTypeInt8 {}) => println!("{}", console::style("Int8").cyan()),
            DataType::Int16(DataTypeInt16 {}) => println!("{}", console::style("Int16").cyan()),
            DataType::Int32(DataTypeInt32 {}) => println!("{}", console::style("Int32").cyan()),
            DataType::Int64(DataTypeInt64 {}) => println!("{}", console::style("Int64").cyan()),
            DataType::UInt8(DataTypeUInt8 {}) => println!("{}", console::style("UInt8").cyan()),
            DataType::UInt16(DataTypeUInt16 {}) => println!("{}", console::style("UInt16").cyan()),
            DataType::UInt32(DataTypeUInt32 {}) => println!("{}", console::style("UInt32").cyan()),
            DataType::UInt64(DataTypeUInt64 {}) => println!("{}", console::style("UInt64").cyan()),
            DataType::List(DataTypeList {
                item_type,
                fixed_length,
            }) => {
                println!("{}", console::style("List").cyan());

                if let Some(fixed_length) = fixed_length {
                    self.indent(depth);
                    println!(
                        "{} {}",
                        console::style("fixedLength:").dim(),
                        console::style(fixed_length).cyan(),
                    );
                }

                self.indent(depth);
                println!("{}", console::style("itemType:").dim());
                self.print_schema_odf_colored_yaml_type(item_type, depth + 1);
            }
            DataType::Map(DataTypeMap {
                key_type,
                value_type,
                keys_sorted,
            }) => {
                println!("{}", console::style("Map").cyan());

                self.indent(depth);
                println!("{}", console::style("keyType:").dim());
                self.print_schema_odf_colored_yaml_type(key_type, depth + 1);

                self.indent(depth);
                println!("{}", console::style("valueType:").dim());
                self.print_schema_odf_colored_yaml_type(value_type, depth + 1);

                if let Some(keys_sorted) = keys_sorted {
                    self.indent(depth);
                    println!(
                        "{} {}",
                        console::style("keysSorted:").dim(),
                        console::style(keys_sorted).cyan(),
                    );
                }
            }
            DataType::Null(DataTypeNull {}) => println!("{}", console::style("Null").cyan()),
            DataType::Option(DataTypeOption { inner }) => {
                println!("{}", console::style("Option").magenta());

                self.indent(depth);
                println!("{}", console::style("inner:").dim());
                self.print_schema_odf_colored_yaml_type(inner, depth + 1);
            }
            DataType::Struct(DataTypeStruct { fields }) => {
                println!("{}", console::style("Struct").cyan());
                self.print_schema_odf_colored_yaml_fields(fields, depth);
            }
            DataType::Time(DataTypeTime { unit }) => {
                println!("{}", console::style("Time").cyan());

                self.indent(depth);
                println!(
                    "{} {}",
                    console::style("unit:").dim(),
                    console::style(format!("{unit:?}")).cyan(),
                );
            }
            DataType::Timestamp(DataTypeTimestamp { unit, timezone }) => {
                println!("{}", console::style("Timestamp").cyan());

                self.indent(depth);
                println!(
                    "{} {}",
                    console::style("unit:").dim(),
                    console::style(format!("{unit:?}")).cyan(),
                );

                if let Some(timezone) = timezone {
                    self.indent(depth);
                    println!(
                        "{} {}",
                        console::style("timezone:").dim(),
                        console::style(timezone).cyan(),
                    );
                }
            }
            DataType::String(DataTypeString {}) => println!("{}", console::style("String").cyan()),
        }
    }

    fn print_schema_odf_colored_yaml_extra(
        &self,
        extra: &odf::schema::ExtraAttributes,
        depth: i32,
    ) {
        self.indent(depth);
        println!("{}", console::style("extra:").dim());
        for (k, v) in &extra.attributes {
            self.indent(depth + 1);
            println!("{k}: {v}");
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
            SchemaOutputFormat::Ddl => {
                if let Some(schema) = self.get_parquet_schema().await? {
                    self.print_schema_ddl(&schema);
                } else {
                    self.print_schema_unavailable();
                }
            }
            SchemaOutputFormat::Parquet => {
                if let Some(schema) = self.get_parquet_schema().await? {
                    odf::utils::schema::format::write_schema_parquet(
                        &mut std::io::stdout(),
                        &schema,
                    )?;
                } else {
                    self.print_schema_unavailable();
                }
            }
            SchemaOutputFormat::ParquetJson => {
                if let Some(schema) = self.get_parquet_schema().await? {
                    odf::utils::schema::format::write_schema_parquet_json(
                        &mut std::io::stdout(),
                        &schema,
                    )?;
                } else {
                    self.print_schema_unavailable();
                }
            }
            SchemaOutputFormat::ArrowJson => {
                if let Some(schema) = self.get_arrow_schema().await? {
                    odf::utils::schema::format::write_schema_arrow_json(
                        &mut std::io::stdout(),
                        schema.as_ref(),
                    )?;
                } else {
                    self.print_schema_unavailable();
                }
            }
            SchemaOutputFormat::OdfJson => {
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
            SchemaOutputFormat::OdfYaml => {
                if let Some(schema) = self.get_odf_schema().await? {
                    if std::io::stdout().is_terminal() {
                        self.print_schema_odf_colored_yaml(&schema);
                    } else {
                        odf::utils::schema::format::write_schema_odf_yaml(
                            &mut std::io::stdout(),
                            &schema,
                        )
                        .int_err()?;
                    }
                } else {
                    self.print_schema_unavailable();
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
