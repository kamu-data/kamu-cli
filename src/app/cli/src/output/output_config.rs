// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::Schema;
use internal_error::{InternalError, ResultIntoInternal};

use super::records_writers::*;
pub use super::records_writers::{ColumnFormat, RecordsFormat};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub quiet: bool,
    pub verbosity_level: u8,
    pub is_tty: bool,
    pub format: OutputFormat,
    /// Points to the output trace file if Perfetto tracing was enabled
    pub trace_file: Option<PathBuf>,
    /// Points to the output metrics file if Prometheus metrics dump was
    /// requested
    pub metrics_file: Option<PathBuf>,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            quiet: false,
            verbosity_level: 0,
            is_tty: false,
            format: OutputFormat::Table,
            trace_file: None,
            metrics_file: None,
        }
    }
}

impl OutputConfig {
    pub fn get_records_writer(
        &self,
        schema: &Schema,
        fmt: RecordsFormat,
    ) -> Box<dyn RecordsWriter> {
        match self.format {
            OutputFormat::Csv => Box::new(CsvWriter::new(
                std::io::stdout(),
                CsvWriterOptions {
                    header: true,
                    ..Default::default()
                },
            )),
            OutputFormat::Json => Box::new(JsonArrayOfStructsWriter::new(std::io::stdout())),
            OutputFormat::JsonSoA => {
                Box::new(JsonStructOfArraysWriter::new(std::io::stdout(), usize::MAX))
            }
            OutputFormat::JsonAoA => Box::new(JsonArrayOfArraysWriter::new(std::io::stdout())),
            OutputFormat::NdJson => Box::new(JsonLineDelimitedWriter::new(std::io::stdout())),
            OutputFormat::Table => Box::new(TableWriter::new(schema, fmt, std::io::stdout())),
            OutputFormat::Parquet => {
                unimplemented!("Parquet format is applicable for data export only")
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(strum::Display, Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum OutputFormat {
    /// Comma-separated values
    #[clap(name = "csv")]
    #[strum(to_string = "csv")]
    Csv,

    /// Array of Structures format
    #[clap(name = "json")]
    #[strum(to_string = "json")]
    Json,

    /// One Json object per line - easily splittable format
    #[clap(name = "ndjson")]
    #[strum(to_string = "ndjson")]
    NdJson,

    /// Structure of arrays - more compact and efficient format for encoding
    /// entire dataframe
    #[clap(name = "json-soa")]
    #[strum(to_string = "json-soa")]
    JsonSoA,

    /// Array of arrays - compact and efficient and preserves column order
    #[clap(name = "json-aoa")]
    #[strum(to_string = "json-aoa")]
    JsonAoA,

    /// A pretty human-readable table
    #[clap(name = "table")]
    #[strum(to_string = "table")]
    Table,

    /// Parquet columnar storage. Only available when exporting to file(s)
    #[clap(name = "parquet")]
    #[strum(to_string = "parquet")]
    Parquet,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait OutputWriter {
    fn records_format(&self) -> RecordsFormat;

    fn schema(&self) -> Arc<Schema>;

    fn records(&self, records_data: Vec<ArrayRef>) -> Result<RecordBatch, InternalError> {
        let records = RecordBatch::try_new(self.schema(), records_data).int_err()?;

        Ok(records)
    }
}
