// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use kamu_data_utils::data::format::RecordsWriter;

use super::records_writers::*;
pub use super::records_writers::{ColumnFormat, RecordsFormat};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub quiet: bool,
    pub verbosity_level: u8,
    pub is_tty: bool,
    pub format: OutputFormat,
    /// Points to the output trace file if Perfetto tracing was enabled
    pub trace_file: Option<PathBuf>,
}

impl OutputConfig {
    pub fn get_records_writer(&self, fmt: RecordsFormat) -> Box<dyn RecordsWriter> {
        match self.format {
            OutputFormat::Csv => Box::new(
                CsvWriterBuilder::new()
                    .with_header(true)
                    .build(std::io::stdout()),
            ),
            OutputFormat::Json => Box::new(JsonArrayWriter::new(std::io::stdout())),
            OutputFormat::NdJson => Box::new(JsonLineDelimitedWriter::new(std::io::stdout())),
            OutputFormat::JsonSoA => unimplemented!("SoA Json format is not yet implemented"),
            OutputFormat::Table => Box::new(TableWriter::new(fmt)),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Csv,
    /// Array of Structures format
    Json,
    /// One Json object per line - easily splittable format
    NdJson,
    /// Structure of arrays - more compact and efficient format for encoding
    /// entire dataframe
    JsonSoA,
    /// A pretty human-readable table
    Table,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            quiet: false,
            verbosity_level: 0,
            is_tty: false,
            format: OutputFormat::Table,
            trace_file: None,
        }
    }
}
