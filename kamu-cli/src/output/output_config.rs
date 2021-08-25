use super::records_writers::*;

#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub quiet: bool,
    pub verbosity_level: u8,
    pub is_tty: bool,
    pub format: OutputFormat,
}

impl OutputConfig {
    pub fn get_writer(&self) -> Box<dyn RecordsWriter> {
        match self.format {
            OutputFormat::Csv => Box::new(
                CsvWriterBuilder::new()
                    .has_headers(true)
                    .build(std::io::stdout()),
            ),
            OutputFormat::Json => Box::new(JsonArrayWriter::new(std::io::stdout())),
            OutputFormat::JsonLD => Box::new(JsonLineDelimitedWriter::new(std::io::stdout())),
            OutputFormat::JsonSoA => unimplemented!("SoA Json format is not yet implemented"),
            OutputFormat::Table => Box::new(TableWriter::new()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Csv,
    /// Array of Structures format
    Json,
    /// One Json object per line - easily splittable format
    JsonLD,
    /// Structure of arrays - more compact and efficient format for encoding entire dataframe
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
        }
    }
}
