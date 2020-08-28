#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub verbosity_level: u8,
    pub is_tty: bool,
    pub format: OutputFormat,
}

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Csv,
    Json,
    Table,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            verbosity_level: 0,
            is_tty: false,
            format: OutputFormat::Table,
        }
    }
}
