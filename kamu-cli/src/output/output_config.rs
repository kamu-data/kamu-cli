#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub quiet: bool,
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
            quiet: false,
            verbosity_level: 0,
            is_tty: false,
            format: OutputFormat::Table,
        }
    }
}
