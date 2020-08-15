#[derive(Debug, Clone)]
pub struct OutputFormat {
    pub verbosity_level: u8,
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self { verbosity_level: 0 }
    }
}
