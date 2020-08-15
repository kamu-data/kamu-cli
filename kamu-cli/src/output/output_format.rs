#[derive(Debug, Clone)]
pub struct OutputFormat {
    pub verbosity_level: u8,
    pub is_tty: bool,
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self {
            verbosity_level: 0,
            is_tty: false,
        }
    }
}
