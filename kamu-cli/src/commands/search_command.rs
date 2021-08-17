use super::{CLIError, Command};
use crate::output::*;
use kamu::domain::*;
use opendatafabric::*;

use std::sync::Arc;

pub struct SearchCommand {
    search_svc: Arc<dyn SearchService>,
    output_config: Arc<OutputConfig>,
    query: Option<String>,
    repository_ids: Vec<RepositoryBuf>,
}

impl SearchCommand {
    pub fn new<S, R, I>(
        search_svc: Arc<dyn SearchService>,
        output_config: Arc<OutputConfig>,
        query: Option<S>,
        repository_ids: I,
    ) -> Self
    where
        S: Into<String>,
        R: Into<RepositoryBuf>,
        I: IntoIterator<Item = R>,
    {
        Self {
            search_svc,
            output_config,
            query: query.map(|s| s.into()),
            repository_ids: repository_ids.into_iter().map(|r| r.into()).collect(),
        }
    }

    fn print_table(&self, mut search_result: SearchResult) {
        use prettytable::*;

        search_result.datasets.sort();

        let mut table = Table::new();
        table.set_format(self.get_table_format());

        table.set_titles(row![bc->"ID"]);

        for id in &search_result.datasets {
            table.add_row(Row::new(vec![Cell::new(id)]));
        }

        // Header doesn't render when there are no data rows in the table
        if search_result.datasets.is_empty() {
            table.add_row(Row::new(vec![Cell::new("")]));
        }

        table.printstd();
    }

    fn get_table_format(&self) -> prettytable::format::TableFormat {
        use prettytable::format::*;

        FormatBuilder::new()
            .column_separator('│')
            .borders('│')
            .separators(&[LinePosition::Top], LineSeparator::new('─', '┬', '┌', '┐'))
            .separators(
                &[LinePosition::Title],
                LineSeparator::new('─', '┼', '├', '┤'),
            )
            .separators(
                &[LinePosition::Bottom],
                LineSeparator::new('─', '┴', '└', '┘'),
            )
            .padding(1, 1)
            .build()
    }

    fn print_csv(&self, mut search_result: SearchResult) {
        use std::io::Write;

        search_result.datasets.sort();

        let mut out = std::io::stdout();
        write!(out, "ID\n").unwrap();

        for id in &search_result.datasets {
            write!(out, "{}\n", id,).unwrap();
        }
    }
}

impl Command for SearchCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let result = self
            .search_svc
            .search(
                self.query.as_ref().map(|s| s.as_str()),
                SearchOptions {
                    repository_ids: self.repository_ids.clone(),
                },
            )
            .map_err(|e| CLIError::failure(e))?;

        // TODO: replace with formatters
        match self.output_config.format {
            OutputFormat::Table => self.print_table(result),
            OutputFormat::Csv => self.print_csv(result),
            _ => unimplemented!("Unsupported format: {:?}", self.output_config.format),
        }

        Ok(())
    }
}
