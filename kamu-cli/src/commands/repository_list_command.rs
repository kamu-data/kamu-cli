use super::{Command, Error};
use crate::output::*;
use kamu::domain::*;
use opendatafabric::RepositoryBuf;

use std::sync::Arc;

pub struct RepositoryListCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    output_config: Arc<OutputConfig>,
}

impl RepositoryListCommand {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        output_config: Arc<OutputConfig>,
    ) -> Self {
        Self {
            metadata_repo,
            output_config,
        }
    }

    // TODO: support multiple format specifiers
    fn print_machine_readable(&self) -> Result<(), Error> {
        use std::io::Write;

        let mut repos: Vec<_> = self.metadata_repo.get_all_repositories().collect();
        repos.sort();

        let mut out = std::io::stdout();
        write!(out, "ID,URL\n")?;

        for id in repos {
            let repo = self.metadata_repo.get_repository(&id)?;
            write!(out, "{},\"{}\"\n", id, repo.url)?;
        }
        Ok(())
    }

    fn print_pretty(&self) -> Result<(), Error> {
        use prettytable::*;

        let mut repos: Vec<RepositoryBuf> = self.metadata_repo.get_all_repositories().collect();
        repos.sort();

        let mut table = Table::new();
        table.set_format(self.get_table_format());

        table.set_titles(row![bc->"ID", bc->"URL"]);

        for id in repos.iter() {
            let repo = self.metadata_repo.get_repository(&id)?;
            table.add_row(Row::new(vec![
                Cell::new(&id),
                Cell::new(&repo.url.to_string()),
            ]));
        }

        // Header doesn't render when there are no data rows in the table
        if repos.is_empty() {
            table.add_row(Row::new(vec![Cell::new(""), Cell::new("")]));
        }

        table.printstd();
        Ok(())
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
}

impl Command for RepositoryListCommand {
    fn run(&mut self) -> Result<(), Error> {
        // TODO: replace with formatters
        match self.output_config.format {
            OutputFormat::Table => self.print_pretty()?,
            OutputFormat::Csv => self.print_machine_readable()?,
            _ => unimplemented!("Unsupported format: {:?}", self.output_config.format),
        }

        Ok(())
    }
}
