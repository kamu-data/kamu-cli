use super::{Command, Error};
use crate::output::*;
use kamu::domain::*;
use opendatafabric::RemoteIDBuf;

use std::sync::Arc;

pub struct RemoteListCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    output_config: OutputConfig,
}

impl RemoteListCommand {
    pub fn new(metadata_repo: Arc<dyn MetadataRepository>, output_config: &OutputConfig) -> Self {
        Self {
            metadata_repo,
            output_config: output_config.clone(),
        }
    }

    // TODO: support multiple format specifiers
    fn print_machine_readable(&self) -> Result<(), Error> {
        use std::io::Write;

        let mut remotes: Vec<RemoteIDBuf> = self.metadata_repo.get_all_remotes().collect();
        remotes.sort();

        let mut out = std::io::stdout();
        write!(out, "ID,URL\n")?;

        for id in remotes {
            let remote = self.metadata_repo.get_remote(&id)?;
            write!(out, "{},\"{}\"\n", id, remote.url)?;
        }
        Ok(())
    }

    fn print_pretty(&self) -> Result<(), Error> {
        use prettytable::*;

        let mut remotes: Vec<RemoteIDBuf> = self.metadata_repo.get_all_remotes().collect();
        remotes.sort();

        let mut table = Table::new();
        table.set_format(self.get_table_format());

        table.set_titles(row![bc->"ID", bc->"URL"]);

        for id in remotes.iter() {
            let remote = self.metadata_repo.get_remote(&id)?;
            table.add_row(Row::new(vec![
                Cell::new(&id),
                Cell::new(&remote.url.to_string()),
            ]));
        }

        // Header doesn't render when there are no data rows in the table
        if remotes.is_empty() {
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

impl Command for RemoteListCommand {
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
