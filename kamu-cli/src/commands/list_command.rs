use super::{Command, Error};
use crate::output::OutputFormat;
use kamu::domain::*;

use chrono::{DateTime, Utc};
use chrono_humanize::HumanTime;
use std::cell::RefCell;
use std::rc::Rc;

pub struct ListCommand {
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
    output_format: OutputFormat,
}

impl ListCommand {
    pub fn new(
        metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
        output_format: &OutputFormat,
    ) -> Self {
        Self {
            metadata_repo: metadata_repo,
            output_format: output_format.clone(),
        }
    }

    // TODO: support multiple format specifiers
    fn print_machine_readable(&self) -> Result<(), Error> {
        use std::io::Write;

        let mut datasets: Vec<DatasetIDBuf> =
            self.metadata_repo.borrow().get_all_datasets().collect();

        datasets.sort();

        let mut out = std::io::stdout();
        write!(out, "ID,Kind,Pulled,Records,Size\n")?;

        for id in datasets {
            let summary = self.metadata_repo.borrow().get_summary(&id)?;
            write!(
                out,
                "{},{:?},{},{},{}\n",
                id,
                summary.kind,
                match summary.last_pulled {
                    None => "".to_owned(),
                    Some(t) => t.to_rfc3339(),
                },
                summary.num_records,
                summary.data_size
            )?;
        }
        Ok(())
    }

    fn print_pretty(&self) -> Result<(), Error> {
        use prettytable::*;

        let mut datasets: Vec<DatasetIDBuf> =
            self.metadata_repo.borrow().get_all_datasets().collect();

        datasets.sort();

        let mut table = Table::new();
        table.set_format(self.get_table_format());

        table.set_titles(row![bc->"ID", bc->"Kind", bc->"Pulled", bc->"Records", bc->"Size"]);

        for id in datasets.iter() {
            let summary = self.metadata_repo.borrow().get_summary(&id)?;

            table.add_row(Row::new(vec![
                Cell::new(&id),
                Cell::new(&format!("{:?}", summary.kind)).style_spec("c"),
                Cell::new(&self.humanize_last_pulled(summary.last_pulled)).style_spec("c"),
                Cell::new(&self.humanize_num_records(summary.num_records)).style_spec("r"),
                Cell::new(&self.humanize_data_size(summary.data_size)).style_spec("r"),
            ]));
        }

        // Header doesn't render when there are no data rows in the table
        if datasets.is_empty() {
            table.add_row(Row::new(vec![
                Cell::new(""),
                Cell::new(""),
                Cell::new(""),
                Cell::new(""),
                Cell::new(""),
            ]));
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

    fn humanize_last_pulled(&self, last_pulled: Option<DateTime<Utc>>) -> String {
        match last_pulled {
            None => "-".to_owned(),
            Some(t) => format!("{}", HumanTime::from(t - Utc::now())),
        }
    }

    fn humanize_data_size(&self, size: u64) -> String {
        if size == 0 {
            return "-".to_owned();
        }
        use humansize::{file_size_opts, FileSize};
        size.file_size(file_size_opts::BINARY).unwrap()
    }

    fn humanize_num_records(&self, num: u64) -> String {
        use num_format::{Locale, ToFormattedString};
        if num == 0 {
            return "-".to_owned();
        }
        num.to_formatted_string(&Locale::en)
    }
}

impl Command for ListCommand {
    fn run(&mut self) -> Result<(), Error> {
        if self.output_format.is_tty {
            self.print_pretty()?
        } else {
            self.print_machine_readable()?
        }

        Ok(())
    }
}
