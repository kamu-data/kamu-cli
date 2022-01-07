// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CLIError, Command};
use crate::{output::*, records_writers::TableWriter};
use kamu::domain::*;

use std::sync::Arc;

pub struct RepositoryListCommand {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    output_config: Arc<OutputConfig>,
}

impl RepositoryListCommand {
    pub fn new(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        output_config: Arc<OutputConfig>,
    ) -> Self {
        Self {
            remote_repo_reg,
            output_config,
        }
    }

    // TODO: support multiple format specifiers
    fn print_machine_readable(&self) -> Result<(), CLIError> {
        use std::io::Write;

        let mut repos: Vec<_> = self.remote_repo_reg.get_all_repositories().collect();
        repos.sort();

        let mut out = std::io::stdout();
        write!(out, "Name,URL\n")?;

        for name in repos {
            let repo = self.remote_repo_reg.get_repository(&name)?;
            write!(out, "{},\"{}\"\n", name, repo.url)?;
        }
        Ok(())
    }

    fn print_pretty(&self) -> Result<(), CLIError> {
        use prettytable::*;

        let mut repos: Vec<_> = self.remote_repo_reg.get_all_repositories().collect();
        repos.sort();

        let mut table = Table::new();
        table.set_format(TableWriter::get_table_format());

        table.set_titles(row![bc->"Name", bc->"URL"]);

        for name in repos.iter() {
            let repo = self.remote_repo_reg.get_repository(&name)?;
            table.add_row(Row::new(vec![
                Cell::new(&name),
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
}

impl Command for RepositoryListCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        // TODO: replace with formatters
        match self.output_config.format {
            OutputFormat::Table => self.print_pretty()?,
            OutputFormat::Csv => self.print_machine_readable()?,
            _ => unimplemented!("Unsupported format: {:?}", self.output_config.format),
        }

        Ok(())
    }
}
