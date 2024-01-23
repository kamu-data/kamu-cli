// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::*;
use opendatafabric::*;

use super::{CLIError, Command};
use crate::output::*;
use crate::records_writers::TableWriter;

pub struct SearchCommand {
    search_svc: Arc<dyn SearchService>,
    output_config: Arc<OutputConfig>,
    query: Option<String>,
    repository_names: Vec<RepoName>,
}

impl SearchCommand {
    pub fn new<S, I>(
        search_svc: Arc<dyn SearchService>,
        output_config: Arc<OutputConfig>,
        query: Option<S>,
        repository_names: I,
    ) -> Self
    where
        S: Into<String>,
        I: IntoIterator<Item = RepoName>,
    {
        Self {
            search_svc,
            output_config,
            query: query.map(|s| s.into()),
            repository_names: repository_names.into_iter().collect(),
        }
    }

    fn print_table(&self, mut search_result: SearchResult) {
        use prettytable::*;

        search_result.datasets.sort();

        let mut table = Table::new();
        table.set_format(TableWriter::<Vec<u8>>::get_table_format());

        table.set_titles(row![
            bc->"Name",
            bc->"Kind",
            bc->"Description",
            bc->"Updated",
            bc->"Records",
            bc->"Size",
        ]);

        for name in &search_result.datasets {
            table.add_row(Row::new(vec![
                Cell::new(&name.to_string()),
                Cell::new("-"),
                Cell::new("-"),
                Cell::new("-"),
                Cell::new("-"),
                Cell::new("-"),
            ]));
        }

        // Header doesn't render when there are no data rows in the table
        if search_result.datasets.is_empty() {
            table.add_row(Row::new(vec![
                Cell::new(""),
                Cell::new(""),
                Cell::new(""),
                Cell::new(""),
                Cell::new(""),
                Cell::new(""),
            ]));
        }

        table.printstd();
    }

    fn print_csv(&self, mut search_result: SearchResult) {
        use std::io::Write;

        search_result.datasets.sort();

        let mut out = std::io::stdout();
        writeln!(out, "Name").unwrap();

        for name in &search_result.datasets {
            writeln!(out, "{name}").unwrap();
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SearchCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let result = self
            .search_svc
            .search(
                self.query.as_deref(),
                SearchOptions {
                    repository_names: self.repository_names.clone(),
                },
            )
            .await
            .map_err(CLIError::failure)?;

        // TODO: replace with formatters
        match self.output_config.format {
            OutputFormat::Table => self.print_table(result),
            OutputFormat::Csv => self.print_csv(result),
            _ => unimplemented!("Unsupported format: {:?}", self.output_config.format),
        }

        Ok(())
    }
}
