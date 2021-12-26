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
use opendatafabric::*;

use std::sync::Arc;

pub struct SearchCommand {
    search_svc: Arc<dyn SearchService>,
    output_config: Arc<OutputConfig>,
    query: Option<String>,
    repository_names: Vec<RepositoryName>,
}

impl SearchCommand {
    pub fn new<S, R, I>(
        search_svc: Arc<dyn SearchService>,
        output_config: Arc<OutputConfig>,
        query: Option<S>,
        repository_names: I,
    ) -> Self
    where
        S: Into<String>,
        I: IntoIterator<Item = R>,
        R: TryInto<RepositoryName>,
        <R as TryInto<RepositoryName>>::Error: std::fmt::Debug,
    {
        Self {
            search_svc,
            output_config,
            query: query.map(|s| s.into()),
            repository_names: repository_names
                .into_iter()
                .map(|r| r.try_into().unwrap())
                .collect(),
        }
    }

    fn print_table(&self, mut search_result: SearchResult) {
        use prettytable::*;

        search_result.datasets.sort();

        let mut table = Table::new();
        table.set_format(TableWriter::get_table_format());

        table.set_titles(
            row![bc->"Name", bc->"Kind", bc->"Description", bc->"Updated", bc->"Records", bc->"Size"],
        );

        for name in &search_result.datasets {
            table.add_row(Row::new(vec![
                Cell::new(name),
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
        write!(out, "Name\n").unwrap();

        for name in &search_result.datasets {
            write!(out, "{}\n", name).unwrap();
        }
    }
}

impl Command for SearchCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let result = self
            .search_svc
            .search(
                self.query.as_deref(),
                SearchOptions {
                    repository_names: self.repository_names.clone(),
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
