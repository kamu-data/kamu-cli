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
use opendatafabric::DatasetRefLocal;

use std::sync::Arc;

pub struct AliasListCommand {
    dataset_reg: Arc<dyn DatasetRegistry>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    output_config: Arc<OutputConfig>,
    dataset_ref: Option<DatasetRefLocal>,
}

impl AliasListCommand {
    pub fn new<R>(
        dataset_reg: Arc<dyn DatasetRegistry>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        output_config: Arc<OutputConfig>,
        dataset_ref: Option<R>,
    ) -> Self
    where
        R: TryInto<DatasetRefLocal>,
        <R as TryInto<DatasetRefLocal>>::Error: std::fmt::Debug,
    {
        Self {
            dataset_reg,
            remote_alias_reg,
            output_config,
            dataset_ref: dataset_ref.map(|s| s.try_into().unwrap()),
        }
    }

    // TODO: support multiple format specifiers
    fn print_machine_readable(&self) -> Result<(), CLIError> {
        use std::io::Write;

        let mut out = std::io::stdout();
        write!(out, "Dataset,Kind,Alias\n")?;

        let mut datasets: Vec<_> = if let Some(dataset_ref) = &self.dataset_ref {
            vec![self.dataset_reg.resolve_dataset_ref(dataset_ref)?]
        } else {
            self.dataset_reg.get_all_datasets().collect()
        };
        datasets.sort_by(|a, b| a.name.cmp(&b.name));

        for ds in &datasets {
            let aliases = self
                .remote_alias_reg
                .get_remote_aliases(&ds.as_local_ref())?;

            for alias in aliases.get_by_kind(RemoteAliasKind::Pull) {
                write!(out, "{},{},{}\n", &ds.name, "pull", &alias)?;
            }
            for alias in aliases.get_by_kind(RemoteAliasKind::Push) {
                write!(out, "{},{},{}\n", &ds.name, "push", &alias)?;
            }
        }

        Ok(())
    }

    fn print_pretty(&self) -> Result<(), CLIError> {
        use prettytable::*;

        let mut datasets: Vec<_> = if let Some(dataset_ref) = &self.dataset_ref {
            vec![self.dataset_reg.resolve_dataset_ref(dataset_ref)?]
        } else {
            self.dataset_reg.get_all_datasets().collect()
        };
        datasets.sort_by(|a, b| a.name.cmp(&b.name));

        let mut items = 0;
        let mut table = Table::new();
        table.set_format(TableWriter::get_table_format());

        table.set_titles(row![bc->"Dataset", bc->"Kind", bc->"Alias"]);

        for ds in &datasets {
            let aliases = self
                .remote_alias_reg
                .get_remote_aliases(&ds.as_local_ref())?;
            let mut pull_aliases: Vec<_> = aliases.get_by_kind(RemoteAliasKind::Pull).collect();
            let mut push_aliases: Vec<_> = aliases.get_by_kind(RemoteAliasKind::Push).collect();
            pull_aliases.sort();
            push_aliases.sort();

            for alias in pull_aliases {
                items += 1;
                table.add_row(Row::new(vec![
                    Cell::new(&ds.name),
                    Cell::new("Pull"),
                    Cell::new(&alias),
                ]));
            }

            for alias in push_aliases {
                items += 1;
                table.add_row(Row::new(vec![
                    Cell::new(&ds.name),
                    Cell::new("Push"),
                    Cell::new(&alias),
                ]));
            }
        }

        // Header doesn't render when there are no data rows in the table
        if items == 0 {
            table.add_row(Row::new(vec![Cell::new(""), Cell::new("")]));
        }

        table.printstd();
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for AliasListCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        // TODO: replace with formatters
        match self.output_config.format {
            OutputFormat::Table => self.print_pretty()?,
            OutputFormat::Csv => self.print_machine_readable()?,
            _ => unimplemented!("Unsupported format: {:?}", self.output_config.format),
        }

        Ok(())
    }
}
