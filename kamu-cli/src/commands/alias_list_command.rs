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
use opendatafabric::DatasetIDBuf;

use std::{convert::TryFrom, sync::Arc};

pub struct AliasListCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    output_config: Arc<OutputConfig>,
    dataset_id: Option<String>,
}

impl AliasListCommand {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        output_config: Arc<OutputConfig>,
        dataset_id: Option<String>,
    ) -> Self {
        Self {
            metadata_repo,
            output_config,
            dataset_id,
        }
    }

    // TODO: support multiple format specifiers
    fn print_machine_readable(&self) -> Result<(), CLIError> {
        use std::io::Write;

        let mut out = std::io::stdout();
        write!(out, "Dataset,Kind,Alias\n")?;

        let mut datasets: Vec<DatasetIDBuf> = if self.dataset_id.is_none() {
            self.metadata_repo.get_all_datasets().collect()
        } else {
            vec![DatasetIDBuf::try_from(self.dataset_id.clone().unwrap()).unwrap()]
        };
        datasets.sort();

        for ds in &datasets {
            let aliases = self.metadata_repo.get_remote_aliases(&ds)?;

            for alias in aliases.get_by_kind(RemoteAliasKind::Pull) {
                write!(out, "{},{},{}\n", &ds, "pull", &alias)?;
            }
            for alias in aliases.get_by_kind(RemoteAliasKind::Push) {
                write!(out, "{},{},{}\n", &ds, "push", &alias)?;
            }
        }

        Ok(())
    }

    fn print_pretty(&self) -> Result<(), CLIError> {
        use prettytable::*;

        let mut datasets: Vec<DatasetIDBuf> = if self.dataset_id.is_none() {
            self.metadata_repo.get_all_datasets().collect()
        } else {
            vec![DatasetIDBuf::try_from(self.dataset_id.clone().unwrap()).unwrap()]
        };
        datasets.sort();

        let mut items = 0;
        let mut table = Table::new();
        table.set_format(TableWriter::get_table_format());

        table.set_titles(row![bc->"Dataset", bc->"Kind", bc->"Alias"]);

        for ds in &datasets {
            let aliases = self.metadata_repo.get_remote_aliases(&ds)?;
            let mut pull_aliases: Vec<_> = aliases.get_by_kind(RemoteAliasKind::Pull).collect();
            let mut push_aliases: Vec<_> = aliases.get_by_kind(RemoteAliasKind::Push).collect();
            pull_aliases.sort();
            push_aliases.sort();

            for alias in pull_aliases {
                items += 1;
                table.add_row(Row::new(vec![
                    Cell::new(&ds),
                    Cell::new("Pull"),
                    Cell::new(&alias),
                ]));
            }

            for alias in push_aliases {
                items += 1;
                table.add_row(Row::new(vec![
                    Cell::new(&ds),
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

impl Command for AliasListCommand {
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
