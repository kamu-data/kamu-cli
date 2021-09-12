// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{OutputConfig, WritePager};

use super::{CLIError, Command};
use chrono::SecondsFormat;
use console::style;
use kamu::domain::*;
use opendatafabric::*;

use std::{io::Write, sync::Arc};

pub struct InspectQueryCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    dataset_id: DatasetIDBuf,
    output_config: Arc<OutputConfig>,
}

impl InspectQueryCommand {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        dataset_id: DatasetIDBuf,
        output_config: Arc<OutputConfig>,
    ) -> Self {
        Self {
            metadata_repo,
            dataset_id,
            output_config,
        }
    }

    fn render(&self, output: &mut impl Write) -> Result<(), CLIError> {
        let chain = self.metadata_repo.get_metadata_chain(&self.dataset_id)?;

        for block in chain.iter_blocks() {
            match block.source {
                Some(DatasetSource::Root(DatasetSourceRoot {
                    preprocess: Some(ref transform),
                    ..
                })) => {
                    self.render_transform(
                        output,
                        &block,
                        &vec![self.dataset_id.clone()],
                        transform,
                    )?;
                }
                Some(DatasetSource::Derivative(DatasetSourceDerivative {
                    ref inputs,
                    ref transform,
                })) => {
                    self.render_transform(output, &block, inputs, transform)?;
                }
                _ => (),
            }
        }

        Ok(())
    }

    fn render_transform(
        &self,
        output: &mut impl Write,
        block: &MetadataBlock,
        inputs: &Vec<DatasetIDBuf>,
        transform: &Transform,
    ) -> Result<(), std::io::Error> {
        writeln!(
            output,
            "{}: {}",
            style("Transform").green(),
            style(block.block_hash).yellow(),
        )?;

        writeln!(
            output,
            "{} {}",
            style("As Of:").dim(),
            block
                .system_time
                .to_rfc3339_opts(SecondsFormat::AutoSi, true)
        )?;

        writeln!(output, "{}", style("Inputs:").dim())?;

        for id in inputs {
            writeln!(output, "  {}", style(id).bold())?;
        }

        match transform {
            Transform::Sql(tr) => {
                writeln!(
                    output,
                    "{} {} ({:?})",
                    style("Engine:").dim(),
                    tr.engine,
                    tr.version
                )?;

                if let Some(temporal_tables) = &tr.temporal_tables {
                    for tt in temporal_tables {
                        writeln!(
                            output,
                            "{} {} PRIMARY KEY ({})",
                            style("Temporal Table:").dim(),
                            tt.id,
                            tt.primary_key.join(", ")
                        )?;
                    }
                }

                if let Some(queries) = &tr.queries {
                    for query in queries {
                        let alias = query
                            .alias
                            .clone()
                            .unwrap_or_else(|| self.dataset_id.to_string());
                        writeln!(output, "{} {}", style("Query:").dim(), style(alias).bold(),)?;
                        for line in query.query.trim_end().split('\n') {
                            writeln!(output, "  {}", line)?;
                        }
                    }
                }

                if let Some(query) = &tr.query {
                    writeln!(
                        output,
                        "{} {}",
                        style("Query:").dim(),
                        style(&self.dataset_id).bold(),
                    )?;
                    for line in query.trim_end().split('\n') {
                        writeln!(output, "  {}", line)?;
                    }
                }
            }
        }

        Ok(())
    }
}

impl Command for InspectQueryCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        if self.output_config.is_tty && self.output_config.verbosity_level == 0 {
            let mut pager = minus::Pager::new().unwrap();
            pager.set_exit_strategy(minus::ExitStrategy::PagerQuit);
            pager.set_prompt(self.dataset_id.to_string());

            self.render(&mut WritePager(&mut pager))?;
            minus::page_all(pager).unwrap();
        } else {
            self.render(&mut std::io::stdout())?;
        }
        Ok(())
    }
}
