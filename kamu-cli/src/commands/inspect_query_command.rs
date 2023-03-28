// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CLIError, Command};
use crate::{OutputConfig, WritePager};

use kamu::domain::*;
use opendatafabric::*;

use chrono::SecondsFormat;
use console::style;
use futures::TryStreamExt;
use std::{io::Write, sync::Arc};

pub struct InspectQueryCommand {
    local_repo: Arc<dyn DatasetRepository>,
    dataset_ref: DatasetRefLocal,
    output_config: Arc<OutputConfig>,
}

impl InspectQueryCommand {
    pub fn new(
        local_repo: Arc<dyn DatasetRepository>,
        dataset_ref: DatasetRefLocal,
        output_config: Arc<OutputConfig>,
    ) -> Self {
        Self {
            local_repo,
            dataset_ref,
            output_config,
        }
    }

    async fn render(
        &self,
        output: &mut impl Write,
        dataset_handle: &DatasetHandle,
    ) -> Result<(), CLIError> {
        let dataset = self
            .local_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let mut blocks = dataset.as_metadata_chain().iter_blocks();
        while let Some((block_hash, block)) = blocks.try_next().await? {
            match &block.event {
                MetadataEvent::SetTransform(SetTransform { inputs, transform }) => {
                    self.render_transform(
                        output,
                        dataset_handle,
                        &block_hash,
                        &block,
                        inputs,
                        transform,
                    )?;
                }
                MetadataEvent::SetPollingSource(SetPollingSource {
                    preprocess: Some(transform),
                    ..
                }) => {
                    self.render_transform(
                        output,
                        dataset_handle,
                        &block_hash,
                        &block,
                        &Vec::new(),
                        transform,
                    )?;
                }
                _ => (),
            }
        }

        Ok(())
    }

    fn render_transform(
        &self,
        output: &mut impl Write,
        dataset_handle: &DatasetHandle,
        block_hash: &Multihash,
        block: &MetadataBlock,
        inputs: &Vec<TransformInput>,
        transform: &Transform,
    ) -> Result<(), std::io::Error> {
        writeln!(
            output,
            "{}: {}",
            style("Transform").green(),
            style(block_hash).yellow(),
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

        for input in inputs {
            writeln!(
                output,
                "  {}  {}",
                style(&input.name).bold(),
                style(input.id.as_ref().unwrap()).dim(),
            )?;
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
                            tt.name,
                            tt.primary_key.join(", ")
                        )?;
                    }
                }

                if let Some(queries) = &tr.queries {
                    for query in queries {
                        let alias = query
                            .alias
                            .clone()
                            .unwrap_or_else(|| dataset_handle.name.to_string());
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
                        style(&dataset_handle.name).bold(),
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

#[async_trait::async_trait(?Send)]
impl Command for InspectQueryCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let dataset_handle = self
            .local_repo
            .resolve_dataset_ref(&self.dataset_ref)
            .await?;

        if self.output_config.is_tty && self.output_config.verbosity_level == 0 {
            let mut pager = minus::Pager::new();
            pager
                .set_exit_strategy(minus::ExitStrategy::PagerQuit)
                .unwrap();
            pager.set_prompt(&dataset_handle.name).unwrap();

            self.render(&mut WritePager(&mut pager), &dataset_handle)
                .await?;
            minus::page_all(pager).unwrap();
        } else {
            self.render(&mut std::io::stdout(), &dataset_handle).await?;
        }
        Ok(())
    }
}
