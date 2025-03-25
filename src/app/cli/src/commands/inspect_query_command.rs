// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;
use std::sync::Arc;

use chrono::SecondsFormat;
use console::style;
use futures::TryStreamExt;
use kamu::domain::*;
use kamu_auth_rebac::RebacDatasetRegistryFacade;

use super::{CLIError, Command};
use crate::{OutputConfig, WritePager};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InspectQueryCommand {
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    dataset_ref: odf::DatasetRef,
    output_config: Arc<OutputConfig>,
}

impl InspectQueryCommand {
    pub fn new(
        rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
        dataset_ref: odf::DatasetRef,
        output_config: Arc<OutputConfig>,
    ) -> Self {
        Self {
            rebac_dataset_registry_facade,
            dataset_ref,
            output_config,
        }
    }

    async fn render(
        &self,
        output: &mut impl Write,
        resolved_dataset: ResolvedDataset,
    ) -> Result<(), CLIError> {
        use odf::dataset::MetadataChainExt;

        let mut blocks = resolved_dataset.as_metadata_chain().iter_blocks();
        while let Some((block_hash, block)) = blocks.try_next().await? {
            match &block.event {
                odf::MetadataEvent::SetTransform(odf::metadata::SetTransform {
                    inputs,
                    transform,
                }) => {
                    self.render_transform(
                        output,
                        resolved_dataset.get_handle(),
                        &block_hash,
                        &block,
                        inputs,
                        transform,
                    )?;
                }
                odf::MetadataEvent::SetPollingSource(odf::metadata::SetPollingSource {
                    preprocess: Some(transform),
                    ..
                }) => {
                    self.render_transform(
                        output,
                        resolved_dataset.get_handle(),
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
        dataset_handle: &odf::DatasetHandle,
        block_hash: &odf::Multihash,
        block: &odf::MetadataBlock,
        inputs: &Vec<odf::metadata::TransformInput>,
        transform: &odf::metadata::Transform,
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
                style(input.alias.as_ref().unwrap()).bold(),
                style(&input.dataset_ref).dim(),
            )?;
        }

        match transform {
            odf::metadata::Transform::Sql(tr) => {
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
                            .unwrap_or_else(|| dataset_handle.alias.to_string());
                        writeln!(output, "{} {}", style("Query:").dim(), style(alias).bold(),)?;
                        for line in query.query.trim_end().split('\n') {
                            writeln!(output, "  {line}")?;
                        }
                    }
                }

                if let Some(query) = &tr.query {
                    writeln!(
                        output,
                        "{} {}",
                        style("Query:").dim(),
                        style(&dataset_handle.alias).bold(),
                    )?;
                    for line in query.trim_end().split('\n') {
                        writeln!(output, "  {line}")?;
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for InspectQueryCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let resolved_dataset = self
            .rebac_dataset_registry_facade
            .resolve_dataset_by_ref(&self.dataset_ref, auth::DatasetAction::Read)
            .await?;

        if self.output_config.is_tty && self.output_config.verbosity_level == 0 {
            let mut pager = minus::Pager::new();
            pager
                .set_exit_strategy(minus::ExitStrategy::PagerQuit)
                .unwrap();
            pager
                .set_prompt(resolved_dataset.get_alias().to_string())
                .unwrap();

            self.render(&mut WritePager(&mut pager), resolved_dataset)
                .await?;
            minus::page_all(pager).unwrap();
        } else {
            self.render(&mut std::io::stdout(), resolved_dataset)
                .await?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
