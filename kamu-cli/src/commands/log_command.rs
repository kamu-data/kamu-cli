// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::output::OutputConfig;

use super::{CLIError, Command};
use kamu::domain::*;
use opendatafabric::*;

use chrono::prelude::*;
use console::style;
use opendatafabric::serde::yaml::YamlMetadataBlockSerializer;
use opendatafabric::serde::MetadataBlockSerializer;
use opendatafabric::MetadataBlock;
use std::fmt::Display;
use std::io::Write;
use std::sync::Arc;

pub struct LogCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    dataset_id: DatasetIDBuf,
    outout_format: Option<String>,
    filter: Option<String>,
    output_config: Arc<OutputConfig>,
}

impl LogCommand {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        dataset_id: DatasetIDBuf,
        outout_format: Option<&str>,
        filter: Option<&str>,
        output_config: Arc<OutputConfig>,
    ) -> Self {
        Self {
            metadata_repo,
            dataset_id,
            outout_format: outout_format.map(|s| s.to_owned()),
            filter: filter.map(|s| s.to_owned()),
            output_config,
        }
    }

    fn filter_block(&self, block: &MetadataBlock) -> bool {
        // Keep in sync with CLI parser
        // TODO: replace with bitfield enum
        match &self.filter {
            None => true,
            Some(f) if f.contains("source") && block.source.is_some() => true,
            Some(f) if f.contains("watermark") && block.output_watermark.is_some() => true,
            Some(f) if f.contains("data") && block.output_slice.is_some() => true,
            _ => false,
        }
    }
}

impl Command for LogCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let mut renderer: Box<dyn MetadataRenderer> = match (
            self.outout_format.as_ref().map(|s| s.as_str()),
            self.output_config.is_tty && self.output_config.verbosity_level == 0,
        ) {
            (None, true) => Box::new(PagedAsciiRenderer::new()),
            (None, false) => Box::new(AsciiRenderer::new()),
            (Some("yaml"), true) => Box::new(PagedYamlRenderer::new()),
            (Some("yaml"), false) => Box::new(YamlRenderer::new()),
            _ => panic!("Unexpected output format combination"),
        };

        let mut blocks = self
            .metadata_repo
            .get_metadata_chain(&self.dataset_id)?
            .iter_blocks()
            .filter(|b| self.filter_block(b));

        renderer.show(&self.dataset_id, &mut blocks)?;

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

trait MetadataRenderer {
    fn show(
        &mut self,
        dataset_id: &DatasetID,
        blocks: &mut dyn Iterator<Item = MetadataBlock>,
    ) -> Result<(), CLIError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

struct AsciiRenderer;

impl AsciiRenderer {
    fn new() -> Self {
        Self
    }

    fn render_blocks(
        output: &mut impl Write,
        blocks: &mut dyn Iterator<Item = MetadataBlock>,
    ) -> Result<(), std::io::Error> {
        for block in blocks {
            Self::render_block(output, &block)?;
            writeln!(output)?;
        }
        Ok(())
    }

    fn render_block(output: &mut impl Write, block: &MetadataBlock) -> Result<(), std::io::Error> {
        Self::render_header(output, block)?;
        Self::render_property(
            output,
            "SystemTime",
            &block
                .system_time
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        )?;

        if let Some(ref s) = block.output_slice {
            Self::render_property(output, "Output.Records", &s.num_records)?;
            Self::render_property(output, "Output.Interval", &s.interval)?;
            if !s.hash.is_empty() {
                Self::render_property(output, "Output.Hash", &s.hash)?;
            }
        }

        if let Some(ref wm) = block.output_watermark {
            Self::render_property(
                output,
                "Output.Watermark",
                &wm.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            )?;
        }

        if let Some(ref slices) = block.input_slices {
            for (i, ref s) in slices.iter().enumerate() {
                Self::render_property(output, &format!("Input[{}].Records", i), &s.num_records)?;
                Self::render_property(output, &format!("Input[{}].Interval", i), &s.interval)?;
                if !s.hash.is_empty() {
                    Self::render_property(output, &format!("Input[{}].Hash", i), &s.hash)?;
                }
            }
        }

        if let Some(ref source) = block.source {
            match source {
                DatasetSource::Root { .. } => {
                    Self::render_property(output, "Source", &"<Root source updated>")?
                }
                DatasetSource::Derivative { .. } => {
                    Self::render_property(output, "Source", &"<Derivative source updated>")?
                }
            }
        }

        Ok(())
    }

    fn render_header(output: &mut impl Write, block: &MetadataBlock) -> Result<(), std::io::Error> {
        writeln!(
            output,
            "{} {}",
            style("Block:").green(),
            style(&block.block_hash).yellow()
        )
    }

    fn render_property<T: Display>(
        output: &mut impl Write,
        name: &str,
        value: &T,
    ) -> Result<(), std::io::Error> {
        writeln!(
            output,
            "{}{} {}",
            style(name).dim(),
            style(":").dim(),
            value
        )
    }
}

impl MetadataRenderer for AsciiRenderer {
    fn show(
        &mut self,
        _dataset_id: &DatasetID,
        blocks: &mut dyn Iterator<Item = MetadataBlock>,
    ) -> Result<(), CLIError> {
        Self::render_blocks(&mut std::io::stdout(), blocks)?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct PagedAsciiRenderer;

impl PagedAsciiRenderer {
    fn new() -> Self {
        Self
    }
}

impl MetadataRenderer for PagedAsciiRenderer {
    fn show(
        &mut self,
        dataset_id: &DatasetID,
        blocks: &mut dyn Iterator<Item = MetadataBlock>,
    ) -> Result<(), CLIError> {
        let mut pager = minus::Pager::new().unwrap();
        pager.set_exit_strategy(minus::ExitStrategy::PagerQuit);
        pager.set_prompt(dataset_id.to_string());

        AsciiRenderer::render_blocks(&mut WritePager(&mut pager), blocks)?;
        minus::page_all(pager).unwrap();
        Ok(())
    }
}

// TODO: Figure out how to use std::fmt::Write with std::io::stdout()
pub struct WritePager<'a>(pub &'a mut minus::Pager);

impl<'a> Write for WritePager<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        use std::fmt::Write;
        self.0.write_str(std::str::from_utf8(buf).unwrap()).unwrap();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct YamlRenderer;

impl YamlRenderer {
    fn new() -> Self {
        Self
    }

    fn render_blocks(
        output: &mut impl Write,
        blocks: &mut dyn Iterator<Item = MetadataBlock>,
    ) -> Result<(), std::io::Error> {
        for block in blocks {
            Self::render_block(output, block)?;
        }
        Ok(())
    }

    fn render_block(output: &mut impl Write, block: MetadataBlock) -> Result<(), std::io::Error> {
        let buf = YamlMetadataBlockSerializer
            .write_manifest_unchecked(&block)
            .unwrap();

        writeln!(output, "{}", std::str::from_utf8(&buf).unwrap())
    }
}

impl MetadataRenderer for YamlRenderer {
    fn show(
        &mut self,
        _dataset_id: &DatasetID,
        blocks: &mut dyn Iterator<Item = MetadataBlock>,
    ) -> Result<(), CLIError> {
        Self::render_blocks(&mut std::io::stdout(), blocks)?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct PagedYamlRenderer;

impl PagedYamlRenderer {
    fn new() -> Self {
        Self
    }
}

impl MetadataRenderer for PagedYamlRenderer {
    fn show(
        &mut self,
        dataset_id: &DatasetID,
        blocks: &mut dyn Iterator<Item = MetadataBlock>,
    ) -> Result<(), CLIError> {
        let mut pager = minus::Pager::new().unwrap();
        pager.set_exit_strategy(minus::ExitStrategy::PagerQuit);
        pager.set_prompt(dataset_id.to_string());

        YamlRenderer::render_blocks(&mut WritePager(&mut pager), blocks)?;
        minus::page_all(pager).unwrap();
        Ok(())
    }
}
