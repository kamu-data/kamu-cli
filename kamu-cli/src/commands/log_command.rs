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
    dataset_ref: DatasetRefLocal,
    outout_format: Option<String>,
    filter: Option<String>,
    output_config: Arc<OutputConfig>,
}

impl LogCommand {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        dataset_ref: DatasetRefLocal,
        outout_format: Option<&str>,
        filter: Option<&str>,
        output_config: Arc<OutputConfig>,
    ) -> Self {
        Self {
            metadata_repo,
            dataset_ref,
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

        let dataset_handle = self.metadata_repo.resolve_dataset_ref(&self.dataset_ref)?;

        let mut blocks = self
            .metadata_repo
            .get_metadata_chain(&dataset_handle.as_local_ref())?
            .iter_blocks()
            .filter(|(_, b)| self.filter_block(b));

        renderer.show(&dataset_handle, &mut blocks)?;

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

trait MetadataRenderer {
    fn show(
        &mut self,
        dataset_handle: &DatasetHandle,
        blocks: &mut dyn Iterator<Item = (Multihash, MetadataBlock)>,
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
        blocks: &mut dyn Iterator<Item = (Multihash, MetadataBlock)>,
    ) -> Result<(), std::io::Error> {
        for (hash, block) in blocks {
            Self::render_block(output, &hash, &block)?;
            writeln!(output)?;
        }
        Ok(())
    }

    fn render_block(
        output: &mut impl Write,
        hash: &Multihash,
        block: &MetadataBlock,
    ) -> Result<(), std::io::Error> {
        Self::render_header(output, hash, block)?;
        Self::render_property(
            output,
            "SystemTime",
            &block
                .system_time
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        )?;

        if let Some(ref s) = block.output_slice {
            Self::render_property(output, "Output.Offset.Start", &s.data_interval.start)?;
            Self::render_property(output, "Output.Offset.End", &s.data_interval.end)?;
            Self::render_property(
                output,
                "Output.Records",
                &(s.data_interval.end - s.data_interval.start + 1),
            )?;
            Self::render_property(output, "Output.LogicalHash", &s.data_logical_hash)?;
        }

        if let Some(ref wm) = block.output_watermark {
            Self::render_property(
                output,
                "Output.Watermark",
                &wm.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            )?;
        }

        if let Some(slices) = &block.input_slices {
            for s in slices.iter() {
                if let Some(bi) = &s.block_interval {
                    Self::render_property(
                        output,
                        &format!("Input[{}].Blocks.Start", s.dataset_id),
                        &bi.start,
                    )?;
                    Self::render_property(
                        output,
                        &format!("Input[{}].Blocks.End", s.dataset_id),
                        &bi.end,
                    )?;
                }
                if let Some(iv) = &s.data_interval {
                    Self::render_property(
                        output,
                        &format!("Input[{}].Offset.Start", s.dataset_id),
                        &iv.start,
                    )?;
                    Self::render_property(
                        output,
                        &format!("Input[{}].Offset.End", s.dataset_id),
                        &iv.end,
                    )?;
                    Self::render_property(
                        output,
                        &format!("Input[{}].Records", s.dataset_id),
                        &(iv.end - iv.start + 1),
                    )?;
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

    fn render_header(
        output: &mut impl Write,
        hash: &Multihash,
        _block: &MetadataBlock,
    ) -> Result<(), std::io::Error> {
        writeln!(
            output,
            "{} {}",
            style("Block:").green(),
            style(&hash).yellow()
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
        _dataset_handle: &DatasetHandle,
        blocks: &mut dyn Iterator<Item = (Multihash, MetadataBlock)>,
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
        dataset_handle: &DatasetHandle,
        blocks: &mut dyn Iterator<Item = (Multihash, MetadataBlock)>,
    ) -> Result<(), CLIError> {
        let mut pager = minus::Pager::new().unwrap();
        pager.set_exit_strategy(minus::ExitStrategy::PagerQuit);
        pager.set_prompt(&dataset_handle.name);

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
        blocks: &mut dyn Iterator<Item = (Multihash, MetadataBlock)>,
    ) -> Result<(), std::io::Error> {
        for (hash, block) in blocks {
            Self::render_block(output, &hash, &block)?;
        }
        Ok(())
    }

    fn render_block(
        output: &mut impl Write,
        _hash: &Multihash,
        block: &MetadataBlock,
    ) -> Result<(), std::io::Error> {
        let buf = YamlMetadataBlockSerializer.write_manifest(&block).unwrap();

        writeln!(output, "{}", std::str::from_utf8(&buf).unwrap())
    }
}

impl MetadataRenderer for YamlRenderer {
    fn show(
        &mut self,
        _dataset_handle: &DatasetHandle,
        blocks: &mut dyn Iterator<Item = (Multihash, MetadataBlock)>,
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
        dataset_handle: &DatasetHandle,
        blocks: &mut dyn Iterator<Item = (Multihash, MetadataBlock)>,
    ) -> Result<(), CLIError> {
        let mut pager = minus::Pager::new().unwrap();
        pager.set_exit_strategy(minus::ExitStrategy::PagerQuit);
        pager.set_prompt(&dataset_handle.name);

        YamlRenderer::render_blocks(&mut WritePager(&mut pager), blocks)?;
        minus::page_all(pager).unwrap();
        Ok(())
    }
}
