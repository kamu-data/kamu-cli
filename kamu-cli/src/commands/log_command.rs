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
use std::collections::BTreeMap;
use std::fmt::Display;
use std::io::Write;
use std::sync::Arc;

pub struct LogCommand {
    dataset_reg: Arc<dyn DatasetRegistry>,
    dataset_ref: DatasetRefLocal,
    outout_format: Option<String>,
    filter: Option<String>,
    output_config: Arc<OutputConfig>,
}

impl LogCommand {
    pub fn new(
        dataset_reg: Arc<dyn DatasetRegistry>,
        dataset_ref: DatasetRefLocal,
        outout_format: Option<&str>,
        filter: Option<&str>,
        output_config: Arc<OutputConfig>,
    ) -> Self {
        Self {
            dataset_reg,
            dataset_ref,
            outout_format: outout_format.map(|s| s.to_owned()),
            filter: filter.map(|s| s.to_owned()),
            output_config,
        }
    }

    fn filter_block(&self, block: &MetadataBlock) -> bool {
        // Keep in sync with CLI parser
        // TODO: replace with bitfield enum
        if let Some(f) = &self.filter {
            match &block.event {
                MetadataEvent::AddData(_) if f.contains("data") || f.contains("watermark") => true,
                MetadataEvent::ExecuteQuery(_) if f.contains("data") || f.contains("watermark") => {
                    true
                }
                MetadataEvent::Seed(_) if f.contains("source") => true,
                MetadataEvent::SetPollingSource(_) if f.contains("source") => true,
                MetadataEvent::SetTransform(_) if f.contains("source") => true,
                MetadataEvent::SetVocab(_) if f.contains("source") => true,
                MetadataEvent::SetWatermark(_) if f.contains("data") || f.contains("watermark") => {
                    true
                }
                _ => false,
            }
        } else {
            true
        }
    }
}

impl Command for LogCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let id_to_name_lookup: BTreeMap<_, _> = self
            .dataset_reg
            .get_all_datasets()
            .map(|h| (h.id, h.name))
            .collect();

        let mut renderer: Box<dyn MetadataRenderer> = match (
            self.outout_format.as_ref().map(|s| s.as_str()),
            self.output_config.is_tty && self.output_config.verbosity_level == 0,
        ) {
            (None, true) => Box::new(PagedAsciiRenderer::new(id_to_name_lookup)),
            (None, false) => Box::new(AsciiRenderer::new(id_to_name_lookup)),
            (Some("yaml"), true) => Box::new(PagedYamlRenderer::new()),
            (Some("yaml"), false) => Box::new(YamlRenderer::new()),
            _ => panic!("Unexpected output format combination"),
        };

        let dataset_handle = self.dataset_reg.resolve_dataset_ref(&self.dataset_ref)?;

        let mut blocks = self
            .dataset_reg
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

struct AsciiRenderer {
    id_to_name_lookup: BTreeMap<DatasetID, DatasetName>,
}

impl AsciiRenderer {
    fn new(id_to_name_lookup: BTreeMap<DatasetID, DatasetName>) -> Self {
        Self { id_to_name_lookup }
    }

    fn render_blocks(
        &self,
        output: &mut impl Write,
        blocks: &mut dyn Iterator<Item = (Multihash, MetadataBlock)>,
    ) -> Result<(), std::io::Error> {
        for (hash, block) in blocks {
            self.render_block(output, &hash, &block)?;
            writeln!(output)?;
        }
        Ok(())
    }

    fn render_block(
        &self,
        output: &mut impl Write,
        hash: &Multihash,
        block: &MetadataBlock,
    ) -> Result<(), std::io::Error> {
        self.render_header(output, hash, block)?;
        self.render_property(
            output,
            0,
            "SystemTime",
            &block
                .system_time
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        )?;

        match &block.event {
            MetadataEvent::AddData(AddData {
                output_data,
                output_watermark,
            }) => {
                self.render_property(output, 0, "Kind", "AddData")?;
                self.render_section(output, 0, "Event")?;

                self.render_section(output, 1, "Data")?;
                self.render_property(output, 2, "Offset.Start", &output_data.interval.start)?;
                self.render_property(output, 2, "Offset.End", &output_data.interval.end)?;
                self.render_property(
                    output,
                    2,
                    "Records",
                    &(output_data.interval.end - output_data.interval.start + 1),
                )?;
                self.render_property(output, 2, "LogicalHash", &output_data.logical_hash)?;
                self.render_property(output, 2, "PhysicalHash", &output_data.physical_hash)?;

                if let Some(wm) = output_watermark {
                    self.render_property(
                        output,
                        1,
                        "Watermark",
                        wm.to_rfc3339_opts(SecondsFormat::AutoSi, true),
                    )?;
                }
            }
            MetadataEvent::ExecuteQuery(ExecuteQuery {
                input_slices,
                output_data,
                output_watermark,
            }) => {
                self.render_property(output, 0, "Kind", "ExecuteQuery")?;
                self.render_section(output, 0, "Inputs")?;
                for (i, s) in input_slices.iter().enumerate() {
                    self.render_section(output, 1, &format!("Input[{}]", i))?;

                    self.render_property(output, 2, "ID", &s.dataset_id)?;

                    if let Some(name) = self.id_to_name_lookup.get(&s.dataset_id) {
                        self.render_property(output, 2, "Name", name)?;
                    }

                    if let Some(bi) = &s.block_interval {
                        self.render_property(output, 2, "Blocks.Start", &bi.start)?;
                        self.render_property(output, 2, "Blocks.End", &bi.end)?;
                    }
                    if let Some(iv) = &s.data_interval {
                        self.render_property(output, 2, "Offset.Start", &iv.start)?;
                        self.render_property(output, 2, "Offset.End", &iv.end)?;
                        self.render_property(output, 2, "Records", &(iv.end - iv.start + 1))?;
                    }
                }

                if let Some(output_data) = output_data {
                    self.render_section(output, 0, "Data")?;
                    self.render_property(output, 1, "Offset.Start", &output_data.interval.start)?;
                    self.render_property(output, 1, "Offset.End", &output_data.interval.end)?;
                    self.render_property(
                        output,
                        1,
                        "NumRecords",
                        &(output_data.interval.end - output_data.interval.start + 1),
                    )?;
                    self.render_property(output, 1, "LogicalHash", &output_data.logical_hash)?;
                    self.render_property(output, 1, "PhysicalHash", &output_data.physical_hash)?;
                }

                if let Some(wm) = output_watermark {
                    self.render_property(
                        output,
                        1,
                        "Watermark",
                        wm.to_rfc3339_opts(SecondsFormat::AutoSi, true),
                    )?;
                }
            }
            MetadataEvent::Seed(e) => {
                self.render_property(output, 0, "Kind", "Seed")?;
                self.render_property(output, 0, "DatasetKind", format!("{:?}", e.dataset_kind))?;
                self.render_property(output, 0, "DatasetID", &e.dataset_id)?;
            }
            MetadataEvent::SetPollingSource(_) => {
                self.render_property(output, 0, "Kind", "SetPollingSource")?;
                self.render_property(output, 0, "Source", "...")?
            }
            MetadataEvent::SetTransform(_) => {
                self.render_property(output, 0, "Kind", &"SetTransform")?;
                self.render_property(output, 0, "Transform", "...")?
            }
            MetadataEvent::SetVocab(SetVocab {
                offset_column,
                system_time_column,
                event_time_column,
            }) => {
                self.render_property(output, 0, "Kind", &"SetVocab")?;
                if let Some(offset_column) = offset_column {
                    self.render_property(output, 0, "OffsetColumn", offset_column)?;
                }
                if let Some(system_time_column) = system_time_column {
                    self.render_property(output, 0, "SystemTimeColumn", system_time_column)?;
                }
                if let Some(event_time_column) = event_time_column {
                    self.render_property(output, 0, "EventTimeColumn", event_time_column)?;
                }
            }
            MetadataEvent::SetWatermark(e) => {
                self.render_property(output, 0, "Kind", &"SetWatermark")?;
                self.render_property(
                    output,
                    0,
                    "Watermark",
                    e.output_watermark
                        .to_rfc3339_opts(SecondsFormat::AutoSi, true),
                )?;
            }
        }

        Ok(())
    }

    fn render_header(
        &self,
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

    fn render_section(
        &self,
        output: &mut impl Write,
        indent: i32,
        name: &str,
    ) -> Result<(), std::io::Error> {
        self.indent(output, indent)?;
        writeln!(output, "{}{}", style(name).dim(), style(":").dim())
    }

    fn render_property<T: Display>(
        &self,
        output: &mut impl Write,
        indent: i32,
        name: &str,
        value: T,
    ) -> Result<(), std::io::Error> {
        self.indent(output, indent)?;
        writeln!(
            output,
            "{}{} {}",
            style(name).dim(),
            style(":").dim(),
            value
        )
    }

    fn indent(&self, output: &mut impl Write, level: i32) -> Result<(), std::io::Error> {
        for _ in 0..level {
            write!(output, "  ")?;
        }
        Ok(())
    }
}

impl MetadataRenderer for AsciiRenderer {
    fn show(
        &mut self,
        _dataset_handle: &DatasetHandle,
        blocks: &mut dyn Iterator<Item = (Multihash, MetadataBlock)>,
    ) -> Result<(), CLIError> {
        self.render_blocks(&mut std::io::stdout(), blocks)?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct PagedAsciiRenderer {
    id_to_name_lookup: BTreeMap<DatasetID, DatasetName>,
}

impl PagedAsciiRenderer {
    fn new(id_to_name_lookup: BTreeMap<DatasetID, DatasetName>) -> Self {
        Self { id_to_name_lookup }
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

        let renderer = AsciiRenderer::new(self.id_to_name_lookup.clone());
        renderer.render_blocks(&mut WritePager(&mut pager), blocks)?;

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
