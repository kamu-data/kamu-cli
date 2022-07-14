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
use futures::{StreamExt, TryStreamExt};
use opendatafabric::serde::yaml::YamlMetadataBlockSerializer;
use opendatafabric::serde::MetadataBlockSerializer;
use opendatafabric::MetadataBlock;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::io::Write;
use std::sync::Arc;

pub struct LogCommand {
    local_repo: Arc<dyn LocalDatasetRepository>,
    dataset_ref: DatasetRefLocal,
    outout_format: Option<String>,
    filter: Option<String>,
    limit: usize,
    output_config: Arc<OutputConfig>,
}

impl LogCommand {
    pub fn new(
        local_repo: Arc<dyn LocalDatasetRepository>,
        dataset_ref: DatasetRefLocal,
        outout_format: Option<&str>,
        filter: Option<&str>,
        limit: usize,
        output_config: Arc<OutputConfig>,
    ) -> Self {
        Self {
            local_repo,
            dataset_ref,
            outout_format: outout_format.map(|s| s.to_owned()),
            filter: filter.map(|s| s.to_owned()),
            limit,
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

#[async_trait::async_trait(?Send)]
impl Command for LogCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let id_to_name_lookup: BTreeMap<_, _> = self
            .local_repo
            .get_all_datasets()
            .map_ok(|h| (h.id, h.name))
            .try_collect()
            .await?;

        let mut renderer: Box<dyn MetadataRenderer> = match (
            self.outout_format.as_ref().map(|s| s.as_str()),
            self.output_config.is_tty && self.output_config.verbosity_level == 0,
        ) {
            (None, true) => Box::new(PagedAsciiRenderer::new(id_to_name_lookup, self.limit)),
            (None, false) => Box::new(AsciiRenderer::new(id_to_name_lookup, self.limit)),
            (Some("yaml"), true) => Box::new(PagedYamlRenderer::new(self.limit)),
            (Some("yaml"), false) => Box::new(YamlRenderer::new(self.limit)),
            _ => panic!("Unexpected output format combination"),
        };

        let dataset_handle = self
            .local_repo
            .resolve_dataset_ref(&self.dataset_ref)
            .await?;

        let dataset = self
            .local_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let blocks = Box::pin(
            dataset
                .as_metadata_chain()
                .iter_blocks()
                .filter_ok(|(_, b)| self.filter_block(b)),
        );

        renderer.show(&dataset_handle, blocks).await?;

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
trait MetadataRenderer {
    async fn show<'a>(
        &'a mut self,
        dataset_handle: &DatasetHandle,
        blocks: DynMetadataStream<'a>,
    ) -> Result<(), CLIError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

struct AsciiRenderer {
    id_to_name_lookup: BTreeMap<DatasetID, DatasetName>,
    limit: usize,
}

impl AsciiRenderer {
    fn new(id_to_name_lookup: BTreeMap<DatasetID, DatasetName>, limit: usize) -> Self {
        Self {
            id_to_name_lookup,
            limit,
        }
    }

    async fn render_blocks<'a, 'b>(
        &'a self,
        output: &mut impl Write,
        blocks: DynMetadataStream<'b>,
    ) -> Result<(), CLIError> {
        let mut blocks = blocks.take(self.limit);

        // TODO: We buffer output per block as writing directly to minus::Pager seems to
        // have a lot of overhead. In future we should improve its async paging support.
        let mut buf = Vec::new();

        while let Some((hash, block)) = blocks.try_next().await? {
            buf.clear();
            self.render_block(&mut buf, &hash, &block)?;
            writeln!(buf)?;

            output.write_all(&buf)?;
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
                input_checkpoint,
                output_data,
                output_checkpoint,
                output_watermark,
            }) => {
                self.render_property(output, 0, "Kind", "AddData")?;

                if let Some(icp) = input_checkpoint {
                    self.render_property(output, 0, "InputCheckpoint", &icp)?;
                }

                self.render_section(output, 0, "Data")?;
                self.render_data_slice(output, 1, &output_data)?;

                if let Some(ocp) = output_checkpoint {
                    self.render_section(output, 0, "OutputCheckpoint")?;
                    self.render_checkpoint(output, 1, &ocp)?;
                }

                if let Some(wm) = output_watermark {
                    self.render_property(
                        output,
                        0,
                        "Watermark",
                        wm.to_rfc3339_opts(SecondsFormat::AutoSi, true),
                    )?;
                }
            }
            MetadataEvent::ExecuteQuery(ExecuteQuery {
                input_slices,
                input_checkpoint,
                output_data,
                output_checkpoint,
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

                if let Some(icp) = input_checkpoint {
                    self.render_property(output, 0, "InputCheckpoint", &icp)?;
                }

                if let Some(output_data) = output_data {
                    self.render_section(output, 0, "Data")?;
                    self.render_data_slice(output, 1, &output_data)?;
                }

                if let Some(ocp) = output_checkpoint {
                    self.render_section(output, 0, "OutputCheckpoint")?;
                    self.render_checkpoint(output, 1, &ocp)?;
                }

                if let Some(wm) = output_watermark {
                    self.render_property(
                        output,
                        0,
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
            MetadataEvent::SetAttachments(e) => {
                self.render_property(output, 0, "Kind", "SetAttachments")?;
                match &e.attachments {
                    Attachments::Embedded(e) => {
                        self.render_section(output, 0, "Embedded")?;
                        for (i, item) in e.items.iter().enumerate() {
                            self.render_section(output, 1, &format!("Item[{}]", i))?;
                            self.render_property(output, 2, "Path", &item.path)?;
                            self.render_property(output, 2, "Content", "...")?;
                        }
                    }
                }
            }
            MetadataEvent::SetInfo(e) => {
                self.render_property(output, 0, "Kind", "SetInfo")?;
                if let Some(description) = &e.description {
                    self.render_property(output, 0, "Description", description)?;
                }
                if let Some(keywords) = &e.keywords {
                    self.render_property(output, 0, "Keywords", keywords.join(", "))?;
                }
            }
            MetadataEvent::SetLicense(e) => {
                self.render_property(output, 0, "Kind", "SetLicense")?;
                self.render_property(output, 0, "ShortName", &e.short_name)?;
                self.render_property(output, 0, "Name", &e.name)?;
                if let Some(spdx_id) = &e.spdx_id {
                    self.render_property(output, 0, "SPDXID", spdx_id)?;
                }
                self.render_property(output, 0, "WebsiteURL", &e.website_url)?;
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

    fn render_data_slice(
        &self,
        output: &mut impl Write,
        indent: i32,
        slice: &DataSlice,
    ) -> Result<(), std::io::Error> {
        self.render_property(output, indent, "Offset.Start", &slice.interval.start)?;
        self.render_property(output, indent, "Offset.End", &slice.interval.end)?;
        self.render_property(
            output,
            indent,
            "NumRecords",
            &(slice.interval.end - slice.interval.start + 1),
        )?;
        self.render_property(output, indent, "LogicalHash", &slice.logical_hash)?;
        self.render_property(output, indent, "PhysicalHash", &slice.physical_hash)?;
        self.render_property(output, indent, "Size", &slice.size)?;
        Ok(())
    }

    fn render_checkpoint(
        &self,
        output: &mut impl Write,
        indent: i32,
        checkpoint: &Checkpoint,
    ) -> Result<(), std::io::Error> {
        self.render_property(output, indent, "PhysicalHash", &checkpoint.physical_hash)?;
        self.render_property(output, indent, "Size", &checkpoint.size)?;
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

#[async_trait::async_trait]
impl MetadataRenderer for AsciiRenderer {
    async fn show<'a>(
        &'a mut self,
        _dataset_handle: &DatasetHandle,
        blocks: DynMetadataStream<'a>,
    ) -> Result<(), CLIError> {
        self.render_blocks(&mut std::io::stdout(), blocks).await?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct PagedAsciiRenderer {
    id_to_name_lookup: BTreeMap<DatasetID, DatasetName>,
    limit: usize,
}

impl PagedAsciiRenderer {
    fn new(id_to_name_lookup: BTreeMap<DatasetID, DatasetName>, limit: usize) -> Self {
        Self {
            id_to_name_lookup,
            limit,
        }
    }
}

#[async_trait::async_trait]
impl MetadataRenderer for PagedAsciiRenderer {
    async fn show<'a>(
        &'a mut self,
        dataset_handle: &DatasetHandle,
        blocks: DynMetadataStream<'a>,
    ) -> Result<(), CLIError> {
        let mut pager = minus::Pager::new();
        pager
            .set_exit_strategy(minus::ExitStrategy::PagerQuit)
            .unwrap();
        pager.set_prompt(&dataset_handle.name).unwrap();

        let renderer = AsciiRenderer::new(self.id_to_name_lookup.clone(), self.limit);
        let mut write = WritePager(&mut pager);
        renderer.render_blocks(&mut write, blocks).await?;

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

struct YamlRenderer {
    limit: usize,
}

impl YamlRenderer {
    fn new(limit: usize) -> Self {
        Self { limit }
    }

    async fn render_blocks<'a, 'b>(
        &self,
        output: &'a mut impl Write,
        blocks: DynMetadataStream<'b>,
    ) -> Result<(), CLIError> {
        let mut blocks = blocks.take(self.limit);

        // TODO: We buffer output per block as writing directly to minus::Pager seems to
        // have a lot of overhead. In future we should improve its async paging support.
        let mut buf = Vec::new();

        while let Some((hash, block)) = blocks.try_next().await? {
            buf.clear();
            Self::render_block(&mut buf, &hash, &block)?;
            writeln!(buf)?;

            output.write_all(&buf)?;
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

#[async_trait::async_trait]
impl MetadataRenderer for YamlRenderer {
    async fn show<'a>(
        &'a mut self,
        _dataset_handle: &DatasetHandle,
        blocks: DynMetadataStream<'a>,
    ) -> Result<(), CLIError> {
        self.render_blocks(&mut std::io::stdout(), blocks).await?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct PagedYamlRenderer {
    limit: usize,
}

impl PagedYamlRenderer {
    fn new(limit: usize) -> Self {
        Self { limit }
    }
}

#[async_trait::async_trait]
impl MetadataRenderer for PagedYamlRenderer {
    async fn show<'a>(
        &'a mut self,
        dataset_handle: &DatasetHandle,
        blocks: DynMetadataStream<'a>,
    ) -> Result<(), CLIError> {
        let mut pager = minus::Pager::new();
        pager
            .set_exit_strategy(minus::ExitStrategy::PagerQuit)
            .unwrap();
        pager.set_prompt(&dataset_handle.name).unwrap();

        {
            let mut write = WritePager(&mut pager);

            let renderer = YamlRenderer::new(self.limit);
            renderer.render_blocks(&mut write, blocks).await?;
            minus::page_all(pager).unwrap();
        }
        Ok(())
    }
}
