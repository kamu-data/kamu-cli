use crate::output::OutputConfig;

use super::{Command, Error};
use kamu::domain::*;
use opendatafabric::*;

use chrono::prelude::*;
use console::style;
use std::fmt::Display;
use std::io::Write;
use std::sync::Arc;

pub struct LogCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    dataset_id: DatasetIDBuf,
    output_config: OutputConfig,
}

impl LogCommand {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        dataset_id: DatasetIDBuf,
        output_config: OutputConfig,
    ) -> Self {
        Self {
            metadata_repo,
            dataset_id,
            output_config,
        }
    }

    fn render_blocks(
        &self,
        output: &mut impl Write,
        blocks: impl Iterator<Item = MetadataBlock>,
    ) -> Result<(), std::io::Error> {
        for block in blocks {
            self.render_block(output, &block)?;
            writeln!(output)?;
        }
        Ok(())
    }

    fn render_block(
        &self,
        output: &mut impl Write,
        block: &MetadataBlock,
    ) -> Result<(), std::io::Error> {
        self.render_header(output, block)?;
        self.render_property(
            output,
            "Date",
            &block
                .system_time
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        )?;

        if let Some(ref s) = block.output_slice {
            self.render_property(output, "Output.Records", &s.num_records)?;
            self.render_property(output, "Output.Interval", &s.interval)?;
            if !s.hash.is_empty() {
                self.render_property(output, "Output.Hash", &s.hash)?;
            }
        }

        if let Some(ref wm) = block.output_watermark {
            self.render_property(
                output,
                "Output.Watermark",
                &wm.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            )?;
        }

        if let Some(ref slices) = block.input_slices {
            for (i, ref s) in slices.iter().enumerate() {
                self.render_property(output, &format!("Input[{}].Records", i), &s.num_records)?;
                self.render_property(output, &format!("Input[{}].Interval", i), &s.interval)?;
                if !s.hash.is_empty() {
                    self.render_property(output, &format!("Input[{}].Hash", i), &s.hash)?;
                }
            }
        }

        if let Some(ref source) = block.source {
            match source {
                DatasetSource::Root { .. } => {
                    self.render_property(output, "Source", &"<Root source updated>")?
                }
                DatasetSource::Derivative { .. } => {
                    self.render_property(output, "Source", &"<Derivative source updated>")?
                }
            }
        }

        Ok(())
    }

    fn render_header(
        &self,
        output: &mut impl Write,
        block: &MetadataBlock,
    ) -> Result<(), std::io::Error> {
        writeln!(
            output,
            "{} {}",
            style("Block:").green(),
            style(&block.block_hash).yellow()
        )
    }

    fn render_property<T: Display>(
        &self,
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

impl Command for LogCommand {
    fn run(&mut self) -> Result<(), Error> {
        let chain = self.metadata_repo.get_metadata_chain(&self.dataset_id)?;

        if self.output_config.is_tty {
            let mut pager = minus::Pager::new().unwrap();
            pager.set_exit_strategy(minus::ExitStrategy::PagerQuit);
            pager.set_prompt(self.dataset_id.clone());

            self.render_blocks(&mut WritePager(&mut pager), chain.iter_blocks())
                .unwrap();
            minus::page_all(pager).unwrap();
        } else {
            self.render_blocks(&mut std::io::stdout(), chain.iter_blocks())
                .unwrap();
        }

        Ok(())
    }
}

// TODO: Figure out how to use std::fmt::Write with std::io::stdout()
struct WritePager<'a>(&'a mut minus::Pager);

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
