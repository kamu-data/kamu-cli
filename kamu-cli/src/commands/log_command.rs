use super::{Command, Error};
use kamu::domain::*;
use opendatafabric::*;

use chrono::prelude::*;
use console::style;
use std::cell::RefCell;
use std::fmt::Display;
use std::rc::Rc;

pub struct LogCommand {
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
    dataset_id: DatasetIDBuf,
}

impl LogCommand {
    pub fn new(
        metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
        dataset_id: DatasetIDBuf,
    ) -> Self {
        Self {
            metadata_repo: metadata_repo,
            dataset_id: dataset_id,
        }
    }

    fn render_block(&self, block: &MetadataBlock) {
        self.render_header(block);
        self.render_property(
            "Date",
            &block
                .system_time
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        );

        if let Some(ref s) = block.output_slice {
            self.render_property("Output.Records", &s.num_records);
            self.render_property("Output.Interval", &s.interval);
            if !s.hash.is_empty() {
                self.render_property("Output.Hash", &s.hash);
            }
        }

        if let Some(ref wm) = block.output_watermark {
            self.render_property(
                "Output.Watermark",
                &wm.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            );
        }

        if let Some(ref slices) = block.input_slices {
            for (i, ref s) in slices.iter().enumerate() {
                self.render_property(&format!("Input[{}].Records", i), &s.num_records);
                self.render_property(&format!("Input[{}].Interval", i), &s.interval);
                if !s.hash.is_empty() {
                    self.render_property(&format!("Input[{}].Hash", i), &s.hash);
                }
            }
        }

        if let Some(ref source) = block.source {
            match source {
                DatasetSource::Root { .. } => {
                    self.render_property("Source", &"<Root source updated>")
                }
                DatasetSource::Derivative { .. } => {
                    self.render_property("Source", &"<Derivative source updated>")
                }
            }
        }
    }

    fn render_header(&self, block: &MetadataBlock) {
        println!(
            "{} {}",
            style("Block:").green(),
            style(&block.block_hash).yellow()
        )
    }

    fn render_property<T: Display>(&self, name: &str, value: &T) {
        println!("{}{} {}", style(name).dim(), style(":").dim(), value);
    }
}

impl Command for LogCommand {
    fn run(&mut self) -> Result<(), Error> {
        let chain = self
            .metadata_repo
            .borrow()
            .get_metadata_chain(&self.dataset_id)?;

        for block in chain.iter_blocks() {
            self.render_block(&block);
            println!();
        }

        Ok(())
    }
}
