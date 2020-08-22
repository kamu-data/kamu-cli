use super::{Command, Error};
use crate::output::OutputFormat;
use kamu::domain::*;
use kamu::infra::serde::yaml::DatasetKind;

use std::cell::RefCell;
use std::rc::Rc;

pub struct DepgraphCommand {
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
    output_format: OutputFormat,
}

impl DepgraphCommand {
    pub fn new(
        metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
        output_format: &OutputFormat,
    ) -> Self {
        Self {
            metadata_repo: metadata_repo,
            output_format: output_format.clone(),
        }
    }
}

impl Command for DepgraphCommand {
    fn run(&mut self) -> Result<(), Error> {
        let mut summaries = self
            .metadata_repo
            .borrow()
            .get_all_datasets()
            .map(|id| self.metadata_repo.borrow().get_summary(&id))
            .collect::<Result<Vec<_>, _>>()?;

        summaries.sort_by(|a, b| a.id.cmp(&b.id));

        println!("digraph datasets {{\nrankdir = LR;");

        for s in summaries.iter() {
            for dep in s.dependencies.iter() {
                println!("\"{}\" -> \"{}\";", dep, s.id);
            }
        }

        for s in summaries.iter() {
            if s.kind == DatasetKind::Root {
                println!("\"{}\" [style=filled, fillcolor=darkolivegreen1];", s.id);
            } else if s.kind == DatasetKind::Derivative {
                println!("\"{}\" [style=filled, fillcolor=lightblue];", s.id);
            }
        }

        println!("}}");

        Ok(())
    }
}
