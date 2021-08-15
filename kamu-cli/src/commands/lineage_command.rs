use crate::OutputConfig;

use super::{CLIError, Command};
use kamu::domain::*;
use kamu::infra::DatasetKind;
use opendatafabric::DatasetID;
use opendatafabric::DatasetIDBuf;

use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;

pub struct LineageCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    ids: Vec<DatasetIDBuf>,
    all: bool,
    output_format: Option<String>,
    output_config: Arc<OutputConfig>,
}

impl LineageCommand {
    pub fn new<I, S>(
        metadata_repo: Arc<dyn MetadataRepository>,
        ids: I,
        all: bool,
        output_format: Option<&str>,
        output_config: Arc<OutputConfig>,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            metadata_repo,
            ids: ids
                .into_iter()
                .map(|s| DatasetIDBuf::try_from(s.into()).unwrap())
                .collect(),
            all,
            output_format: output_format.map(|s| s.to_owned()),
            output_config,
        }
    }

    fn get_visitor(&self) -> Box<dyn DependencyVisitor> {
        if self.output_format.is_none() {
            return if self.output_config.is_tty {
                Box::new(AsciiVisitor::new())
            } else {
                Box::new(CsvVisitor::new())
            };
        }

        match self.output_format.as_ref().unwrap().as_str() {
            "dot" => Box::new(DotVisitor::new()),
            "csv" => Box::new(CsvVisitor::new()),
            _ => unimplemented!(),
        }
    }

    fn visit_dependencies(
        &self,
        mut visitor: Box<dyn DependencyVisitor>,
        dataset_ids: Vec<DatasetIDBuf>,
    ) -> Result<(), CLIError> {
        visitor.begin();
        for id in &dataset_ids {
            self.visit_dependencies_rec(id, visitor.as_mut())?;
        }
        visitor.done();
        Ok(())
    }

    fn visit_dependencies_rec(
        &self,
        id: &DatasetID,
        visitor: &mut dyn DependencyVisitor,
    ) -> Result<(), CLIError> {
        let summary = match self.metadata_repo.get_summary(id) {
            Ok(s) => Some(s),
            Err(DomainError::DoesNotExist { .. }) => None,
            Err(e) => return Err(e.into()),
        };

        let info = summary
            .as_ref()
            .map(|s| NodeInfo::Local {
                kind: s.kind,
                dependencies: &s.dependencies,
            })
            .unwrap_or_else(|| NodeInfo::Remote);

        if !visitor.enter(id, &info) {
            return Ok(());
        }

        if let Some(s) = &summary {
            for dep_id in &s.dependencies {
                self.visit_dependencies_rec(dep_id, visitor)?;
            }
        }

        visitor.exit(id, &info);

        Ok(())
    }
}

// TODO: Support temporality and evolution
impl Command for LineageCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let mut dataset_ids: Vec<_> = if self.ids.is_empty() {
            if self.all {
                self.metadata_repo.get_all_datasets().collect()
            } else {
                return Err(CLIError::usage_error(
                    "Specify at least one dataset or use --all flag",
                ));
            }
        } else {
            self.ids.clone()
        };

        dataset_ids.sort();

        let visitor = self.get_visitor();
        self.visit_dependencies(visitor, dataset_ids)?;

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
enum NodeInfo<'a> {
    Local {
        kind: DatasetKind,
        dependencies: &'a [DatasetIDBuf],
    },
    Remote,
}

trait DependencyVisitor {
    fn begin(&mut self);
    fn enter(&mut self, id: &DatasetID, info: &NodeInfo<'_>) -> bool;
    fn exit(&mut self, id: &DatasetID, info: &NodeInfo<'_>);
    fn done(&mut self);
}

/////////////////////////////////////////////////////////////////////////////////////////
// GraphViz
/////////////////////////////////////////////////////////////////////////////////////////

struct DotVisitor {
    visited: HashSet<DatasetIDBuf>,
}

impl DotVisitor {
    fn new() -> Self {
        Self {
            visited: HashSet::new(),
        }
    }
}

impl DependencyVisitor for DotVisitor {
    fn begin(&mut self) {
        println!("digraph datasets {{\nrankdir = LR;");
    }

    fn enter(&mut self, id: &DatasetID, info: &NodeInfo<'_>) -> bool {
        if !self.visited.insert(id.to_owned()) {
            return false;
        }

        match info {
            &NodeInfo::Local { kind, .. } => match kind {
                DatasetKind::Root => {
                    println!("\"{}\" [style=filled, fillcolor=darkolivegreen1];", id)
                }
                DatasetKind::Derivative => {
                    println!("\"{}\" [style=filled, fillcolor=lightblue];", id)
                }
            },
            &NodeInfo::Remote => println!("\"{}\" [style=filled, fillcolor=gray];", id),
        }

        if let &NodeInfo::Local { dependencies, .. } = info {
            for dep in dependencies {
                println!("\"{}\" -> \"{}\";", dep, id);
            }
        }

        true
    }

    fn exit(&mut self, _id: &DatasetID, _info: &NodeInfo<'_>) {}

    fn done(&mut self) {
        println!("}}");
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// ASCII
/////////////////////////////////////////////////////////////////////////////////////////

struct AsciiVisitor {
    buffer: Vec<String>,
}

impl AsciiVisitor {
    fn new() -> Self {
        Self { buffer: Vec::new() }
    }
}

impl DependencyVisitor for AsciiVisitor {
    fn begin(&mut self) {}

    fn enter(&mut self, id: &DatasetID, info: &NodeInfo<'_>) -> bool {
        let fmt = match info {
            &NodeInfo::Local { kind, .. } => match kind {
                DatasetKind::Root => format!(
                    "{}{}",
                    console::style(id).bold(),
                    console::style(": Root").dim(),
                ),
                DatasetKind::Derivative => format!(
                    "{}{}",
                    console::style(id).bold(),
                    console::style(": Derivative").dim(),
                ),
            },
            &NodeInfo::Remote => {
                format!(
                    "{}{}",
                    console::style(id).dim(),
                    console::style(": N/A").dim(),
                )
            }
        };

        self.buffer.push(fmt);

        if let &NodeInfo::Remote = info {
            self.buffer.push(format!("{}", console::style("???").dim()))
        }

        true
    }

    fn exit(&mut self, _id: &DatasetID, info: &NodeInfo<'_>) {
        let num_deps = match info {
            &NodeInfo::Local { dependencies, .. } => dependencies.len(),
            &NodeInfo::Remote => 1, // Questionmark dep
        };

        let mut deps_left = num_deps;

        for line in self.buffer.iter_mut().rev() {
            if deps_left == 0 {
                break;
            }

            let is_last = deps_left == num_deps;

            let suffix = if line.starts_with(|c| c == '└' || c == '├' || c == '│' || c == ' ')
            {
                if is_last {
                    "    "
                } else {
                    "│   "
                }
            } else {
                deps_left -= 1;

                if is_last {
                    "└── "
                } else {
                    "├── "
                }
            };
            line.insert_str(0, suffix);
        }
    }

    fn done(&mut self) {
        for line in &self.buffer {
            println!("{}", line);
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// CSV
/////////////////////////////////////////////////////////////////////////////////////////

struct CsvVisitor {
    visited: HashSet<DatasetIDBuf>,
}

impl CsvVisitor {
    fn new() -> Self {
        Self {
            visited: HashSet::new(),
        }
    }
}

impl DependencyVisitor for CsvVisitor {
    fn begin(&mut self) {
        println!("id,available,depends_on");
    }

    fn enter(&mut self, id: &DatasetID, info: &NodeInfo<'_>) -> bool {
        if !self.visited.insert(id.to_owned()) {
            return false;
        }

        match info {
            &NodeInfo::Local { dependencies, .. } => {
                for dep in dependencies {
                    println!("\"{}\",\"true\",\"{}\"", id, dep);
                }
            }
            &NodeInfo::Remote => {
                println!("\"{}\",\"false\",\"\"", id);
            }
        }

        true
    }

    fn exit(&mut self, _id: &DatasetID, _info: &NodeInfo<'_>) {}

    fn done(&mut self) {}
}
