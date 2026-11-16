// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt::Write;
use std::path::PathBuf;
use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::*;
use kamu::{DotStyle, DotVisitor};
use kamu_datasets::DatasetRegistry;

use super::{CLIError, Command};
use crate::{OutputConfig, WorkspaceLayout};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum LineageOutputFormat {
    Shell,
    Dot,
    Csv,
    Html,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct InspectLineageCommand {
    output_config: Arc<OutputConfig>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    provenance_svc: Arc<dyn ProvenanceService>,
    workspace_layout: Arc<WorkspaceLayout>,

    #[dill::component(explicit)]
    dataset_refs: Vec<odf::DatasetRef>,

    #[dill::component(explicit)]
    browse: bool,

    #[dill::component(explicit)]
    output_format: Option<LineageOutputFormat>,
}

impl InspectLineageCommand {
    fn get_visitor(&self) -> Box<dyn LineageVisitor> {
        match self.output_format {
            None => {
                if self.output_config.is_tty {
                    if self.browse {
                        Box::new(HtmlBrowseVisitor::new(
                            self.workspace_layout.run_info_dir.join("lineage.html"),
                        ))
                    } else {
                        Box::new(ShellVisitor::new())
                    }
                } else {
                    Box::new(CsvVisitor::new())
                }
            }
            Some(LineageOutputFormat::Shell) => Box::new(ShellVisitor::new()),
            Some(LineageOutputFormat::Dot) => {
                Box::new(DotVisitor::new(WriteAdapter(std::io::stdout())))
            }
            Some(LineageOutputFormat::Csv) => Box::new(CsvVisitor::new()),
            Some(LineageOutputFormat::Html) => {
                Box::new(HtmlVisitor::new(WriteAdapter(std::io::stdout())))
            }
        }
    }
}

// TODO: Support temporality and evolution
#[async_trait::async_trait(?Send)]
impl Command for InspectLineageCommand {
    async fn run(&self) -> Result<(), CLIError> {
        use futures::{StreamExt, TryStreamExt};
        let mut dataset_handles: Vec<_> = if self.dataset_refs.is_empty() {
            self.dataset_registry
                .all_dataset_handles()
                .try_collect()
                .await?
        } else {
            futures::stream::iter(&self.dataset_refs)
                .then(|r| self.dataset_registry.resolve_dataset_handle_by_ref(r))
                .try_collect()
                .await
                .map_err(CLIError::failure)?
        };

        dataset_handles.sort_by(|a, b| a.alias.cmp(&b.alias));

        let mut visitor = self.get_visitor();
        visitor.begin();
        for dataset_handle in dataset_handles {
            self.provenance_svc
                .get_dataset_lineage(
                    &dataset_handle.as_local_ref(),
                    visitor.as_mut(),
                    LineageOptions {},
                )
                .await
                .map_err(CLIError::failure)?;
        }
        visitor.done().map_err(CLIError::failure)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Shell
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ShellVisitor {
    buffer: Vec<String>,
}

impl ShellVisitor {
    fn new() -> Self {
        Self { buffer: Vec::new() }
    }
}

impl LineageVisitor for ShellVisitor {
    fn begin(&mut self) {}

    fn enter(&mut self, dataset: &NodeInfo<'_>) -> bool {
        let fmt = match dataset {
            NodeInfo::Local { alias, kind, .. } => match kind {
                odf::DatasetKind::Root => format!(
                    "{}{}",
                    console::style(alias).bold(),
                    console::style(": Root").dim(),
                ),
                odf::DatasetKind::Derivative => format!(
                    "{}{}",
                    console::style(alias).bold(),
                    console::style(": Derivative").dim(),
                ),
            },
            NodeInfo::Remote { alias, .. } => {
                format!(
                    "{}{}",
                    console::style(alias).dim(),
                    console::style(": N/A").dim(),
                )
            }
        };

        self.buffer.push(fmt);

        if let &NodeInfo::Remote { .. } = dataset {
            self.buffer.push(format!("{}", console::style("???").dim()));
        }

        true
    }

    fn exit(&mut self, dataset: &NodeInfo<'_>) {
        let num_deps = match dataset {
            NodeInfo::Local { dependencies, .. } => dependencies.len(),
            NodeInfo::Remote { .. } => 1, // Questionmark dep
        };

        let mut deps_left = num_deps;

        for line in self.buffer.iter_mut().rev() {
            if deps_left == 0 {
                break;
            }

            let is_last = deps_left == num_deps;

            let suffix = if line.starts_with(['└', '├', '│', ' ']) {
                if is_last { "    " } else { "│   " }
            } else {
                deps_left -= 1;

                if is_last { "└── " } else { "├── " }
            };
            line.insert_str(0, suffix);
        }
    }

    fn done(&mut self) -> Result<(), InternalError> {
        for line in &self.buffer {
            println!("{line}");
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CSV
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CsvVisitor {
    visited: HashSet<odf::DatasetAlias>,
}

impl CsvVisitor {
    fn new() -> Self {
        Self {
            visited: HashSet::new(),
        }
    }
}

impl LineageVisitor for CsvVisitor {
    fn begin(&mut self) {
        println!("name,available,depends_on");
    }

    fn enter(&mut self, dataset: &NodeInfo<'_>) -> bool {
        if !self.visited.insert(dataset.alias().clone()) {
            return false;
        }

        match dataset {
            NodeInfo::Local { dependencies, .. } => {
                for dep in *dependencies {
                    println!("\"{}\",\"true\",\"{}\"", dataset.alias(), dep.name);
                }
            }
            NodeInfo::Remote { .. } => {
                println!("\"{}\",\"false\",\"\"", dataset.alias());
            }
        }

        true
    }

    fn exit(&mut self, _dataset: &NodeInfo<'_>) {}

    fn done(&mut self) -> Result<(), InternalError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// HTML
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct HtmlVisitor<W: Write> {
    dot_visitor: DotVisitor<String, HtmlStyle>,
    writer: W,
}

impl<W: Write> HtmlVisitor<W> {
    fn new(writer: W) -> Self {
        Self {
            dot_visitor: DotVisitor::new_with_style(String::new()),
            writer,
        }
    }

    fn unwrap(self) -> W {
        self.writer
    }

    const TEMPLATE: &'static str = include_str!("../../resources/lineage.html");
}

impl<W: Write + Send> LineageVisitor for HtmlVisitor<W> {
    fn begin(&mut self) {
        self.dot_visitor.begin();
    }

    fn enter(&mut self, dataset: &NodeInfo<'_>) -> bool {
        self.dot_visitor.enter(dataset)
    }

    fn exit(&mut self, dataset: &NodeInfo<'_>) {
        self.dot_visitor.exit(dataset);
    }

    fn done(&mut self) -> Result<(), InternalError> {
        self.dot_visitor.done()?;
        let mut visitor = DotVisitor::new_with_style(String::new());
        std::mem::swap(&mut visitor, &mut self.dot_visitor);
        let dot = visitor.unwrap();
        let dot_encoded = urlencoding::encode(&dot);

        let html = Self::TEMPLATE.replace("<URL_ENCODED_DOT>", &dot_encoded);
        write!(self.writer, "{html}").int_err()
    }
}

struct HtmlStyle;

impl DotStyle for HtmlStyle {
    fn root_style() -> String {
        r#"style="fill: orange""#.to_string()
    }

    fn derivative_style() -> String {
        r#"style="fill: lightblue""#.to_string()
    }

    fn remote_style() -> String {
        r#"style="fill: lightgrey""#.to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// HTML Browse
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct HtmlBrowseVisitor {
    html_visitor: HtmlVisitor<String>,
    temp_path: PathBuf,
}

impl HtmlBrowseVisitor {
    fn new<P: Into<PathBuf>>(temp_path: P) -> Self {
        Self {
            html_visitor: HtmlVisitor::new(String::new()),
            temp_path: temp_path.into(),
        }
    }
}

impl LineageVisitor for HtmlBrowseVisitor {
    fn begin(&mut self) {
        self.html_visitor.begin();
    }

    fn enter(&mut self, dataset: &NodeInfo<'_>) -> bool {
        self.html_visitor.enter(dataset)
    }

    fn exit(&mut self, dataset: &NodeInfo<'_>) {
        self.html_visitor.exit(dataset);
    }

    fn done(&mut self) -> Result<(), InternalError> {
        self.html_visitor.done()?;

        let mut visitor = HtmlVisitor::new(String::new());
        std::mem::swap(&mut visitor, &mut self.html_visitor);

        std::fs::write(&self.temp_path, visitor.unwrap()).int_err()?;
        webbrowser::open(url::Url::from_file_path(&self.temp_path).unwrap().as_ref()).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct WriteAdapter<W>(W);

impl<W: std::io::Write> Write for WriteAdapter<W> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        write!(self.0, "{s}").map_err(|_| std::fmt::Error)
    }
}
