// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::OutputConfig;

use super::{CLIError, Command};
use kamu::domain::*;
use kamu::infra::DatasetKind;
use kamu::infra::DotStyle;
use kamu::infra::DotVisitor;
use kamu::infra::WorkspaceLayout;
use opendatafabric::DatasetID;
use opendatafabric::DatasetIDBuf;

use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt::Write;
use std::path::PathBuf;
use std::sync::Arc;

pub struct LineageCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    provenance_svc: Arc<dyn ProvenanceService>,
    workspace_layout: Arc<WorkspaceLayout>,
    ids: Vec<DatasetIDBuf>,
    browse: bool,
    output_format: Option<String>,
    output_config: Arc<OutputConfig>,
}

impl LineageCommand {
    pub fn new<I, S>(
        metadata_repo: Arc<dyn MetadataRepository>,
        provenance_svc: Arc<dyn ProvenanceService>,
        workspace_layout: Arc<WorkspaceLayout>,
        ids: I,
        browse: bool,
        output_format: Option<&str>,
        output_config: Arc<OutputConfig>,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            metadata_repo,
            provenance_svc,
            workspace_layout,
            ids: ids
                .into_iter()
                .map(|s| DatasetIDBuf::try_from(s.into()).unwrap())
                .collect(),
            browse,
            output_format: output_format.map(|s| s.to_owned()),
            output_config,
        }
    }

    fn get_visitor(&self) -> Box<dyn LineageVisitor> {
        if self.output_format.is_none() {
            return if self.output_config.is_tty {
                if self.browse {
                    Box::new(HtmlBrowseVisitor::new(
                        self.workspace_layout.run_info_dir.join("lineage.html"),
                    ))
                } else {
                    Box::new(ShellVisitor::new())
                }
            } else {
                Box::new(CsvVisitor::new())
            };
        }

        match self.output_format.as_ref().unwrap().as_str() {
            "shell" => Box::new(ShellVisitor::new()),
            "dot" => Box::new(DotVisitor::new(WriteAdapter(std::io::stdout()))),
            "csv" => Box::new(CsvVisitor::new()),
            "html" => Box::new(HtmlVisitor::new(WriteAdapter(std::io::stdout()))),
            "html-browse" => Box::new(HtmlBrowseVisitor::new(
                self.workspace_layout.run_info_dir.join("lineage.html"),
            )),
            _ => unimplemented!(),
        }
    }
}

// TODO: Support temporality and evolution
impl Command for LineageCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let mut dataset_ids: Vec<_> = if self.ids.is_empty() {
            self.metadata_repo.get_all_datasets().collect()
        } else {
            self.ids.clone()
        };

        dataset_ids.sort();

        let mut visitor = self.get_visitor();
        visitor.begin();
        for dataset_id in dataset_ids {
            self.provenance_svc
                .get_dataset_lineage(&dataset_id, visitor.as_mut(), LineageOptions {})
                .map_err(|e| CLIError::failure(e))?;
        }
        visitor.done();

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Shell
/////////////////////////////////////////////////////////////////////////////////////////

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

impl LineageVisitor for CsvVisitor {
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

/////////////////////////////////////////////////////////////////////////////////////////
// HTML
/////////////////////////////////////////////////////////////////////////////////////////

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

    const TEMPLATE: &'static str = indoc::indoc!(
        r#"
    <!doctype html>

    <!--
    Based on https://github.com/cpettitt/dagre-d3/blob/d215446e7e40ebfca303f4733e746e96420e3b46/demo/interactive-demo.html
    which is published under this license:
    
    Copyright (c) 2013 Chris Pettitt
    
    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:
    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    THE SOFTWARE.
    -->

    <meta charset="utf-8">
    <title>Dependency Graph</title>
    
    <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.17/d3.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/2.1.4/jquery.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/dagre-d3/0.4.16/dagre-d3.min.js"></script>
    <script src="https://dagrejs.github.io/project/graphlib-dot/v0.6.3/graphlib-dot.min.js"></script>
    
    <style>
        h1,
        h2 {
            color: #333;
        }

        body {
            margin: 0;
            overflow: hidden;
        }
    
        .node {
            white-space: nowrap;
        }
    
        .node rect,
        .node circle,
        .node ellipse {
            stroke: #333;
            fill: #fff;
            stroke-width: 1.5px;
        }
    
        .cluster rect {
            stroke: #333;
            fill: #000;
            fill-opacity: 0.1;
            stroke-width: 1.5px;
        }
    
        .edgePath path.path {
            stroke: #333;
            stroke-width: 1.5px;
            fill: none;
        }
    </style>
    
    <body onLoad="initialize()">
    
        <svg width=1280 height=1024>
            <g />
        </svg>
    
        <script>
            data = "<URL_ENCODED_DOT>";
    
            function initialize() {
                // Set up zoom support
                var svg = d3.select("svg"),
                    inner = d3.select("svg g"),
                    zoom = d3.behavior.zoom().on("zoom", function () {
                        inner.attr("transform", "translate(" + d3.event.translate + ")" +
                            "scale(" + d3.event.scale + ")");
                    });
                svg.attr("width", window.innerWidth);
    
                svg.call(zoom);
                // Create and configure the renderer
                var render = dagreD3.render();
                function tryDraw(inputGraph) {
                    var g;
                    {
                        g = graphlibDot.read(inputGraph);
                        g.graph().rankdir = "LR";
                        d3.select("svg g").call(render, g);
    
                        // Center the graph
                        //var initialScale = 0.10;
                        //zoom
                        //  .translate([(svg.attr("width") - g.graph().width * initialScale) / 2, 20])
                        //  .scale(initialScale)
                        //  .event(svg);
                        //svg.attr('height', g.graph().height * initialScale + 40);
                    }
                }
                tryDraw(decodeURIComponent(data));
            }
        </script>
    </body>"#
    );
}

impl<W: Write> LineageVisitor for HtmlVisitor<W> {
    fn begin(&mut self) {
        self.dot_visitor.begin()
    }

    fn enter(&mut self, id: &DatasetID, info: &NodeInfo<'_>) -> bool {
        self.dot_visitor.enter(id, info)
    }

    fn exit(&mut self, id: &DatasetID, info: &NodeInfo<'_>) {
        self.dot_visitor.exit(id, info)
    }

    fn done(&mut self) {
        self.dot_visitor.done();
        let mut visitor = DotVisitor::new_with_style(String::new());
        std::mem::swap(&mut visitor, &mut self.dot_visitor);
        let dot = visitor.unwrap();
        let dot_encoded = urlencoding::encode(&dot);

        let html = Self::TEMPLATE.replace("<URL_ENCODED_DOT>", &dot_encoded);
        write!(self.writer, "{}", html).unwrap();
    }
}

struct HtmlStyle;

impl DotStyle for HtmlStyle {
    fn root_style() -> String {
        format!(r#"style="fill: orange""#)
    }

    fn derivative_style() -> String {
        format!(r#"style="fill: lightblue""#)
    }

    fn remote_style() -> String {
        format!(r#"style="fill: lightgrey""#)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// HTML Browse
/////////////////////////////////////////////////////////////////////////////////////////

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
        self.html_visitor.begin()
    }

    fn enter(&mut self, id: &DatasetID, info: &NodeInfo<'_>) -> bool {
        self.html_visitor.enter(id, info)
    }

    fn exit(&mut self, id: &DatasetID, info: &NodeInfo<'_>) {
        self.html_visitor.exit(id, info)
    }

    fn done(&mut self) {
        self.html_visitor.done();

        let mut visitor = HtmlVisitor::new(String::new());
        std::mem::swap(&mut visitor, &mut self.html_visitor);

        std::fs::write(&self.temp_path, visitor.unwrap()).unwrap();
        webbrowser::open(
            &url::Url::from_file_path(&self.temp_path)
                .unwrap()
                .to_string(),
        )
        .unwrap();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct WriteAdapter<W>(W);

impl<W: std::io::Write> Write for WriteAdapter<W> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        write!(self.0, "{}", s).map_err(|_| std::fmt::Error)
    }
}
