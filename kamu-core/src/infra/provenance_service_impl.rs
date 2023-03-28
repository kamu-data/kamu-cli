// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use dill::*;
use opendatafabric::*;

use std::collections::HashSet;
use std::fmt::Write;
use std::marker::PhantomData;
use std::sync::Arc;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct ProvenanceServiceImpl {
    local_repo: Arc<dyn DatasetRepository>,
}

#[component(pub)]
impl ProvenanceServiceImpl {
    pub fn new(local_repo: Arc<dyn DatasetRepository>) -> Self {
        Self { local_repo }
    }

    #[async_recursion::async_recursion(?Send)]
    async fn visit_upstream_dependencies_rec(
        &self,
        dataset_handle: &DatasetHandle,
        visitor: &mut dyn LineageVisitor,
    ) -> Result<(), GetLineageError> {
        if let Some(dataset) = self
            .local_repo
            .try_get_dataset(&dataset_handle.as_local_ref())
            .await?
        {
            let summary = dataset
                .get_summary(GetSummaryOpts::default())
                .await
                .int_err()?;

            // Resolving by ID because name of the input
            // can be different than the dataset name in the workspace
            let mut resolved_inputs = Vec::new();
            for input in &summary.dependencies {
                let input_id = input.id.as_ref().unwrap();

                let handle = self
                    .local_repo
                    .resolve_dataset_ref(&input_id.as_local_ref())
                    .await?;

                resolved_inputs.push(ResolvedTransformInput {
                    handle,
                    name: input.name.clone(),
                })
            }

            let dataset_info = NodeInfo::Local {
                id: summary.id.clone(),
                name: dataset_handle.name.clone(),
                kind: summary.kind,
                dependencies: &resolved_inputs,
            };

            if visitor.enter(&dataset_info) {
                for input in &resolved_inputs {
                    self.visit_upstream_dependencies_rec(&input.handle, visitor)
                        .await?;
                }

                visitor.exit(&dataset_info);
            }
        } else {
            // Remote dataset
            let dataset_info = NodeInfo::Remote {
                id: dataset_handle.id.clone(),
                name: dataset_handle.name.clone(),
            };

            visitor.enter(&dataset_info);
        }

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl ProvenanceService for ProvenanceServiceImpl {
    async fn get_dataset_lineage(
        &self,
        dataset_ref: &DatasetRefLocal,
        visitor: &mut dyn LineageVisitor,
        _options: LineageOptions,
    ) -> Result<(), GetLineageError> {
        let hdl = self.local_repo.resolve_dataset_ref(dataset_ref).await?;
        self.visit_upstream_dependencies_rec(&hdl, visitor).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// DOT LineageVisitor
/////////////////////////////////////////////////////////////////////////////////////////

pub struct DotVisitor<W: Write, S: DotStyle = DefaultStyle> {
    visited: HashSet<DatasetID>,
    writer: W,
    _style: PhantomData<S>,
}

impl<W: Write> DotVisitor<W> {
    pub fn new(writer: W) -> Self {
        Self {
            visited: HashSet::new(),
            writer,
            _style: PhantomData,
        }
    }
}

impl<W: Write, S: DotStyle> DotVisitor<W, S> {
    pub fn new_with_style(writer: W) -> Self {
        Self {
            visited: HashSet::new(),
            writer,
            _style: PhantomData,
        }
    }

    pub fn unwrap(self) -> W {
        self.writer
    }
}

impl<W: Write, S: DotStyle> LineageVisitor for DotVisitor<W, S> {
    fn begin(&mut self) {
        writeln!(self.writer, "digraph datasets {{\nrankdir = LR;").unwrap();
    }

    fn enter(&mut self, dataset: &NodeInfo<'_>) -> bool {
        if !self.visited.insert(dataset.id().clone()) {
            return false;
        }

        match dataset {
            NodeInfo::Local { name, kind, .. } => match kind {
                DatasetKind::Root => {
                    writeln!(self.writer, "\"{}\" [{}];", name, S::root_style())
                }
                DatasetKind::Derivative => {
                    writeln!(self.writer, "\"{}\" [{}];", name, S::derivative_style())
                }
            },
            NodeInfo::Remote { name, .. } => {
                writeln!(self.writer, "\"{}\" [{}];", name, S::remote_style())
            }
        }
        .unwrap();

        if let &NodeInfo::Local { dependencies, .. } = dataset {
            for dep in dependencies {
                writeln!(
                    self.writer,
                    "\"{}\" -> \"{}\";",
                    dep.handle.name,
                    dataset.name()
                )
                .unwrap();
            }
        }

        true
    }

    fn exit(&mut self, _dataset: &NodeInfo<'_>) {}

    fn done(&mut self) {
        writeln!(self.writer, "}}").unwrap();
    }
}

pub trait DotStyle {
    fn root_style() -> String;
    fn derivative_style() -> String;
    fn remote_style() -> String;
}

pub struct DefaultStyle;

impl DotStyle for DefaultStyle {
    fn root_style() -> String {
        format!("style=filled, fillcolor=darkolivegreen1")
    }

    fn derivative_style() -> String {
        format!("style=filled, fillcolor=lightblue")
    }

    fn remote_style() -> String {
        format!("style=filled, fillcolor=gray")
    }
}
