// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::HashSet, fmt::Write, marker::PhantomData, sync::Arc};

use dill::*;
use opendatafabric::*;

use crate::domain::{
    DomainError, LineageOptions, LineageVisitor, MetadataRepository, NodeInfo, ProvenanceError,
    ProvenanceService,
};

use super::DatasetKind;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct ProvenanceServiceImpl {
    metadata_repo: Arc<dyn MetadataRepository>,
}

#[component(pub)]
impl ProvenanceServiceImpl {
    pub fn new(metadata_repo: Arc<dyn MetadataRepository>) -> Self {
        Self { metadata_repo }
    }

    fn visit_upstream_dependencies_rec(
        &self,
        dataset_handle: &DatasetHandle,
        visitor: &mut dyn LineageVisitor,
    ) -> Result<(), ProvenanceError> {
        let summary = match self
            .metadata_repo
            .get_summary(&dataset_handle.as_local_ref())
        {
            Ok(s) => Some(s),
            Err(DomainError::DoesNotExist { .. }) => None,
            Err(e) => return Err(e.into()),
        };

        let dataset_info = summary
            .as_ref()
            .map(|s| NodeInfo::Local {
                id: s.id.clone(),
                name: s.name.clone(),
                kind: s.kind,
                dependencies: &s.dependencies,
            })
            .unwrap_or_else(|| NodeInfo::Remote {
                id: dataset_handle.id.clone(),
                name: dataset_handle.name.clone(),
            });

        if !visitor.enter(&dataset_info) {
            return Ok(());
        }

        if let Some(s) = &summary {
            for input in &s.dependencies {
                let input_handle = DatasetHandle {
                    id: input.id.clone().unwrap(),
                    name: input.name.clone(),
                };
                self.visit_upstream_dependencies_rec(&input_handle, visitor)?;
            }
        }

        visitor.exit(&dataset_info);

        Ok(())
    }
}

impl ProvenanceService for ProvenanceServiceImpl {
    fn get_dataset_lineage(
        &self,
        dataset_ref: &DatasetRefLocal,
        visitor: &mut dyn LineageVisitor,
        _options: LineageOptions,
    ) -> Result<(), ProvenanceError> {
        let hdl = self.metadata_repo.resolve_dataset_ref(dataset_ref)?;
        self.visit_upstream_dependencies_rec(&hdl, visitor)
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
                writeln!(self.writer, "\"{}\" -> \"{}\";", dep.name, dataset.name()).unwrap();
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
