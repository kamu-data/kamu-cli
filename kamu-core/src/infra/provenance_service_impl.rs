use std::{collections::HashSet, fmt::Write, marker::PhantomData, sync::Arc};

use dill::*;
use opendatafabric::{DatasetID, DatasetIDBuf};

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
        id: &DatasetID,
        visitor: &mut dyn LineageVisitor,
    ) -> Result<(), ProvenanceError> {
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
                self.visit_upstream_dependencies_rec(dep_id, visitor)?;
            }
        }

        visitor.exit(id, &info);

        Ok(())
    }
}

impl ProvenanceService for ProvenanceServiceImpl {
    fn get_dataset_lineage(
        &self,
        dataset_id: &DatasetID,
        visitor: &mut dyn LineageVisitor,
        _options: LineageOptions,
    ) -> Result<(), ProvenanceError> {
        self.visit_upstream_dependencies_rec(dataset_id, visitor)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// DOT LineageVisitor
/////////////////////////////////////////////////////////////////////////////////////////

pub struct DotVisitor<W: Write, S: DotStyle = DefaultStyle> {
    visited: HashSet<DatasetIDBuf>,
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

    fn enter(&mut self, id: &DatasetID, info: &NodeInfo<'_>) -> bool {
        if !self.visited.insert(id.to_owned()) {
            return false;
        }

        match info {
            &NodeInfo::Local { kind, .. } => match kind {
                DatasetKind::Root => {
                    writeln!(self.writer, "\"{}\" [{}];", id, S::root_style())
                }
                DatasetKind::Derivative => {
                    writeln!(self.writer, "\"{}\" [{}];", id, S::derivative_style())
                }
            },
            &NodeInfo::Remote => {
                writeln!(self.writer, "\"{}\" [{}];", id, S::remote_style())
            }
        }
        .unwrap();

        if let &NodeInfo::Local { dependencies, .. } = info {
            for dep in dependencies {
                writeln!(self.writer, "\"{}\" -> \"{}\";", dep, id).unwrap();
            }
        }

        true
    }

    fn exit(&mut self, _id: &DatasetID, _info: &NodeInfo<'_>) {}

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
