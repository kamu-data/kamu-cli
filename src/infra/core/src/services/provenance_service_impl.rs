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
use std::marker::PhantomData;
use std::sync::Arc;

use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_auth_rebac::{RebacDatasetIdUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::*;
use kamu_datasets::{DatasetRegistry, DependencyGraphService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ProvenanceServiceImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
}

#[component(pub)]
#[interface(dyn ProvenanceService)]
impl ProvenanceServiceImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
    ) -> Self {
        Self {
            dataset_registry,
            rebac_dataset_registry_facade,
            dependency_graph_service,
        }
    }

    #[async_recursion::async_recursion]
    async fn visit_upstream_dependencies_rec(
        &self,
        dataset_handle: &odf::DatasetHandle,
        visitor: &mut dyn LineageVisitor,
    ) -> Result<(), GetLineageError> {
        let target = self
            .rebac_dataset_registry_facade
            .resolve_dataset_by_handle(dataset_handle, auth::DatasetAction::Read)
            .await
            .map_err(|e| match e {
                RebacDatasetIdUnresolvedError::Access(e) => GetLineageError::Access(e),
                e @ RebacDatasetIdUnresolvedError::Internal(_) => {
                    GetLineageError::Internal(e.int_err())
                }
            })?;

        use tokio_stream::StreamExt;
        let upstream_dependencies = self
            .dependency_graph_service
            .get_upstream_dependencies(target.get_id())
            .await
            .collect::<Vec<_>>()
            .await;

        let mut resolved_inputs = Vec::new();
        for input_id in upstream_dependencies {
            // TODO: Private Datasets: check inputs
            //       Private Datasets: Checking dataset accessibility in `kamu` subcommands
            //       (multi-tenant workspace)
            //       https://github.com/kamu-data/kamu-cli/issues/1055
            let handle = self
                .dataset_registry
                .resolve_dataset_handle_by_ref(&input_id.as_local_ref())
                .await?;

            resolved_inputs.push(ResolvedTransformInput {
                // TODO: This likely needs to be changed into query alias
                name: handle.alias.dataset_name.clone(),
                handle,
            });
        }

        let dataset_info = NodeInfo::Local {
            id: dataset_handle.id.clone(),
            alias: dataset_handle.alias.clone(),
            kind: target.get_kind(),
            dependencies: &resolved_inputs,
        };

        if visitor.enter(&dataset_info) {
            for input in &resolved_inputs {
                self.visit_upstream_dependencies_rec(&input.handle, visitor)
                    .await?;
            }

            visitor.exit(&dataset_info);
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl ProvenanceService for ProvenanceServiceImpl {
    async fn get_dataset_lineage(
        &self,
        dataset_ref: &odf::DatasetRef,
        visitor: &mut dyn LineageVisitor,
        _options: LineageOptions,
    ) -> Result<(), GetLineageError> {
        let hdl = self
            .dataset_registry
            .resolve_dataset_handle_by_ref(dataset_ref)
            .await?;
        self.visit_upstream_dependencies_rec(&hdl, visitor).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DOT LineageVisitor
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DotVisitor<W: Write, S: DotStyle = DefaultStyle> {
    visited: HashSet<odf::DatasetID>,
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

impl<W: Write + Send, S: DotStyle + Send> LineageVisitor for DotVisitor<W, S> {
    fn begin(&mut self) {
        writeln!(self.writer, "digraph datasets {{\nrankdir = LR;").unwrap();
    }

    fn enter(&mut self, dataset: &NodeInfo<'_>) -> bool {
        if !self.visited.insert(dataset.id().clone()) {
            return false;
        }

        match dataset {
            NodeInfo::Local { alias, kind, .. } => match kind {
                odf::DatasetKind::Root => {
                    writeln!(self.writer, "\"{}\" [{}];", alias, S::root_style())
                }
                odf::DatasetKind::Derivative => {
                    writeln!(self.writer, "\"{}\" [{}];", alias, S::derivative_style())
                }
            },
            NodeInfo::Remote { alias, .. } => {
                writeln!(self.writer, "\"{}\" [{}];", alias, S::remote_style())
            }
        }
        .unwrap();

        if let &NodeInfo::Local { dependencies, .. } = dataset {
            for dep in dependencies {
                writeln!(
                    self.writer,
                    "\"{}\" -> \"{}\";",
                    dep.handle.alias,
                    dataset.alias()
                )
                .unwrap();
            }
        }

        true
    }

    fn exit(&mut self, _dataset: &NodeInfo<'_>) {}

    fn done(&mut self) -> Result<(), InternalError> {
        writeln!(self.writer, "}}").int_err()
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
        "style=filled, fillcolor=darkolivegreen1".to_string()
    }

    fn derivative_style() -> String {
        "style=filled, fillcolor=lightblue".to_string()
    }

    fn remote_style() -> String {
        "style=filled, fillcolor=gray".to_string()
    }
}
