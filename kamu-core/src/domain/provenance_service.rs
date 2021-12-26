// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::*;
use thiserror::Error;

use super::DomainError;
use crate::infra::DatasetKind;

/////////////////////////////////////////////////////////////////////////////////////////

pub trait ProvenanceService: Sync + Send {
    /// Passes the visitor through the dependency graph of a dataset
    /// Some predefined visitors are available.
    fn get_dataset_lineage<'a>(
        &self,
        dataset_ref: &DatasetRefLocal,
        visitor: &mut dyn LineageVisitor,
        options: LineageOptions,
    ) -> Result<(), ProvenanceError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub trait LineageVisitor {
    fn begin(&mut self);
    fn enter(&mut self, dataset: &NodeInfo<'_>) -> bool;
    fn exit(&mut self, dataset: &NodeInfo<'_>);
    fn done(&mut self);
}

#[derive(Debug, Clone)]
pub enum NodeInfo<'a> {
    Local {
        id: DatasetID,
        name: DatasetName,
        kind: DatasetKind,
        dependencies: &'a [TransformInput],
    },
    Remote {
        id: DatasetID,
        name: DatasetName,
    },
}

impl<'a> NodeInfo<'a> {
    pub fn id(&self) -> &DatasetID {
        match self {
            NodeInfo::Local { id, .. } => id,
            NodeInfo::Remote { id, .. } => id,
        }
    }

    pub fn name(&self) -> &DatasetName {
        match self {
            NodeInfo::Local { name, .. } => name,
            NodeInfo::Remote { name, .. } => name,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct LineageOptions {}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ProvenanceError {
    #[error("Domain error: {0}")]
    DomainError(#[from] DomainError),
}
