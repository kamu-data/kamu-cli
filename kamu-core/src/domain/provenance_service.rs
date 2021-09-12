// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{DatasetID, DatasetIDBuf};
use thiserror::Error;

use crate::infra::DatasetKind;

use super::DomainError;

/////////////////////////////////////////////////////////////////////////////////////////

pub trait ProvenanceService: Sync + Send {
    /// Passes the visitor through the dependency graph of a dataset
    /// Some predefined visitors are available.
    fn get_dataset_lineage(
        &self,
        dataset_id: &DatasetID,
        visitor: &mut dyn LineageVisitor,
        options: LineageOptions,
    ) -> Result<(), ProvenanceError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub trait LineageVisitor {
    fn begin(&mut self);
    fn enter(&mut self, id: &DatasetID, info: &NodeInfo<'_>) -> bool;
    fn exit(&mut self, id: &DatasetID, info: &NodeInfo<'_>);
    fn done(&mut self);
}

#[derive(Debug, Clone)]
pub enum NodeInfo<'a> {
    Local {
        kind: DatasetKind,
        dependencies: &'a [DatasetIDBuf],
    },
    Remote,
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct LineageOptions {}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ProvenanceError {
    #[error("Domain error: {0}")]
    DomainError(#[from] DomainError),
}
