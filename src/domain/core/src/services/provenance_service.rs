// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::auth;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ProvenanceService: Sync + Send {
    /// Passes the visitor through the dependency graph of a dataset
    /// Some predefined visitors are available.
    async fn get_dataset_lineage(
        &self,
        dataset_ref: &odf::DatasetRef,
        visitor: &mut dyn LineageVisitor,
        options: LineageOptions,
    ) -> Result<(), GetLineageError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait LineageVisitor: Send {
    fn begin(&mut self);
    fn enter(&mut self, dataset: &NodeInfo<'_>) -> bool;
    fn exit(&mut self, dataset: &NodeInfo<'_>);
    fn done(&mut self) -> Result<(), InternalError>;
}

#[derive(Debug, Clone)]
pub enum NodeInfo<'a> {
    Local {
        id: odf::DatasetID,
        alias: odf::DatasetAlias,
        kind: odf::DatasetKind,
        dependencies: &'a [ResolvedTransformInput],
    },
    Remote {
        id: odf::DatasetID,
        alias: odf::DatasetAlias,
    },
}

impl NodeInfo<'_> {
    pub fn id(&self) -> &odf::DatasetID {
        match self {
            NodeInfo::Local { id, .. } | NodeInfo::Remote { id, .. } => id,
        }
    }

    pub fn alias(&self) -> &odf::DatasetAlias {
        match self {
            NodeInfo::Local { alias, .. } | NodeInfo::Remote { alias, .. } => alias,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedTransformInput {
    /// Resolved input handle
    pub handle: odf::DatasetHandle,
    /// An alias of this input to be used in queries.
    pub name: odf::DatasetName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct LineageOptions {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetLineageError {
    #[error(transparent)]
    NotFound(#[from] odf::dataset::DatasetNotFoundError),
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<odf::dataset::GetDatasetError> for GetLineageError {
    fn from(v: odf::dataset::GetDatasetError) -> Self {
        match v {
            odf::dataset::GetDatasetError::NotFound(e) => Self::NotFound(e),
            odf::dataset::GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<auth::DatasetActionUnauthorizedError> for GetLineageError {
    fn from(v: auth::DatasetActionUnauthorizedError) -> Self {
        match v {
            auth::DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            auth::DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
