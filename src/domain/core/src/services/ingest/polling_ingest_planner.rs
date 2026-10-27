// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use internal_error::InternalError;
use kamu_datasets::{DatasetEnvVar, ResolvedDataset};
use thiserror::Error;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Plans polling ingest iterations of root datasets. Every method only reads.
///
/// Call it in a transaction, with `target` resolved in that transaction, and
/// detach `target` before passing the plan to [`PollingIngestService`].
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait PollingIngestPlanner: Send + Sync {
    /// Plans the next iteration of `target` at its current head
    async fn plan_ingest(
        &self,
        target: ResolvedDataset,
    ) -> Result<PollingIngestPlan, PollingIngestPlanError>;

    /// Rebuilds the plan of a stored payload at the head it names
    async fn rebuild_ingest(
        &self,
        target: ResolvedDataset,
        stored: &PollingIngestPlanPayload,
    ) -> Result<PollingIngestPlan, PollingIngestRebuildError>;

    /// Resolves the dataset's environment variables for its fetch step. Empty
    /// when dataset environment variables are not enabled on this node. The
    /// values are never part of a plan; resolve them for each execution.
    async fn resolve_env_vars(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<IngestEnvVars, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One ingest iteration of a root dataset with a polling source
#[derive(Debug, Clone)]
pub struct PollingIngestPlan {
    metadata_state: Box<DataWriterMetadataState>,
}

impl PollingIngestPlan {
    /// Fails when the writer state has no polling source
    pub fn new(
        dataset_handle: &odf::DatasetHandle,
        metadata_state: Box<DataWriterMetadataState>,
    ) -> Result<Self, PollingSourceNotDefinedError> {
        if let (Some(odf::MetadataEvent::SetPollingSource(_)), Some(_)) =
            (&metadata_state.source_event, &metadata_state.source_block)
        {
            Ok(Self { metadata_state })
        } else {
            Err(PollingSourceNotDefinedError {
                dataset_handle: dataset_handle.clone(),
            })
        }
    }

    /// Head the new blocks are chained onto
    pub fn expected_head(&self) -> &odf::Multihash {
        &self.metadata_state.head
    }

    pub fn polling_source(&self) -> &odf::metadata::SetPollingSource {
        let Some(odf::MetadataEvent::SetPollingSource(source)) = &self.metadata_state.source_event
        else {
            unreachable!("Checked on construction")
        };
        source
    }

    /// Hash of the `SetPollingSource` block in effect
    pub fn source_block(&self) -> &odf::Multihash {
        self.metadata_state
            .source_block
            .as_ref()
            .expect("Checked on construction")
    }

    pub fn metadata_state(&self) -> &DataWriterMetadataState {
        &self.metadata_state
    }

    pub fn into_metadata_state(self) -> Box<DataWriterMetadataState> {
        self.metadata_state
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolved environment variables of a dataset. Held in memory for one
/// execution only: it has no serialized form, and its `Debug` lists keys only.
#[derive(Clone, Default)]
pub struct IngestEnvVars(HashMap<String, DatasetEnvVar>);

impl IngestEnvVars {
    pub fn new(env_vars: HashMap<String, DatasetEnvVar>) -> Self {
        Self(env_vars)
    }

    pub fn as_map(&self) -> &HashMap<String, DatasetEnvVar> {
        &self.0
    }
}

impl std::fmt::Debug for IngestEnvVars {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keys: Vec<_> = self.0.keys().collect();
        keys.sort();
        f.debug_tuple("IngestEnvVars").field(&keys).finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum PollingIngestPlanError {
    #[error(transparent)]
    SourceNotDefined(
        #[from]
        #[backtrace]
        PollingSourceNotDefinedError,
    ),
    #[error(transparent)]
    ScanMetadata(
        #[from]
        #[backtrace]
        ScanMetadataError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl PollingIngestPlanError {
    /// Whether planning again could succeed. A dataset without a polling
    /// source needs a change of metadata first.
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::SourceNotDefined(_)
            | Self::ScanMetadata(ScanMetadataError::SourceNotFound(_)) => false,
            Self::ScanMetadata(
                ScanMetadataError::HeadNotFound(_) | ScanMetadataError::Internal(_),
            )
            | Self::Internal(_) => true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum PollingIngestRebuildError {
    #[error(transparent)]
    HeadMoved(
        #[from]
        #[backtrace]
        PollingIngestHeadMovedError,
    ),
    #[error(transparent)]
    Mismatch(
        #[from]
        #[backtrace]
        PollingIngestPlanMismatchError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Dataset {dataset_handle} does not define a polling source")]
pub struct PollingSourceNotDefinedError {
    pub dataset_handle: odf::DatasetHandle,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The dataset's head is no longer the one a stored plan was made at. Discard
/// the plan and plan again.
#[derive(Debug, Error)]
#[error(
    "Head of {dataset_id} moved from {expected_head} to {actual_head} since the ingest was planned"
)]
pub struct PollingIngestHeadMovedError {
    pub dataset_id: odf::DatasetID,
    pub expected_head: odf::Multihash,
    pub actual_head: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The plan rebuilt from a stored payload differs from that payload. Metadata
/// at a fixed head is immutable, so this indicates a defect; the stored plan
/// cannot be trusted.
#[derive(Debug, Error)]
#[error("Ingest of {dataset_id} rebuilt from its stored plan differs from that plan: {reason}")]
pub struct PollingIngestPlanMismatchError {
    pub dataset_id: odf::DatasetID,
    pub reason: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
