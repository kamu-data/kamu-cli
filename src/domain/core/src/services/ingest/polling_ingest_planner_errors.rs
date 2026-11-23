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

use crate::ScanMetadataError;

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
            Self::SourceNotDefined(_) => false,
            Self::ScanMetadata(e) => e.is_recoverable(),
            Self::Internal(_) => true,
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
