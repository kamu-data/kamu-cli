// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{DomainError, TransformError, TransformListener};
use opendatafabric::{DatasetHandle, DatasetRefLocal, MetadataBlock, Multihash};

use std::fmt::Display;
use std::sync::Arc;
use std::usize;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait VerificationService: Send + Sync {
    fn verify(
        &self,
        dataset_ref: &DatasetRefLocal,
        block_range: (Option<Multihash>, Option<Multihash>),
        options: VerificationOptions,
        listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<VerificationResult, VerificationError>;

    fn verify_multi(
        &self,
        requests: &mut dyn Iterator<Item = VerificationRequest>,
        options: VerificationOptions,
        listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Result<VerificationResult, VerificationError>;
}

///////////////////////////////////////////////////////////////////////////////
// DTOs
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VerificationRequest {
    pub dataset_ref: DatasetRefLocal,
    pub block_range: (Option<Multihash>, Option<Multihash>),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum VerificationResult {
    Valid,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct VerificationOptions {
    pub check_integrity: bool,
    pub replay_transformations: bool,
}

impl Default for VerificationOptions {
    fn default() -> Self {
        Self {
            check_integrity: true,
            replay_transformations: true,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// Listeners
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationPhase {
    DataIntegrity,
    ReplayTransform,
}

// The call pattern is:
//   begin()
//     begin_phase(DataIntegrity)
//       begin_block()
//       end_block()
//       ...
//     end_phase(DataIntegrity)
//     begin_phase(ReplayTransform)
//       begin_block()
//         get_transform_listener()
//       end_block()
//       ...
//     end_phase(ReplayTransform)
//   success()
pub trait VerificationListener {
    fn begin(&self) {}
    fn success(&self, _result: &VerificationResult) {}
    fn error(&self, _error: &VerificationError) {}

    fn begin_phase(&self, _phase: VerificationPhase, _num_blocks: usize) {}
    fn end_phase(&self, _phase: VerificationPhase, _num_blocks: usize) {}

    fn begin_block(
        &self,
        _block_hash: &Multihash,
        _block_index: usize,
        _num_blocks: usize,
        _phase: VerificationPhase,
    ) {
    }
    fn end_block(
        &self,
        _block_hash: &Multihash,
        _block_index: usize,
        _num_blocks: usize,
        _phase: VerificationPhase,
    ) {
    }

    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformListener>> {
        None
    }
}

pub struct NullVerificationListener;
impl VerificationListener for NullVerificationListener {}

///////////////////////////////////////////////////////////////////////////////

pub trait VerificationMultiListener {
    fn begin_verify(&self, _dataset: &DatasetHandle) -> Option<Arc<dyn VerificationListener>> {
        None
    }
}

pub struct NullVerificationMultiListener;
impl VerificationMultiListener for NullVerificationMultiListener {}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum VerificationError {
    #[error("Dataset is not derivative")]
    NotDerivative,
    #[error("Block {0} not found")]
    NoSuchBlock(Multihash),
    #[error("Data doesn't match metadata: {0}")]
    DataDoesNotMatchMetadata(DataDoesNotMatchMetadata),
    #[error("Data is not reproducible: {0}")]
    DataNotReproducible(DataNotReproducible),
    #[error("Tranform error: {0}")]
    TransformError(#[from] TransformError),
    #[error("Domain error: {0}")]
    DomainError(#[from] DomainError),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct HashMismatch {
    pub expected: Multihash,
    pub actual: Multihash,
}

#[derive(Debug)]
pub struct DataDoesNotMatchMetadata {
    pub block_hash: Multihash,
    pub logical_hash: Option<HashMismatch>,
    pub physical_hash: Option<HashMismatch>,
}

impl Display for DataDoesNotMatchMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(logical) = &self.logical_hash {
            write!(
                f,
                "Logical data hash for block {} is expected to be {} but actual {}",
                self.block_hash, logical.expected, logical.actual
            )
        } else if let Some(physical) = &self.physical_hash {
            write!(
                f,
                "Physical data hash for block {} is expected to be {} but actual {}",
                self.block_hash, physical.expected, physical.actual
            )
        } else {
            Ok(())
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DataNotReproducible {
    pub expected_block_hash: Multihash,
    pub expected_block: MetadataBlock,
    pub actual_block_hash: Multihash,
    pub actual_block: MetadataBlock,
}

impl Display for DataNotReproducible {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Expected block {:?} ({:?}) but got non-equivalent block {:?} ({:?})",
            self.expected_block,
            self.expected_block_hash,
            self.actual_block,
            self.actual_block_hash
        )
    }
}
