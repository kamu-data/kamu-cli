// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{DomainError, TransformError, TransformListener};
use opendatafabric::{DatasetID, MetadataBlock, Sha3_256};

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
        dataset_id: &DatasetID,
        block_range: (Option<Sha3_256>, Option<Sha3_256>),
        options: VerificationOptions,
        listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<VerificationResult, VerificationError>;

    fn verify_multi(
        &self,
        datasets: &mut dyn Iterator<Item = VerificationRequest>,
        options: VerificationOptions,
        listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Result<VerificationResult, VerificationError>;
}

///////////////////////////////////////////////////////////////////////////////
// DTOs
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VerificationRequest<'a> {
    pub dataset_id: &'a DatasetID,
    pub block_range: (Option<Sha3_256>, Option<Sha3_256>),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum VerificationResult {
    Valid { blocks_verified: usize },
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
        _block_hash: &Sha3_256,
        _block_index: usize,
        _num_blocks: usize,
        _phase: VerificationPhase,
    ) {
    }
    fn end_block(
        &self,
        _block_hash: &Sha3_256,
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
    fn begin_verify(&self, _dataset_id: &DatasetID) -> Option<Arc<dyn VerificationListener>> {
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
    NoSuchBlock(Sha3_256),
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
pub struct DataDoesNotMatchMetadata {
    pub block_hash: Sha3_256,
    pub data_logical_hash_expected: String,
    pub data_logical_hash_actual: String,
}

impl Display for DataDoesNotMatchMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Data hash for block {} is expected to be {} but actual {}",
            self.block_hash, self.data_logical_hash_expected, self.data_logical_hash_actual
        )
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DataNotReproducible {
    pub expected_block: MetadataBlock,
    pub actual_block: MetadataBlock,
}

impl Display for DataNotReproducible {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Expected block {:?} but got non-equivalent block {:?}",
            self.expected_block, self.actual_block
        )
    }
}
