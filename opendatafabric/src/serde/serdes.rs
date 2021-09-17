// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{DatasetSnapshot, ExecuteQueryRequest, ExecuteQueryResponse, MetadataBlock, Sha3_256};
use std::backtrace::Backtrace;
use thiserror::Error;

use super::Buffer;

///////////////////////////////////////////////////////////////////////////////
// MetadataBlock
///////////////////////////////////////////////////////////////////////////////

pub trait MetadataBlockSerializer {
    fn write_manifest(&self, block: &MetadataBlock) -> Result<(Sha3_256, Buffer<u8>), Error>;

    fn write_manifest_unchecked(&self, block: &MetadataBlock) -> Result<Buffer<u8>, Error>;
}

pub trait MetadataBlockDeserializer {
    fn validate_manifest(&self, data: &[u8]) -> Result<(), Error>;

    fn read_manifest(&self, data: &[u8]) -> Result<MetadataBlock, Error>;

    fn read_manifest_unchecked(&self, data: &[u8]) -> Result<MetadataBlock, Error>;
}

///////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
///////////////////////////////////////////////////////////////////////////////

pub trait DatasetSnapshotSerializer {
    fn write_manifest(&self, snapshot: &DatasetSnapshot) -> Result<Buffer<u8>, Error>;
}

pub trait DatasetSnapshotDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<DatasetSnapshot, Error>;
}

///////////////////////////////////////////////////////////////////////////////
// EngineProtocol
///////////////////////////////////////////////////////////////////////////////

pub trait EngineProtocolSerializer {
    fn write_execute_query_request(&self, inst: &ExecuteQueryRequest) -> Result<Buffer<u8>, Error>;

    fn write_execute_query_response(
        &self,
        inst: &ExecuteQueryResponse,
    ) -> Result<Buffer<u8>, Error>;
}

pub trait EngineProtocolDeserializer {
    fn read_execute_query_request(&self, data: &[u8]) -> Result<ExecuteQueryRequest, Error>;
    fn read_execute_query_response(&self, data: &[u8]) -> Result<ExecuteQueryResponse, Error>;
}

///////////////////////////////////////////////////////////////////////////////

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid hash {actual} expected {expected}")]
    InvalidHash {
        actual: Sha3_256,
        expected: Sha3_256,
    },
    #[error("IO error: {source}")]
    IoError {
        #[from]
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[error("Serde error: {source}")]
    SerdeError {
        source: BoxedError,
        backtrace: Backtrace,
    },
}

impl Error {
    pub fn invalid_hash(actual: Sha3_256, expected: Sha3_256) -> Self {
        Self::InvalidHash {
            actual: actual,
            expected: expected,
        }
    }

    pub fn io_error(e: std::io::Error) -> Self {
        Self::IoError {
            source: e,
            backtrace: Backtrace::capture(),
        }
    }

    pub fn serde(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::SerdeError {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}
