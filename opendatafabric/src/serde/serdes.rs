// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{DatasetSnapshot, ExecuteQueryRequest, ExecuteQueryResponse, MetadataBlock};
use std::backtrace::Backtrace;
use thiserror::Error;

use super::Buffer;

///////////////////////////////////////////////////////////////////////////////
// MetadataBlockVersion
///////////////////////////////////////////////////////////////////////////////

pub enum MetadataBlockVersion {
    Initial = 1,
    SequenceNumbers = 2,
}

#[derive(Error, Debug)]
pub enum MetadataBlockVersionError {
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(i32),
}

impl TryFrom<i32> for MetadataBlockVersion {
    type Error = MetadataBlockVersionError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(MetadataBlockVersion::Initial),
            2 => Ok(MetadataBlockVersion::SequenceNumbers),
            _ => Err(MetadataBlockVersionError::UnsupportedVersion(value)),
        }
    }
}

pub const METADATA_BLOCK_MINIMUM_SUPPORTED_VERSION: MetadataBlockVersion =
    MetadataBlockVersion::SequenceNumbers;

pub const METADATA_BLOCK_CURRENT_VERSION: MetadataBlockVersion =
    MetadataBlockVersion::SequenceNumbers;

///////////////////////////////////////////////////////////////////////////////
// MetadataBlock
///////////////////////////////////////////////////////////////////////////////

pub trait MetadataBlockSerializer {
    fn write_manifest(&self, block: &MetadataBlock) -> Result<Buffer<u8>, Error>;
}

pub trait MetadataBlockDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<MetadataBlock, Error>;

    fn check_version_compatibility(version: MetadataBlockVersion) -> Result<(), Error> {
        match version {
            MetadataBlockVersion::Initial => Err(Error::ObsoleteVersion {
                manifest_version: version as i32,
                minimum_supported_version: METADATA_BLOCK_MINIMUM_SUPPORTED_VERSION as i32,
            }),
            MetadataBlockVersion::SequenceNumbers => Ok(()),
        }
    }
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
    #[error("Unsupported version: manifest has version {manifest_version} while maximum supported version is {supported_version}")]
    UnsupportedVersion {
        manifest_version: i32,
        supported_version: i32,
    },
    #[error("Obsolete version: manifest has version {manifest_version} while minimum supported version is {minimum_supported_version}")]
    ObsoleteVersion {
        manifest_version: i32,
        minimum_supported_version: i32,
    },
}

impl Error {
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

impl From<MetadataBlockVersionError> for Error {
    fn from(e: MetadataBlockVersionError) -> Self {
        match e {
            MetadataBlockVersionError::UnsupportedVersion(e) => Error::UnsupportedVersion {
                manifest_version: e,
                supported_version: (METADATA_BLOCK_CURRENT_VERSION as i32),
            },
        }
    }
}
