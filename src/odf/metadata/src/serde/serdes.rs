// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;
use std::fmt::Display;

use chrono::{DateTime, Utc};
use prost::bytes;
use thiserror::Error;

use super::Buffer;
use crate::{
    DatasetSnapshot,
    MetadataBlock,
    OperationType,
    RawQueryRequest,
    RawQueryResponse,
    TransformRequest,
    TransformResponse,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataBlockVersion
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

pub const METADATA_BLOCK_SUPPORTED_VERSION_RANGE: (MetadataBlockVersion, MetadataBlockVersion) = (
    METADATA_BLOCK_MINIMUM_SUPPORTED_VERSION,
    METADATA_BLOCK_CURRENT_VERSION,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait MetadataBlockSerializer {
    fn write_manifest(&self, block: &MetadataBlock) -> Result<Buffer<u8>, Error>;
}

pub trait MetadataBlockDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<MetadataBlock, Error>;

    fn check_version_compatibility(version: MetadataBlockVersion) -> Result<(), Error> {
        match version {
            MetadataBlockVersion::Initial => {
                Err(Error::UnsupportedVersion(UnsupportedVersionError {
                    manifest_version: version as i32,
                    supported_version_range: METADATA_BLOCK_SUPPORTED_VERSION_RANGE,
                }))
            }
            MetadataBlockVersion::SequenceNumbers => Ok(()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatasetSnapshotSerializer {
    fn write_manifest(&self, snapshot: &DatasetSnapshot) -> Result<Buffer<u8>, Error>;
}

pub trait DatasetSnapshotDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<DatasetSnapshot, Error>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EngineProtocol
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait EngineProtocolSerializer {
    fn write_raw_query_request(&self, inst: &RawQueryRequest) -> Result<Buffer<u8>, Error>;
    fn write_raw_query_response(&self, inst: &RawQueryResponse) -> Result<Buffer<u8>, Error>;

    fn write_transform_request(&self, inst: &TransformRequest) -> Result<Buffer<u8>, Error>;
    fn write_transform_response(&self, inst: &TransformResponse) -> Result<Buffer<u8>, Error>;
}

pub trait EngineProtocolDeserializer {
    fn read_raw_query_request(&self, data: &[u8]) -> Result<RawQueryRequest, Error>;
    fn read_raw_query_response(&self, data: &[u8]) -> Result<RawQueryResponse, Error>;

    fn read_transform_request(&self, data: &[u8]) -> Result<TransformRequest, Error>;
    fn read_transform_response(&self, data: &[u8]) -> Result<TransformResponse, Error>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    #[error(transparent)]
    UnsupportedVersion(UnsupportedVersionError),
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
            MetadataBlockVersionError::UnsupportedVersion(e) => {
                Error::UnsupportedVersion(UnsupportedVersionError {
                    manifest_version: e,
                    supported_version_range: METADATA_BLOCK_SUPPORTED_VERSION_RANGE,
                })
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, PartialEq, Eq, Debug)]
pub struct UnsupportedVersionError {
    pub manifest_version: i32,
    pub supported_version_range: (MetadataBlockVersion, MetadataBlockVersion),
}

impl Display for UnsupportedVersionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let min_version = self.supported_version_range.0 as i32;
        let max_version = self.supported_version_range.1 as i32;
        if self.manifest_version < min_version {
            write!(
                f,
                "Obsolete version: manifest has version {} while minimum supported version is {}",
                self.manifest_version, min_version
            )?;
        } else if self.manifest_version > max_version {
            write!(
                f,
                "Unsupported version: manifest has version {} while maximum supported version is \
                 {}",
                self.manifest_version, max_version
            )?;
        } else {
            panic!("Version is supported")
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// OperationType
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl serde::Serialize for OperationType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> serde::Deserialize<'de> for OperationType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u8::deserialize(deserializer)?;

        OperationType::try_from(value).map_err(|e| {
            let error_message = e.to_string();
            serde::de::Error::custom(error_message)
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetDefaultVocabularyRecord
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DatasetDefaultVocabularySystemColumns {
    pub offset: Option<u64>, // Optional for creation

    pub op: OperationType,

    #[serde(with = "crate::serde::yaml::datetime_rfc3339")]
    pub system_time: DateTime<Utc>,

    #[serde(with = "crate::serde::yaml::datetime_rfc3339")]
    pub event_time: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound = "T: serde::Serialize + for<'a> serde::Deserialize<'a>")]
pub struct DatasetDefaultVocabularyChangelogEntry<T> {
    #[serde(flatten)]
    pub system_columns: DatasetDefaultVocabularySystemColumns,

    #[serde(flatten)]
    pub record: T,
}

impl<T> DatasetDefaultVocabularyChangelogEntry<T>
where
    T: serde::Serialize + for<'a> serde::Deserialize<'a>,
{
    pub fn to_bytes(&self) -> bytes::Bytes {
        let json = serde_json::to_string(self).unwrap();
        bytes::Bytes::from_owner(json.into_bytes())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
