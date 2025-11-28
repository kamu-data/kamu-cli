// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use file_utils::{MediaType, MediaTypeRef};
use internal_error::{InternalError, ResultIntoInternal};

use crate::ExtraDataFields;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type FileVersion = u32;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const VERSION_COLUMN_NAME: &str = "version";
pub const CONTENT_TYPE_COLUMN_NAME: &str = "content_type";
pub const CONTENT_LENGTH_COLUMN_NAME: &str = "content_length";
pub const CONTENT_HASH_COLUMN_NAME: &str = "content_hash";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VersionedFileEntry {
    /// Time when this entry was created
    pub system_time: DateTime<Utc>,

    /// Time when this entry was created
    pub event_time: DateTime<Utc>,

    /// File version
    pub version: FileVersion,

    /// Media type of the file content
    pub content_type: String,

    /// Size of the content in bytes
    pub content_length: usize,

    /// Multihash of the file content
    pub content_hash: odf::Multihash,

    /// Extra data associated with this file version
    #[serde(flatten)]
    pub extra_data: ExtraDataFields,
}

impl VersionedFileEntry {
    pub const DEFAULT_CONTENT_TYPE: MediaTypeRef<'static> =
        MediaTypeRef("application/octet-stream");

    pub fn new(
        system_time: DateTime<Utc>,
        event_time: DateTime<Utc>,
        version: FileVersion,
        content_hash: odf::Multihash,
        content_length: usize,
        content_type: Option<impl Into<MediaType>>,
        extra_data: Option<ExtraDataFields>,
    ) -> Self {
        let extra_data = extra_data.unwrap_or_default();

        Self {
            system_time,
            event_time,
            version,
            content_length,
            content_type: content_type
                .map(Into::into)
                .unwrap_or_else(|| Self::DEFAULT_CONTENT_TYPE.to_owned())
                .to_string(),
            content_hash,
            extra_data,
        }
    }

    pub fn to_bytes(self) -> bytes::Bytes {
        let buf = serde_json::to_string(&self).unwrap().into_bytes();
        bytes::Bytes::from_owner(buf)
    }

    pub fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let mut event: VersionedFileEvent = serde_json::from_value(record).int_err()?;

        let vocab = odf::metadata::DatasetVocabulary::default();
        event
            .record
            .extra_data
            .as_mut_map()
            .remove(&vocab.offset_column);
        event
            .record
            .extra_data
            .as_mut_map()
            .remove(&vocab.operation_type_column);

        Ok(Self {
            system_time: event.system_time,
            event_time: event.event_time,
            version: event.record.version,
            content_length: event.record.content_length,
            content_type: event.record.content_type,
            content_hash: event.record.content_hash,
            extra_data: event.record.extra_data,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Serialize, serde::Deserialize)]
struct VersionedFileEvent {
    #[serde(with = "odf::serde::yaml::datetime_rfc3339")]
    pub system_time: DateTime<Utc>,

    #[serde(with = "odf::serde::yaml::datetime_rfc3339")]
    pub event_time: DateTime<Utc>,

    #[serde(flatten)]
    pub record: VersionedFileRecord,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Used to serialize/deserialize entry from a dataset
#[derive(serde::Serialize, serde::Deserialize)]
struct VersionedFileRecord {
    pub version: FileVersion,

    pub content_type: String,

    pub content_length: usize,

    pub content_hash: odf::Multihash,

    #[serde(flatten)]
    pub extra_data: ExtraDataFields,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
