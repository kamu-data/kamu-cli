// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use file_utils::{MediaType, MediaTypeRef};
use odf::Multihash;

pub type FileVersion = u32;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const VERSION_COLUMN_NAME: &str = "version";
pub const CONTENT_TYPE_COLUMN_NAME: &str = "content_type";
pub const CONTENT_LENGTH_COLUMN_NAME: &str = "content_length";
pub const CONTENT_HASH_COLUMN_NAME: &str = "content_hash";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ExtraDataFields(serde_json::Value);

impl ExtraDataFields {
    pub fn new(value: serde_json::Value) -> Self {
        Self(value)
    }

    pub fn into_inner(self) -> serde_json::Value {
        self.0
    }
}

impl Default for ExtraDataFields {
    fn default() -> Self {
        Self::new(serde_json::Value::Object(Default::default()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VersionedFileEntity {
    /// File version
    pub version: FileVersion,

    /// Media type of the file content
    pub content_type: String,

    /// Size of the content in bytes
    pub content_length: usize,

    /// Multihash of the file content
    pub content_hash: Multihash,

    /// Extra data associated with this file version
    pub extra_data: ExtraDataFields,
}

impl VersionedFileEntity {
    pub const DEFAULT_CONTENT_TYPE: MediaTypeRef<'static> =
        MediaTypeRef("application/octet-stream");

    pub fn new(
        version: FileVersion,
        content_hash: odf::Multihash,
        content_length: usize,
        content_type: Option<impl Into<MediaType>>,
        extra_data: Option<ExtraDataFields>,
    ) -> Self {
        let extra_data = extra_data.unwrap_or_default();

        Self {
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

    pub fn to_record_data(self) -> serde_json::Value {
        let mut record = self.extra_data.into_inner();
        record[VERSION_COLUMN_NAME] = self.version.into();
        record[CONTENT_HASH_COLUMN_NAME] = self.content_hash.to_string().into();
        record[CONTENT_LENGTH_COLUMN_NAME] = self.content_length.into();
        record[CONTENT_TYPE_COLUMN_NAME] = self.content_type.clone().into();
        record
    }

    pub fn to_bytes(self) -> bytes::Bytes {
        let buf = self.to_record_data().to_string().into_bytes();
        bytes::Bytes::from_owner(buf)
    }

    pub fn from_last_record(record: serde_json::Value) -> Self {
        let serde_json::Value::Object(mut record) = record else {
            unreachable!()
        };

        // Parse core columns
        let version = FileVersion::try_from(
            record
                .remove(VERSION_COLUMN_NAME)
                .unwrap()
                .as_i64()
                .unwrap(),
        )
        .unwrap();

        // TODO: Restrict after migration
        let content_length = usize::try_from(
            record
                .remove(CONTENT_LENGTH_COLUMN_NAME)
                .unwrap_or_default()
                .as_u64()
                .unwrap_or_default(),
        )
        .unwrap();
        let content_type = record
            .remove(CONTENT_TYPE_COLUMN_NAME)
            .unwrap()
            .as_str()
            .unwrap()
            .into();
        let content_hash = odf::Multihash::from_multibase(
            record
                .remove(CONTENT_HASH_COLUMN_NAME)
                .unwrap()
                .as_str()
                .unwrap(),
        )
        .unwrap()
        .into();

        Self {
            version,
            content_length,
            content_type,
            content_hash,
            extra_data: ExtraDataFields::new(record.into()),
        }
    }
}
