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

#[nutype::nutype(derive(AsRef, Clone, Debug, Into))]
pub struct ExtraDataFields(serde_json::Value);

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
        let mut record: serde_json::Value = self.extra_data.into();
        record["version"] = self.version.into();
        record["content_hash"] = self.content_hash.to_string().into();
        record["content_length"] = self.content_length.into();
        record["content_type"] = self.content_type.clone().into();
        record
    }

    pub fn to_bytes(self) -> bytes::Bytes {
        let buf = self.to_record_data().to_string().into_bytes();
        bytes::Bytes::from_owner(buf)
    }
}
