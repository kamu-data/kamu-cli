// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_core::{MediaType, MediaTypeRef, ResolvedDataset};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type FileVersion = u32;

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct VersionedFileEntry {
    #[graphql(skip)]
    dataset: ResolvedDataset,

    /// System time when this version was created/updated
    pub system_time: DateTime<Utc>,

    /// Event time when this version was created/updated
    pub event_time: DateTime<Utc>,

    /// File version
    pub version: FileVersion,

    /// Media type of the file content
    pub content_type: String,

    /// Size of the content in bytes
    pub content_length: usize,

    /// Multihash of the file content
    pub content_hash: Multihash<'static>,

    /// Extra data associated with this file version
    pub extra_data: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl VersionedFileEntry {
    pub const DEFAULT_CONTENT_TYPE: MediaTypeRef<'static> =
        MediaTypeRef("application/octet-stream");

    pub fn new(
        dataset: ResolvedDataset,
        version: FileVersion,
        content_hash: odf::Multihash,
        content_length: usize,
        content_type: Option<impl Into<MediaType>>,
        extra_data: Option<serde_json::Value>,
    ) -> Self {
        let extra_data = extra_data.unwrap_or(serde_json::Value::Object(Default::default()));
        let now = Utc::now();

        Self {
            dataset,
            system_time: now,
            event_time: now,
            version,
            content_length,
            content_type: content_type
                .map(Into::into)
                .unwrap_or_else(|| Self::DEFAULT_CONTENT_TYPE.to_owned())
                .to_string(),
            content_hash: content_hash.into(),
            extra_data,
        }
    }

    pub fn from_json(dataset: ResolvedDataset, record: serde_json::Value) -> Self {
        let serde_json::Value::Object(mut record) = record else {
            unreachable!()
        };

        // Parse system columns
        let vocab = odf::metadata::DatasetVocabulary::default();
        record.remove(&vocab.offset_column);
        record.remove(&vocab.operation_type_column);
        let system_time = DateTime::parse_from_rfc3339(
            record
                .remove(&vocab.system_time_column)
                .unwrap()
                .as_str()
                .unwrap(),
        )
        .unwrap()
        .into();
        let event_time = DateTime::parse_from_rfc3339(
            record
                .remove(&vocab.event_time_column)
                .unwrap()
                .as_str()
                .unwrap(),
        )
        .unwrap()
        .into();

        // Parse core columns
        let version =
            FileVersion::try_from(record.remove("version").unwrap().as_i64().unwrap()).unwrap();
        // TODO: Restrict after migration
        let content_length = usize::try_from(
            record
                .remove("content_length")
                .unwrap()
                .as_i64()
                .unwrap_or_default(),
        )
        .unwrap();
        let content_type = record
            .remove("content_type")
            .unwrap()
            .as_str()
            .unwrap()
            .into();
        let content_hash = odf::Multihash::from_multibase(
            record.remove("content_hash").unwrap().as_str().unwrap(),
        )
        .unwrap()
        .into();

        Self {
            dataset,
            system_time,
            event_time,
            version,
            content_length,
            content_type,
            content_hash,
            extra_data: record.into(),
        }
    }

    pub fn to_record_data(&self) -> serde_json::Value {
        let mut record = self.extra_data.clone();
        record["version"] = self.version.into();
        record["content_hash"] = self.content_hash.to_string().into();
        record["content_length"] = self.content_length.into();
        record["content_type"] = self.content_type.clone().into();
        record
    }

    pub fn to_bytes(&self) -> bytes::Bytes {
        let buf = self.to_record_data().to_string().into_bytes();
        bytes::Bytes::from_owner(buf)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[ComplexObject]
impl VersionedFileEntry {
    /// Returns encoded content in-band. Should be used for small files only and
    /// will retrurn error if called on large data.
    #[tracing::instrument(level = "info", name = VersionedFileEntry_content, skip_all)]
    pub async fn content(&self) -> Result<Base64Usnp> {
        // TODO: Restrict by content size
        let data_repo = self.dataset.as_data_repo();
        let data = data_repo.get_bytes(&self.content_hash).await.int_err()?;
        Ok(Base64Usnp(data))
    }

    /// Returns a direct download URL
    #[tracing::instrument(level = "info", name = VersionedFileEntry_content_url, skip_all)]
    pub async fn content_url(&self) -> Result<VersionedFileContentDownload> {
        let data_repo = self.dataset.as_data_repo();
        let download = match data_repo
            .get_external_download_url(
                self.content_hash.as_ref(),
                odf::storage::ExternalTransferOpts::default(),
            )
            .await
        {
            Ok(res) => res,
            Err(err @ odf::storage::GetExternalUrlError::NotSupported) => {
                return Err(GqlError::Gql(err.into()))
            }
            Err(err) => return Err(err.int_err().into()),
        };

        Ok(VersionedFileContentDownload {
            url: download.url.to_string(),
            headers: download
                .header_map
                .into_iter()
                .map(|(k, v)| KeyValue {
                    key: k.unwrap().to_string(),
                    value: v.to_str().unwrap().to_string(),
                })
                .collect(),
            expires_at: download.expires_at,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    VersionedFileEntry,
    VersionedFileEntryConnection,
    VersionedFileEntryEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct VersionedFileContentDownload {
    /// Direct download URL
    pub url: String,

    /// Headers to include in request
    pub headers: Vec<KeyValue>,

    /// Download URL expiration timestamp
    pub expires_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
