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
    pub extra_data: ExtraData,
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
        extra_data: Option<ExtraData>,
    ) -> Self {
        let extra_data = extra_data.unwrap_or_default();
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

    pub fn from_json(
        dataset: ResolvedDataset,
        record: serde_json::Value,
    ) -> Result<Self, InternalError> {
        let mut event: VersionedFileEvent = serde_json::from_value(record).int_err()?;

        let vocab = odf::metadata::DatasetVocabulary::default();
        event.record.extra_data.remove(&vocab.offset_column);
        event.record.extra_data.remove(&vocab.operation_type_column);

        Ok(Self {
            dataset,
            system_time: event.system_time,
            event_time: event.event_time,
            version: event.record.version,
            content_length: event.record.content_length,
            content_type: event.record.content_type,
            content_hash: event.record.content_hash.into(),
            extra_data: ExtraData::new(event.record.extra_data),
        })
    }

    pub fn into_input_record(self) -> VersionedFileRecord {
        VersionedFileRecord {
            version: self.version,
            content_type: self.content_type,
            content_length: self.content_length,
            content_hash: self.content_hash.into(),
            extra_data: self.extra_data.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[ComplexObject]
impl VersionedFileEntry {
    /// Returns encoded content in-band. Should be used for small files only and
    /// will return an error if called on large data.
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
                return Err(GqlError::Gql(err.into()));
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

    /// Headers to include in the request
    pub headers: Vec<KeyValue>,

    /// Download URL expiration timestamp
    pub expires_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Used to serialize/deserialize entry from a dataset
#[derive(serde::Serialize, serde::Deserialize)]
pub struct VersionedFileRecord {
    pub version: FileVersion,
    pub content_type: String,
    pub content_length: usize,
    pub content_hash: odf::Multihash,

    #[serde(flatten)]
    pub extra_data: serde_json::Map<String, serde_json::Value>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct VersionedFileEvent {
    #[serde(with = "odf::serde::yaml::datetime_rfc3339")]
    pub system_time: DateTime<Utc>,

    #[serde(with = "odf::serde::yaml::datetime_rfc3339")]
    pub event_time: DateTime<Utc>,

    #[serde(flatten)]
    pub record: VersionedFileRecord,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
