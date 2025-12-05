// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: scalar?
pub type FileVersion = u32;

#[derive(Clone)]
pub struct VersionedFileEntry {
    pub(crate) file_dataset: kamu_datasets::ResolvedDataset,
    pub(crate) entity: kamu_datasets::VersionedFileEntry,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl VersionedFileEntry {
    pub fn new(
        file_dataset: kamu_datasets::ResolvedDataset,
        entity: kamu_datasets::VersionedFileEntry,
    ) -> Self {
        Self {
            file_dataset,
            entity,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl VersionedFileEntry {
    /// System time when this version was created
    pub async fn system_time(&self) -> DateTime<Utc> {
        self.entity.system_time
    }

    /// Event time when this version was created
    pub async fn event_time(&self) -> DateTime<Utc> {
        self.entity.event_time
    }

    /// File version
    pub async fn version(&self) -> FileVersion {
        self.entity.version
    }

    /// Media type of the file content
    pub async fn content_type(&self) -> &str {
        &self.entity.content_type
    }

    /// Size of the content in bytes
    pub async fn content_length(&self) -> usize {
        self.entity.content_length
    }

    /// Multihash of the file content
    pub async fn content_hash(&self) -> Multihash<'_> {
        Multihash::from(&self.entity.content_hash)
    }

    /// Extra data associated with this file version
    pub async fn extra_data(&self) -> ExtraData {
        // TODO: avoid clone
        ExtraData::new(self.entity.extra_data.as_map().clone())
    }

    /// Returns encoded content in-band. Should be used for small files only and
    /// will return an error if called on large data.
    #[tracing::instrument(level = "info", name = VersionedFileEntry_content, skip_all)]
    pub async fn content(&self) -> Result<Base64Usnp> {
        // TODO: Restrict by content size
        let data_repo = self.file_dataset.as_data_repo();
        let data = data_repo
            .get_bytes(&self.entity.content_hash)
            .await
            .int_err()?;
        Ok(Base64Usnp(data))
    }

    /// Returns a direct download URL
    #[tracing::instrument(level = "info", name = VersionedFileEntry_content_url, skip_all)]
    pub async fn content_url(&self) -> Result<VersionedFileContentDownload> {
        let data_repo = self.file_dataset.as_data_repo();
        let download = match data_repo
            .get_external_download_url(
                &self.entity.content_hash,
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
