// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_core::{FileDownload, FileDownloadService};

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

/// Single version of a versioned file
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
        self.entity.content_type.0.as_str()
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

    /// Returns encoded content in-band. Should be used for small files only:
    /// files over the node's in-band size limit fail with an error whose
    /// `extensions.code` is `CONTENT_TOO_LARGE`
    #[tracing::instrument(level = "info", name = VersionedFileEntry_content, skip_all)]
    pub async fn content(&self, ctx: &Context<'_>) -> Result<Base64Usnp> {
        let config = from_catalog_n!(ctx, kamu_datasets::VersionedFilesConfig);
        let limit = config.max_in_band_content_size_in_bytes;
        let content_length = self.entity.content_length;

        if content_length > limit {
            return Err(GqlError::Gql(
                async_graphql::Error::new(format!(
                    "Content of {content_length} bytes exceeds the in-band limit of {limit} \
                     bytes, use contentUrl instead"
                ))
                .extend_with(|_, ext| {
                    ext.set("code", "CONTENT_TOO_LARGE");
                    ext.set("contentLength", content_length);
                    ext.set("limit", limit);
                }),
            ));
        }

        let data_repo = self.file_dataset.as_data_repo();
        let data = data_repo
            .get_bytes(&self.entity.content_hash)
            .await
            .int_err()?;
        Ok(Base64Usnp(data))
    }

    /// Returns a direct download URL. When the storage cannot issue one, the
    /// URL points to the node itself and needs no extra headers
    #[tracing::instrument(level = "info", name = VersionedFileEntry_content_url, skip_all)]
    pub async fn content_url(&self, ctx: &Context<'_>) -> Result<VersionedFileContentDownload> {
        let data_repo = self.file_dataset.as_data_repo();
        let download_data = match data_repo
            .get_external_download_url(
                &self.entity.content_hash,
                odf::storage::ExternalTransferOpts::default(),
            )
            .await
        {
            Ok(res) => res,
            Err(odf::storage::GetExternalUrlError::NotSupported) => {
                let download_service = from_catalog_n!(ctx, dyn FileDownloadService);
                download_service.make_download_url(
                    FileDownload {
                        dataset_id: self.file_dataset.get_id().clone(),
                        content_hash: self.entity.content_hash.clone(),
                        content_type: self.entity.content_type.clone(),
                        content_length: self.entity.content_length,
                    },
                    odf::storage::ExternalTransferOpts::default(),
                )?
            }
            Err(err) => return Err(err.int_err().into()),
        };

        Ok(VersionedFileContentDownload::from(download_data))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    VersionedFileEntry,
    VersionedFileEntryConnection,
    VersionedFileEntryEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Information needed to download file content directly from storage
#[derive(SimpleObject)]
pub struct VersionedFileContentDownload {
    /// Direct download URL
    pub url: String,

    /// Headers to include in the request
    pub headers: Vec<KeyValue>,

    /// Download URL expiration timestamp
    pub expires_at: Option<DateTime<Utc>>,
}

impl From<odf::storage::GetExternalUrlResult> for VersionedFileContentDownload {
    fn from(value: odf::storage::GetExternalUrlResult) -> Self {
        Self {
            url: value.url.to_string(),
            headers: value
                .header_map
                .into_iter()
                .map(|(k, v)| KeyValue {
                    key: k.unwrap().to_string(),
                    value: v.to_str().unwrap().to_string(),
                })
                .collect(),
            expires_at: value.expires_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
