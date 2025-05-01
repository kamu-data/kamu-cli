// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu::domain;
use kamu_core::{MediaType, MediaTypeRef};

use crate::prelude::*;
use crate::queries::DatasetRequestStateWithOwner;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) type FileVersion = u32;

pub(crate) struct FileVersionRecord(serde_json::Map<String, serde_json::Value>);

impl FileVersionRecord {
    pub const DEFAULT_CONTENT_TYPE: MediaTypeRef<'static> =
        MediaTypeRef("application/octet-stream");

    pub fn new(
        version: FileVersion,
        content_hash: odf::Multihash,
        content_type: Option<impl Into<MediaType>>,
        extra_data: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Self {
        let mut record = extra_data.unwrap_or_default();

        record.insert("version".to_string(), version.into());
        record.insert("content_hash".to_string(), content_hash.to_string().into());
        record.insert(
            "content_type".to_string(),
            content_type
                .map(Into::into)
                .unwrap_or_else(|| Self::DEFAULT_CONTENT_TYPE.to_owned())
                .to_string()
                .into(),
        );

        Self(record)
    }

    pub fn from_json(record: serde_json::Map<String, serde_json::Value>) -> Self {
        Self(record)
    }

    pub fn into_json_string(self) -> String {
        serde_json::Value::Object(self.0).to_string()
    }

    pub fn version(&self) -> FileVersion {
        FileVersion::try_from(self.0["version"].as_i64().unwrap()).unwrap()
    }

    pub fn content_hash(&self) -> odf::Multihash {
        odf::Multihash::from_multibase(self.0["content_hash"].as_str().unwrap()).unwrap()
    }

    pub fn content_type(&self) -> MediaTypeRef {
        MediaTypeRef(self.0["content_type"].as_str().unwrap())
    }

    pub fn extra_data(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut record = self.0.clone();

        // Filter out system columns
        let vocab = odf::metadata::DatasetVocabulary::default();
        record.remove(&vocab.offset_column);
        record.remove(&vocab.operation_type_column);
        record.remove(&vocab.system_time_column);
        record.remove(&vocab.event_time_column);

        // Filter out core columns
        record.remove("version");
        record.remove("content_hash");
        record.remove("content_type");

        record
    }

    pub fn set_version(&mut self, version: FileVersion) {
        self.0["version"] = version.into();
    }

    pub fn set_extra_data(&mut self, extra_data: serde_json::Map<String, serde_json::Value>) {
        let record = Self::new(
            self.version(),
            self.content_hash(),
            Some(self.content_type().to_owned()),
            Some(extra_data),
        );
        self.0 = record.0;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VersionedFile {
    state: DatasetRequestStateWithOwner,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl VersionedFile {
    #[graphql(skip)]
    pub fn new(state: DatasetRequestStateWithOwner) -> Self {
        Self { state }
    }

    #[graphql(skip)]
    pub async fn get_record(
        &self,
        ctx: &Context<'_>,
        as_of_version: Option<FileVersion>,
        as_of_block_hash: Option<Multihash<'static>>,
    ) -> Result<(Option<FileVersionRecord>, odf::Multihash)> {
        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        let query_result = if let Some(block_hash) = as_of_block_hash {
            query_svc
                .tail(
                    &self.state.dataset_handle().as_local_ref(),
                    0,
                    1,
                    domain::GetDataOptions {
                        block_hash: Some(block_hash.into()),
                    },
                )
                .await
        } else if let Some(version) = as_of_version {
            use datafusion::logical_expr::{col, lit};

            query_svc
                .get_data(
                    &self.state.dataset_handle().as_local_ref(),
                    domain::GetDataOptions::default(),
                )
                .await
                .map(|res| domain::GetDataResponse {
                    df: res.df.filter(col("version").eq(lit(version))).unwrap(),
                    dataset_handle: res.dataset_handle,
                    block_hash: res.block_hash,
                })
        } else {
            query_svc
                .tail(
                    &self.state.dataset_handle().as_local_ref(),
                    0,
                    1,
                    domain::GetDataOptions::default(),
                )
                .await
        };

        let query_response = match query_result {
            Ok(res) => res,
            Err(domain::QueryError::DatasetSchemaNotAvailable(e)) => {
                return Ok((None, e.block_hash))
            }
            Err(e) => return Err(e.int_err().into()),
        };

        let records = query_response.df.collect_json_aos().await.int_err()?;

        assert_eq!(records.len(), 1);
        let serde_json::Value::Object(record) = records.into_iter().next().unwrap() else {
            unreachable!()
        };

        let record = FileVersionRecord::from_json(record);

        Ok((Some(record), query_response.block_hash))
    }

    /// Returns encoded content in-band. Can be used for very small files only.
    #[tracing::instrument(level = "info", name = VersionedFile_get_content, skip_all)]
    pub async fn get_content(
        &self,
        ctx: &Context<'_>,
        as_of_version: Option<FileVersion>,
        as_of_block_hash: Option<Multihash<'static>>,
    ) -> Result<GetFileContentResult> {
        // Get record
        let (record, block_hash) = self
            .get_record(ctx, as_of_version, as_of_block_hash)
            .await?;

        let Some(record) = record else {
            return Ok(GetFileContentResult::NotFound(GetFileContentErrorNotFound));
        };

        // Read payload
        // TODO: Restrict by content size
        let dataset = self.state.resolved_dataset(ctx).await?;
        let data_repo = dataset.as_data_repo();
        let content = data_repo
            .get_bytes(&record.content_hash())
            .await
            .int_err()?;

        Ok(GetFileContentResult::Success(GetFileContentSuccess {
            version: record.version(),
            block_hash: block_hash.into(),
            content_type: record.content_type().to_string(),
            content_hash: record.content_hash().into(),
            content: Base64Usnp(content),
            extra_data: serde_json::Value::Object(record.extra_data()),
        }))
    }

    /// Returns a direct download URL for use with large files
    #[tracing::instrument(level = "info", name = VersionedFile_get_content_url, skip_all)]
    pub async fn get_content_url(
        &self,
        ctx: &Context<'_>,
        as_of_version: Option<FileVersion>,
        as_of_block_hash: Option<Multihash<'static>>,
    ) -> Result<GetFileContentUrlResult> {
        // Get record
        let (record, block_hash) = self
            .get_record(ctx, as_of_version, as_of_block_hash)
            .await?;

        let Some(record) = record else {
            return Ok(GetFileContentUrlResult::NotFound(
                GetFileContentErrorNotFound,
            ));
        };

        // Get content url
        let dataset = self.state.resolved_dataset(ctx).await?;
        let data_repo = dataset.as_data_repo();
        let download = data_repo
            .get_external_download_url(
                &record.content_hash(),
                odf::storage::ExternalTransferOpts::default(),
            )
            .await
            .int_err()?;

        Ok(GetFileContentUrlResult::Success(GetFileContentUrlSuccess {
            version: record.version(),
            block_hash: block_hash.into(),
            content_type: record.content_type().to_string(),
            content_hash: record.content_hash().into(),
            extra_data: serde_json::Value::Object(record.extra_data()),
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
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "error_message", ty = "String")
)]
pub enum GetFileContentResult {
    Success(GetFileContentSuccess),
    NotFound(GetFileContentErrorNotFound),
    TooLarge(GetFileContentErrorTooLarge),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct GetFileContentSuccess {
    /// File version this result corresponds to
    pub version: FileVersion,

    /// Latest block that this result has considered
    pub block_hash: Multihash<'static>,

    /// Base64-encoded data (url-safe, no padding)
    pub content: Base64Usnp,

    /// Media type of the file content
    pub content_type: String,

    /// Multihash of the file content
    pub content_hash: Multihash<'static>,

    /// Extra data associated with this file version
    pub extra_data: serde_json::Value,
}
#[ComplexObject]
impl GetFileContentSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn error_message(&self) -> String {
        String::new()
    }
}

pub struct GetFileContentErrorNotFound;
#[Object]
impl GetFileContentErrorNotFound {
    async fn is_success(&self) -> bool {
        false
    }
    async fn error_message(&self) -> &str {
        "Specified version or block not found"
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct GetFileContentErrorTooLarge {
    in_band_content_size_limit: usize,
}
#[ComplexObject]
impl GetFileContentErrorTooLarge {
    async fn is_success(&self) -> bool {
        false
    }
    async fn error_message(&self) -> String {
        format!(
            "This file is too large to return in-band (>{}). Use getContentUrl instead",
            humansize::format_size(self.in_band_content_size_limit, humansize::BINARY)
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "error_message", ty = "String")
)]
pub enum GetFileContentUrlResult {
    Success(GetFileContentUrlSuccess),
    NotFound(GetFileContentErrorNotFound),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct GetFileContentUrlSuccess {
    /// File version this result corresponds to
    pub version: FileVersion,

    /// Block that this result corresponds to
    pub block_hash: Multihash<'static>,

    /// Media type of the file content
    pub content_type: String,

    /// Multihash of the file content
    pub content_hash: Multihash<'static>,

    /// Extra data associated with this file version
    pub extra_data: serde_json::Value,

    /// Direct download URL
    pub url: String,

    /// Headers to include in request
    pub headers: Vec<KeyValue>,

    /// Download URL expiration timestamp
    pub expires_at: Option<DateTime<Utc>>,
}
#[ComplexObject]
impl GetFileContentUrlSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn error_message(&self) -> String {
        String::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
