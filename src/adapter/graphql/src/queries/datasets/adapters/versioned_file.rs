// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain;
use odf::utils::data::format::{JsonArrayOfStructsWriter, RecordsWriter};

use crate::prelude::*;
use crate::queries::DatasetRequestStateWithOwner;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) type FileVersion = u32;

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
    ) -> Result<domain::GetDataResponse, domain::QueryError> {
        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        if let Some(block_hash) = as_of_block_hash {
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

            let res = query_svc
                .get_data(
                    &self.state.dataset_handle().as_local_ref(),
                    domain::GetDataOptions::default(),
                )
                .await?;

            let df = res.df.filter(col("version").eq(lit(version))).int_err()?;

            Ok(domain::GetDataResponse {
                df,
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
        }
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
        let res = match self.get_record(ctx, as_of_version, as_of_block_hash).await {
            Ok(res) => res,
            Err(domain::QueryError::DatasetSchemaNotAvailable(e)) => {
                return Ok(GetFileContentResult::NotFound(GetFileContentErrorNotFound))
            }
            Err(e) => return Err(e.int_err().into()),
        };

        let (mut record, block_hash) = {
            let record_batches = res.df.collect().await.int_err()?;

            let mut json = Vec::new();
            let mut writer = JsonArrayOfStructsWriter::new(&mut json);
            writer.write_batches(&record_batches).int_err()?;
            writer.finish().int_err()?;

            let serde_json::Value::Array(records) = serde_json::from_slice(&json).unwrap() else {
                unreachable!()
            };

            assert_eq!(records.len(), 1);
            let serde_json::Value::Object(record) = records.into_iter().next().unwrap() else {
                unreachable!()
            };

            (record, res.block_hash)
        };

        let vocab = odf::metadata::DatasetVocabulary::default();
        record.remove(&vocab.offset_column);
        record.remove(&vocab.operation_type_column);
        record.remove(&vocab.system_time_column);
        record.remove(&vocab.event_time_column);

        let version =
            FileVersion::try_from(record.remove("version").unwrap().as_i64().unwrap()).unwrap();
        let content_type = record
            .remove("content_type")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let content_hash = odf::Multihash::from_multibase(
            record.remove("content_hash").unwrap().as_str().unwrap(),
        )
        .int_err()?;

        // Read payload
        // TODO: Restrict by content size
        let dataset = self.state.resolved_dataset(ctx).await?;
        let data_repo = dataset.as_data_repo();
        let content = data_repo.get_bytes(&content_hash).await.int_err()?;

        Ok(GetFileContentResult::Success(GetFileContentSuccess {
            version,
            block_hash: block_hash.into(),
            content_type,
            content_hash: content_hash.into(),
            content: Base64Usnp(content),
            extra_data: serde_json::Value::Object(record),
        }))
    }

    /// Returns a direct download URL for use with large files
    #[tracing::instrument(level = "info", name = VersionedFile_get_content_url, skip_all)]
    pub async fn get_content_url(
        &self,
        ctx: &Context<'_>,
        as_of_version: Option<FileVersion>,
        as_of_block_hash: Option<Multihash<'static>>,
    ) -> GetFileContentUrlResult {
        todo!()
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

    /// Direct download URL
    pub content_url: String,

    /// Media type of the file content
    pub content_type: String,

    /// Extra data associated with this file version
    pub extra_data: serde_json::Value,
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
