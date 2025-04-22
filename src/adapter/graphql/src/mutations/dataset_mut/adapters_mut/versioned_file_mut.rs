// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain;

use crate::prelude::*;
use crate::queries::{DatasetRequestState, FileVersion};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VersionedFileMut {
    state: DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl VersionedFileMut {
    const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

    #[graphql(skip)]
    pub fn new(state: DatasetRequestState) -> Self {
        Self { state }
    }

    /// Uploads new version of content in-band. Can be used for very small files
    /// only.
    #[tracing::instrument(level = "info", name = VersionedFileMut_upload_new_version, skip_all)]
    pub async fn upload_new_version(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Base64-encoded file content (url-safe, no padding)")] content: Base64Usnp,
        #[graphql(desc = "Media type of content (e.g. application/pdf)")] content_type: Option<
            String,
        >,
        #[graphql(desc = "Json object containing values of extra columns")] extra_data: Option<
            serde_json::Value,
        >,
    ) -> Result<UpdateVersionResult> {
        let (query_svc, push_ingest_use_case) = from_catalog_n!(
            ctx,
            dyn domain::QueryService,
            dyn domain::PushIngestDataUseCase
        );

        // Get latest version and head
        let (new_version, head) = match query_svc
            .tail(&self.state.dataset_handle().as_local_ref(), 0, 1)
            .await
        {
            Ok(res) => {
                let last_version = res
                    .df
                    .select_columns(&["version"])
                    .int_err()?
                    .collect_scalar::<datafusion::arrow::datatypes::Int32Type>()
                    .await
                    .int_err()?;

                let last_version = FileVersion::try_from(last_version.unwrap_or(0)).unwrap();

                (last_version + 1, res.block_hash)
            }
            Err(kamu_core::QueryError::DatasetSchemaNotAvailable(e)) => (1, e.block_hash),
            Err(e) => return Err(e.int_err().into()),
        };

        let content = content.into_inner();
        let content_hash = odf::Multihash::from_digest_sha3_256(&content);
        let content_type = content_type
            .as_deref()
            .unwrap_or(Self::DEFAULT_CONTENT_TYPE);

        // For a new record
        let record = serde_json::json!({
            "version": new_version,
            "content_hash": content_hash.to_string(),
            "content_type": content_type,
        })
        .to_string();

        // Push ingest the new record
        // TODO: Handle errors on invalid extra data columns
        let ingest_result = push_ingest_use_case
            .execute(
                self.state.resolved_dataset(ctx).await?,
                kamu_core::DataSource::Stream(Box::new(std::io::Cursor::new(record))),
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: Some(kamu_core::MediaType::NDJSON.to_owned()),
                },
                None,
            )
            .await
            .int_err()?;

        match ingest_result {
            kamu_core::PushIngestResult::Updated {
                old_head,
                new_head,
                num_blocks: _,
            } => Ok(UpdateVersionResult::Success(UpdateVersionSuccess {
                new_version,
                old_head: old_head.into(),
                new_head: new_head.into(),
                content_hash: content_hash.into(),
            })),
            kamu_core::PushIngestResult::UpToDate => unreachable!(),
        }
    }

    /// Returns a pre-signed URL and upload token for direct uploads of large
    /// files
    #[tracing::instrument(level = "info", name = VersionedFileMut_start_upload_new_version, skip_all)]
    pub async fn start_upload_new_version(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Size of the file being uploaded")] content_size: usize,
        #[graphql(desc = "Media type of content (e.g. application/pdf)")] content_type: Option<
            String,
        >,
    ) -> StartUploadVersionResult {
        todo!()
    }

    /// Finalizes the content upload by incoporating the content into the
    /// dataset as a new version
    #[tracing::instrument(level = "info", name = VersionedFileMut_finish_upload_new_version, skip_all)]
    pub async fn finish_upload_new_version(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Token received when starting the upload")] upload_token: String,
        #[graphql(desc = "Json object containing values of extra columns")] extra_data: Option<
            serde_json::Value,
        >,
    ) -> UpdateVersionResult {
        todo!()
    }

    /// Creating a new version with that has updated values of extra columns but
    /// with the file content unchanged
    #[tracing::instrument(level = "info", name = VersionedFileMut_update_extra_data, skip_all)]
    pub async fn update_extra_data(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Json object containing values of extra columns")]
        extra_data: serde_json::Value,
    ) -> UpdateVersionResult {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "error_message", ty = "String")
)]
pub enum UpdateVersionResult {
    Success(UpdateVersionSuccess),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct UpdateVersionSuccess {
    pub new_version: FileVersion,
    pub old_head: Multihash<'static>,
    pub new_head: Multihash<'static>,
    pub content_hash: Multihash<'static>,
}
#[ComplexObject]
impl UpdateVersionSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn error_message(&self) -> String {
        String::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "error_message", ty = "String")
)]
pub enum StartUploadVersionResult {
    Success(StartUploadVersionSuccess),
    TooLarge(StartUploadVersionErrorTooLarge),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct StartUploadVersionSuccess {
    upload_context: UploadContext,
}
#[ComplexObject]
impl StartUploadVersionSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn error_message(&self) -> String {
        String::new()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct StartUploadVersionErrorTooLarge {
    upload_size: usize,
    upload_limit: usize,
}
#[ComplexObject]
impl StartUploadVersionErrorTooLarge {
    async fn is_success(&self) -> bool {
        false
    }
    async fn error_message(&self) -> String {
        format!(
            "Upload of {} exceeds the {} limit",
            humansize::format_size(self.upload_size, humansize::BINARY),
            humansize::format_size(self.upload_limit, humansize::BINARY)
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, SimpleObject)]
pub struct UploadContext {
    pub upload_url: String,
    pub method: String,
    // pub use_multipart: bool,
    pub headers: Vec<KeyValue>,
    // pub fields: Vec<KeyValue>,
    pub upload_token: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
