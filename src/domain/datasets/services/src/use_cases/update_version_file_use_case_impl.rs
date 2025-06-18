// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use file_utils::MediaType;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::{
    DatasetRegistry,
    FileUploadLimitConfig,
    GetDataOptions,
    PushIngestDataError,
    PushIngestDataUseCase,
    PushIngestError,
    QueryService,
};
use kamu_datasets::{
    ContentArgs,
    ExtraDataFields,
    FileVersion,
    UpdateVersionFileResult,
    UpdateVersionFileUseCase,
    UpdateVersionFileUseCaseError,
    UploadTooLargeError,
    VersionedFileEntity,
};
use serde_json::Value;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn UpdateVersionFileUseCase)]
pub struct UpdateVersionFileUseCaseImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    upload_config: Arc<FileUploadLimitConfig>,
    push_ingest_data_use_case: Arc<dyn PushIngestDataUseCase>,
    query_svc: Arc<dyn QueryService>,
}

impl UpdateVersionFileUseCaseImpl {
    async fn get_latest_version(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<(FileVersion, odf::Multihash), InternalError> {
        // TODO: Consider retractions / corrections
        let query_res = self
            .query_svc
            .tail(dataset_ref, 0, 1, GetDataOptions::default())
            .await
            .int_err()?;

        let Some(df) = query_res.df else {
            return Ok((0, query_res.block_hash));
        };

        let last_version = df
            .select_columns(&["version"])
            .int_err()?
            .collect_scalar::<datafusion::arrow::datatypes::Int32Type>()
            .await
            .int_err()?;

        let last_version = FileVersion::try_from(last_version.unwrap_or(0)).unwrap();

        Ok((last_version, query_res.block_hash))
    }

    async fn get_content_args_from_latest_entry(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<ContentArgs>, InternalError> {
        // TODO: Consider retractions / corrections
        let query_res = self
            .query_svc
            .tail(dataset_ref, 0, 1, GetDataOptions::default())
            .await
            .int_err()?;

        let Some(df) = query_res.df else {
            return Ok(None);
        };

        let records = df.collect_json_aos().await.int_err()?;

        assert_eq!(records.len(), 1);
        let Value::Object(mut record) = records.into_iter().next().unwrap() else {
            unreachable!()
        };

        // TODO: Restrict after migration
        let content_length = usize::try_from(
            record
                .remove("content_length")
                .unwrap_or_default()
                .as_u64()
                .unwrap_or_default(),
        )
        .unwrap();
        let content_type =
            MediaType::from(record.remove("content_type").unwrap().as_str().unwrap());
        let content_hash = odf::Multihash::from_multibase(
            record.remove("content_hash").unwrap().as_str().unwrap(),
        )
        .unwrap();

        Ok(Some(ContentArgs {
            content_length,
            content_stream: None,
            content_hash,
            content_type: Some(content_type),
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl UpdateVersionFileUseCase for UpdateVersionFileUseCaseImpl {
    #[tracing::instrument(level = "info", name = UpdateVersionFileUseCaseImpl_execute, skip_all, fields(%dataset_handle.id))]
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        content_args_maybe: Option<ContentArgs>,
        expected_head: Option<odf::Multihash>,
        extra_data: Option<ExtraDataFields>,
    ) -> Result<UpdateVersionFileResult, UpdateVersionFileUseCaseError> {
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_handle(dataset_handle)
            .await
            .clone();

        let content_args = if let Some(args) = content_args_maybe {
            args
        } else {
            self.get_content_args_from_latest_entry(&dataset_handle.as_local_ref())
                .await?
                .unwrap()
        };

        if content_args.content_length > self.upload_config.max_file_size_in_bytes() {
            return Err(UpdateVersionFileUseCaseError::TooLarge(
                UploadTooLargeError::new(
                    content_args.content_length,
                    self.upload_config.max_file_size_in_bytes(),
                ),
            ));
        }

        // Upload data object
        if let Some(content_stream) = content_args.content_stream {
            let data_repo = resolved_dataset.as_data_repo();
            data_repo
                .insert_stream(
                    content_stream,
                    odf::storage::InsertOpts {
                        precomputed_hash: Some(&content_args.content_hash),
                        size_hint: Some(content_args.content_length as u64),
                        ..Default::default()
                    },
                )
                .await
                .int_err()?;
        }

        let (latest_version, _) = self
            .get_latest_version(&resolved_dataset.get_handle().as_local_ref())
            .await?;
        let new_version = latest_version + 1;

        let entity = VersionedFileEntity::new(
            new_version,
            content_args.content_hash.clone(),
            content_args.content_length,
            content_args.content_type,
            extra_data,
        );

        let ingest_result = self
            .push_ingest_data_use_case
            .execute(
                &resolved_dataset,
                kamu_core::DataSource::Buffer(entity.to_bytes()),
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: Some(MediaType::NDJSON.to_owned()),
                    expected_head,
                },
                None,
            )
            .await
            .map_err(|err| match err {
                PushIngestDataError::Execution(PushIngestError::CommitError(
                    odf::dataset::CommitError::MetadataAppendError(
                        odf::dataset::AppendError::RefCASFailed(e),
                    ),
                )) => UpdateVersionFileUseCaseError::RefCASFailed(e),
                err => UpdateVersionFileUseCaseError::Internal(err.int_err()),
            })?;

        match ingest_result {
            kamu_core::PushIngestResult::Updated {
                old_head,
                new_head,
                num_blocks: _,
            } => Ok(UpdateVersionFileResult {
                new_version,
                old_head,
                new_head,
                content_hash: content_args.content_hash,
            }),
            kamu_core::PushIngestResult::UpToDate => unreachable!(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
