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
    VERSION_COLUMN_NAME,
    VersionedFileEntity,
};

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
            .select_columns(&[VERSION_COLUMN_NAME])
            .int_err()?
            .collect_scalar::<datafusion::arrow::datatypes::Int32Type>()
            .await
            .int_err()?;

        let last_version = FileVersion::try_from(last_version.unwrap_or(0)).unwrap();

        Ok((last_version, query_res.block_hash))
    }

    async fn get_versioned_file_entity_from_latest_entry(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<VersionedFileEntity>, InternalError> {
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

        Ok(Some(VersionedFileEntity::from_last_record(
            records.into_iter().next().unwrap(),
        )))
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

        let entity = if let Some(args) = content_args_maybe {
            let (latest_version, _) = self
                .get_latest_version(&resolved_dataset.get_handle().as_local_ref())
                .await?;
            let new_version = latest_version + 1;

            let result = VersionedFileEntity::new(
                new_version,
                args.content_hash.clone(),
                args.content_length,
                args.content_type,
                extra_data,
            );

            // Upload data object in case when content is present
            if let Some(content_stream) = args.content_stream {
                let data_repo = resolved_dataset.as_data_repo();
                data_repo
                    .insert_stream(
                        content_stream,
                        odf::storage::InsertOpts {
                            precomputed_hash: Some(&result.content_hash),
                            size_hint: Some(result.content_length as u64),
                            ..Default::default()
                        },
                    )
                    .await
                    .int_err()?;
            };

            result
        } else {
            let mut last_entity = self
                .get_versioned_file_entity_from_latest_entry(&dataset_handle.as_local_ref())
                .await?
                .unwrap();

            // Increment version to match next record value
            last_entity.version = last_entity.version + 1;

            last_entity
        };

        if entity.content_length > self.upload_config.max_file_size_in_bytes() {
            return Err(UpdateVersionFileUseCaseError::TooLarge(
                UploadTooLargeError::new(
                    entity.content_length,
                    self.upload_config.max_file_size_in_bytes(),
                ),
            ));
        }

        let content_hash = entity.content_hash.clone();
        let version = entity.version;

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
                new_version: version,
                old_head,
                new_head,
                content_hash,
            }),
            kamu_core::PushIngestResult::UpToDate => unreachable!(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
