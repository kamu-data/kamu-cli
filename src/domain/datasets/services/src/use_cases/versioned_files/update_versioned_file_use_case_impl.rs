// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use file_utils::MediaType;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::{
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
    ResolvedDataset,
    UpdateVersionFileResult,
    UpdateVersionFileUseCaseError,
    UpdateVersionedFileUseCase,
    VERSION_COLUMN_NAME,
    VersionedFileEntry,
    WriteCheckedDataset,
};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn UpdateVersionedFileUseCase)]
pub struct UpdateVersionedFileUseCaseImpl {
    push_ingest_data_use_case: Arc<dyn PushIngestDataUseCase>,
    query_svc: Arc<dyn QueryService>,
    system_time_source: Arc<dyn SystemTimeSource>,
}

impl UpdateVersionedFileUseCaseImpl {
    async fn get_latest_version(
        &self,
        file_dataset: ResolvedDataset,
    ) -> Result<(FileVersion, odf::Multihash), InternalError> {
        // TODO: Consider retractions / corrections
        let query_res = self
            .query_svc
            .tail(file_dataset, 0, 1, GetDataOptions::default())
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
        file_dataset: ResolvedDataset,
    ) -> Result<Option<VersionedFileEntry>, InternalError> {
        // TODO: Consider retractions / corrections
        let query_res = self
            .query_svc
            .tail(file_dataset, 0, 1, GetDataOptions::default())
            .await
            .int_err()?;

        let Some(df) = query_res.df else {
            return Ok(None);
        };

        let records = df.collect_json_aos().await.int_err()?;

        assert_eq!(records.len(), 1);

        Ok(Some(VersionedFileEntry::from_json(
            records.into_iter().next().unwrap(),
        )?))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl UpdateVersionedFileUseCase for UpdateVersionedFileUseCaseImpl {
    #[tracing::instrument(level = "info", name = UpdateVersionedFileUseCaseImpl_execute, skip_all, fields(id = %file_dataset.get_id()))]
    async fn execute(
        &self,
        file_dataset: WriteCheckedDataset<'_>,
        source_event_time: Option<DateTime<Utc>>,
        content_args_maybe: Option<ContentArgs>,
        expected_head: Option<odf::Multihash>,
        extra_data: Option<ExtraDataFields>,
    ) -> Result<UpdateVersionFileResult, UpdateVersionFileUseCaseError> {
        let entity = if let Some(args) = content_args_maybe {
            let (latest_version, _) = self.get_latest_version((*file_dataset).clone()).await?;
            let new_version = latest_version + 1;

            let now = self.system_time_source.now();

            let result = VersionedFileEntry::new(
                now,
                source_event_time.unwrap_or(now),
                new_version,
                args.content_hash.clone(),
                args.content_length,
                args.content_type,
                extra_data,
            );

            // Upload data object in case when content is present
            if let Some(content_stream) = args.content_stream {
                let data_repo = file_dataset.as_data_repo();
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
            }

            result
        } else {
            let mut last_entity = self
                .get_versioned_file_entity_from_latest_entry((*file_dataset).clone())
                .await?
                .unwrap();

            // Increment version to match next record value
            last_entity.version += 1;
            last_entity.extra_data = extra_data.unwrap_or_default();

            last_entity
        };

        let content_hash = entity.content_hash.clone();
        let version = entity.version;

        let ingest_result = self
            .push_ingest_data_use_case
            .execute(
                file_dataset.into_inner(),
                kamu_core::DataSource::Buffer(entity.to_bytes()),
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time,
                    is_ingest_from_upload: false,
                    media_type: Some(MediaType::NDJSON.to_owned()),
                    expected_head,
                    skip_quota_check: false,
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
                PushIngestDataError::Execution(PushIngestError::QuotaExceeded(e)) => {
                    UpdateVersionFileUseCaseError::QuotaExceeded(e)
                }
                err => UpdateVersionFileUseCaseError::Internal(err.int_err()),
            })?;

        match ingest_result {
            kamu_core::PushIngestResult::Updated {
                old_head,
                new_head,
                num_blocks: _,
                system_time,
            } => Ok(UpdateVersionFileResult {
                new_version: version,
                old_head,
                new_head,
                content_hash,
                system_time,
            }),
            kamu_core::PushIngestResult::UpToDate => unreachable!(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
