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
    ResolvedDataset,
    UpdateVersionFileResult,
    UpdateVersionFileUseCaseError,
    UpdateVersionedFileUseCase,
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
    /// Returns the latest version and the head it was read at.
    ///
    /// Versioned files use the `Append` merge strategy and their read step has no
    /// operation type column, so the last record is always the latest version.
    async fn get_latest_entry(
        &self,
        file_dataset: ResolvedDataset,
    ) -> Result<(Option<VersionedFileEntry>, odf::Multihash), InternalError> {
        let query_res = self
            .query_svc
            .tail(file_dataset, 0, 1, GetDataOptions::default())
            .await
            .int_err()?;

        let Some(df) = query_res.df else {
            return Ok((None, query_res.block_hash));
        };

        let records = df.collect_json_aos().await.int_err()?;
        let entry = records
            .into_iter()
            .next()
            .map(VersionedFileEntry::from_json)
            .transpose()?;

        Ok((entry, query_res.block_hash))
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
        // Upload data object in case when content is present
        let content = if let Some(args) = content_args_maybe {
            if let Some(content_stream) = args.content_stream {
                let data_repo = file_dataset.as_data_repo();
                data_repo
                    .insert_stream(
                        content_stream,
                        odf::storage::InsertOpts {
                            precomputed_hash: Some(&args.content_hash),
                            size_hint: Some(u64::try_from(args.content_length).unwrap()),
                            ..Default::default()
                        },
                    )
                    .await
                    .int_err()?;
            }
            Some((args.content_hash, args.content_length, args.content_type))
        } else {
            None
        };

        let (latest_entry, latest_head) = self.get_latest_entry((*file_dataset).clone()).await?;

        let entity = if let Some((content_hash, content_length, content_type)) = content {
            let now = self.system_time_source.now();

            VersionedFileEntry::new(
                now,
                source_event_time.unwrap_or(now),
                latest_entry.map_or(0, |entry| entry.version) + 1,
                content_hash,
                content_length,
                content_type,
                extra_data,
            )
        } else {
            let Some(mut last_entity) = latest_entry else {
                return Err(UpdateVersionFileUseCaseError::NoVersions);
            };

            // Increment version to match next record value
            last_entity.version += 1;
            last_entity.extra_data = extra_data.unwrap_or_default();

            last_entity
        };

        let content_hash = entity.content_hash.clone();
        let version = entity.version;

        // The version is derived from the latest entry, so the new one may only be
        // committed on top of the head that entry was read at
        let ingest_result = self
            .push_ingest_data_use_case
            .execute(
                (*file_dataset).clone(),
                kamu_core::DataSource::Buffer(entity.to_bytes()),
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time,
                    is_ingest_from_upload: false,
                    media_type: Some(MediaType::NDJSON.to_owned()),
                    expected_head: Some(expected_head.unwrap_or(latest_head)),
                    skip_quota_check: false,
                },
                None,
            )
            .await;

        // A concurrent update results in an error rather than in a duplicate version
        let ingest_result = match ingest_result {
            Ok(res) => res,
            Err(PushIngestDataError::Execution(PushIngestError::CommitError(
                odf::dataset::CommitError::MetadataAppendError(
                    odf::dataset::AppendError::RefCASFailed(e),
                ),
            ))) => {
                return Err(UpdateVersionFileUseCaseError::RefCASFailed(e));
            }
            Err(PushIngestDataError::Execution(PushIngestError::QuotaExceeded(e))) => {
                return Err(UpdateVersionFileUseCaseError::QuotaExceeded(e));
            }
            Err(
                err @ (PushIngestDataError::Planning(_)
                | PushIngestDataError::Execution(_)
                | PushIngestDataError::NotFound(_)
                | PushIngestDataError::Access(_)
                | PushIngestDataError::Internal(_)),
            ) => {
                return Err(UpdateVersionFileUseCaseError::Internal(err.int_err()));
            }
        };

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
