// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use file_utils::MediaType;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::{
    GetDataOptions,
    PushIngestDataError,
    PushIngestDataUseCase,
    PushIngestError,
    PushIngestPlanningError,
    QueryService,
};
use kamu_datasets::{
    CollectionEntry,
    CollectionEntryNotFound,
    CollectionEntryRecord,
    CollectionPath,
    CollectionUpdateOperation,
    UpdateCollectionEntriesResult,
    UpdateCollectionEntriesSuccess,
    UpdateCollectionEntriesUseCase,
    UpdateCollectionEntriesUseCaseError,
    WriteCheckedDataset,
};
use odf::metadata::OperationType as Op;
use tokio::time::{Duration, sleep};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn UpdateCollectionEntriesUseCase)]
pub struct UpdateCollectionEntriesUseCaseImpl {
    push_ingest_data_use_case: Arc<dyn PushIngestDataUseCase>,
    query_svc: Arc<dyn QueryService>,
}

impl UpdateCollectionEntriesUseCaseImpl {
    async fn load_current_entries(
        &self,
        collection_dataset: &WriteCheckedDataset<'_>,
    ) -> Result<
        (
            BTreeMap<CollectionPath, CollectionEntryRecord>,
            odf::Multihash,
        ),
        InternalError,
    > {
        // TODO: PERF: Filter paths relevant to operations
        let query_res = self
            .query_svc
            .get_data((*collection_dataset).clone(), GetDataOptions::default())
            .await
            .int_err()?;

        let entries: BTreeMap<_, _> = match query_res.df {
            None => Default::default(),
            Some(df) => {
                let df = odf::utils::data::changelog::project(
                    df,
                    &["path".to_string()],
                    &odf::metadata::DatasetVocabulary::default(),
                )
                .int_err()?;

                df.collect_json_aos()
                    .await
                    .int_err()?
                    .into_iter()
                    .map(|event_json| {
                        let entry = CollectionEntry::from_json(event_json).int_err()?;
                        let record: CollectionEntryRecord = entry.into();
                        Ok((record.path.clone(), record))
                    })
                    .collect::<Result<_, _>>()?
            }
        };

        Ok((entries, query_res.block_hash))
    }

    fn apply_operations(
        &self,
        mut current_entries: BTreeMap<CollectionPath, CollectionEntryRecord>,
        operations: Vec<CollectionUpdateOperation>,
    ) -> Result<Vec<(Op, CollectionEntryRecord)>, CollectionEntryNotFound> {
        let mut diff = Vec::new();

        for op in operations {
            match op {
                CollectionUpdateOperation::Add(add) => {
                    let new_record = add.record;

                    if let Some(existing) = current_entries.remove(&new_record.path) {
                        if existing == new_record {
                            current_entries.insert(new_record.path.clone(), existing);
                            continue;
                        }
                        diff.push((Op::Retract, existing));
                    }

                    current_entries.insert(new_record.path.clone(), new_record.clone());
                    diff.push((Op::Append, new_record));
                }
                CollectionUpdateOperation::Remove(remove) => {
                    if let Some(existing) = current_entries.remove(&remove.path) {
                        diff.push((Op::Retract, existing));
                    }
                }
                CollectionUpdateOperation::Move(mov) => {
                    let Some(old_entry) = current_entries.remove(&mov.path_from) else {
                        return Err(CollectionEntryNotFound {
                            path: mov.path_from,
                        });
                    };

                    let mut new_entry = old_entry.clone();
                    new_entry.path = mov.path_to;

                    if let Some(extra_data) = mov.extra_data {
                        new_entry.extra_data = extra_data;
                    }

                    if old_entry == new_entry {
                        current_entries.insert(new_entry.path.clone(), old_entry);
                        continue;
                    }

                    if new_entry.path != old_entry.path {
                        if let Some(collision) = current_entries.remove(&new_entry.path) {
                            diff.push((Op::Retract, collision));
                        }

                        diff.push((Op::Retract, old_entry));
                        diff.push((Op::Append, new_entry.clone()));
                    } else {
                        diff.push((Op::CorrectFrom, old_entry));
                        diff.push((Op::CorrectTo, new_entry.clone()));
                    }

                    current_entries.insert(new_entry.path.clone(), new_entry);
                }
            }
        }

        Ok(diff)
    }

    fn build_data_batches(
        &self,
        entries: Vec<(Op, CollectionEntryRecord)>,
    ) -> Result<Vec<bytes::Bytes>, UpdateCollectionEntriesUseCaseError> {
        // TODO: PERF: FIXME: Writing each operation in a different block to work around
        //       changelog sorting issue.
        //       See: https://github.com/kamu-data/kamu-cli/issues/1228
        use std::io::Write;

        entries
            .into_iter()
            .map(|(op, entry)| {
                let mut ndjson = Vec::<u8>::new();
                let mut record = serde_json::to_value(entry).int_err()?;
                record["op"] = u8::from(op).into();
                writeln!(&mut ndjson, "{record}").int_err()?;
                Ok(bytes::Bytes::from_owner(ndjson))
            })
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl UpdateCollectionEntriesUseCase for UpdateCollectionEntriesUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = UpdateCollectionEntriesUseCaseImpl_execute,
        skip_all,
        fields(id = %collection_dataset.get_id())
    )]
    async fn execute(
        &self,
        collection_dataset: WriteCheckedDataset<'_>,
        operations: Vec<CollectionUpdateOperation>,
        expected_head: Option<odf::Multihash>,
    ) -> Result<UpdateCollectionEntriesResult, UpdateCollectionEntriesUseCaseError> {
        if operations.is_empty() {
            return Ok(UpdateCollectionEntriesResult::UpToDate);
        }
        const MAX_RETRIES: usize = 3;
        let mut retry_count = 0usize;

        loop {
            let (current_entries, chain_head) =
                self.load_current_entries(&collection_dataset).await?;

            let diff = match self.apply_operations(current_entries, operations.clone()) {
                Ok(diff) => diff,
                Err(not_found) => {
                    return Ok(UpdateCollectionEntriesResult::NotFound(not_found));
                }
            };

            if diff.is_empty() {
                return Ok(UpdateCollectionEntriesResult::UpToDate);
            }

            for (operation, state) in &diff {
                tracing::debug!(
                    ?operation,
                    path = %state.path,
                    reference = %state.reference,
                    extra = ?state.extra_data,
                    "Preparing collection diff operation"
                );
            }

            let expected_head_value = expected_head.clone().unwrap_or_else(|| chain_head.clone());

            let data_sources: Vec<_> = self
                .build_data_batches(diff.clone())?
                .into_iter()
                .map(kamu_core::DataSource::Buffer)
                .collect();

            match self
                .push_ingest_data_use_case
                .execute_multi(
                    collection_dataset.clone(),
                    data_sources,
                    kamu_core::PushIngestDataUseCaseOptions {
                        source_name: None,
                        source_event_time: None,
                        is_ingest_from_upload: false,
                        media_type: Some(MediaType::NDJSON.to_owned()),
                        expected_head: Some(expected_head_value),
                    },
                    None,
                )
                .await
            {
                Ok(kamu_core::PushIngestResult::Updated {
                    old_head,
                    new_head,
                    num_blocks: _,
                }) => {
                    return Ok(UpdateCollectionEntriesResult::Success(
                        UpdateCollectionEntriesSuccess {
                            old_head,
                            new_head,
                            inserted_records: diff,
                        },
                    ));
                }
                Ok(kamu_core::PushIngestResult::UpToDate) => unreachable!(),
                Err(PushIngestDataError::Planning(PushIngestPlanningError::HeadNotFound(e))) => {
                    return Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(
                        odf::dataset::RefCASError {
                            reference: odf::BlockRef::Head,
                            expected: Some(e.hash),
                            actual: None,
                        },
                    ));
                }
                Err(PushIngestDataError::Execution(PushIngestError::CommitError(
                    odf::dataset::CommitError::MetadataAppendError(
                        odf::dataset::AppendError::RefCASFailed(e),
                    ),
                ))) => {
                    // We run retry only if expected_head was not specified by the caller
                    // to cover case of concurrent updates
                    if expected_head.is_none() && retry_count < MAX_RETRIES {
                        tracing::warn!(
                            "RefCASFailed encountered during collection entries update. \
                             Retrying... (attempt #{}), dataset: {}",
                            retry_count + 1,
                            collection_dataset.get_alias(),
                        );
                        retry_count += 1;
                        sleep(Duration::from_secs(retry_count as u64)).await;
                        continue;
                    }

                    return Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(e));
                }
                Err(PushIngestDataError::Execution(PushIngestError::QuotaExceeded(e))) => {
                    return Err(UpdateCollectionEntriesUseCaseError::QuotaExceeded(e));
                }
                Err(err) => {
                    return Err(UpdateCollectionEntriesUseCaseError::Internal(err.int_err()));
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
