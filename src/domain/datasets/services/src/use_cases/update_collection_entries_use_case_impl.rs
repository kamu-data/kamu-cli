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

use chrono::{DateTime, Utc};
use dill::{component, interface};
use file_utils::MediaType;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_auth_rebac::{RebacDatasetIdUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::{
    GetDataOptions,
    PushIngestDataError,
    PushIngestDataUseCase,
    PushIngestError,
    PushIngestPlanningError,
    QueryService,
    ResolvedDataset,
    auth,
};
use kamu_datasets::{
    CollectionEntryNotFound,
    CollectionEntryUpdate,
    CollectionUpdateOperation,
    ExtraDataFields,
    UpdateCollectionEntriesResult,
    UpdateCollectionEntriesSuccess,
    UpdateCollectionEntriesUseCase,
    UpdateCollectionEntriesUseCaseError,
};
use odf::metadata::OperationType as Op;
use tokio::time::{Duration, sleep};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn UpdateCollectionEntriesUseCase)]
pub struct UpdateCollectionEntriesUseCaseImpl {
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    push_ingest_data_use_case: Arc<dyn PushIngestDataUseCase>,
    query_svc: Arc<dyn QueryService>,
}

impl UpdateCollectionEntriesUseCaseImpl {
    async fn load_current_entries(
        &self,
        target_dataset: ResolvedDataset,
    ) -> Result<(BTreeMap<String, CollectionEntryState>, odf::Multihash), InternalError> {
        // TODO: PERF: Filter paths relevant to operations
        let query_res = self
            .query_svc
            .get_data(target_dataset, GetDataOptions::default())
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
                    .map(|record| {
                        CollectionEntryState::from_json(record)
                            .map(|entry| (entry.path.clone(), entry))
                    })
                    .collect::<Result<_, _>>()?
            }
        };

        Ok((entries, query_res.block_hash))
    }

    fn apply_operations(
        &self,
        mut current_entries: BTreeMap<String, CollectionEntryState>,
        operations: Vec<CollectionUpdateOperation>,
    ) -> Result<Vec<(Op, CollectionEntryState)>, CollectionEntryNotFound> {
        let mut diff = Vec::new();

        for op in operations {
            match op {
                CollectionUpdateOperation::Add(add) => {
                    let new_entry = CollectionEntryState::from_new_entry(add);

                    if let Some(existing) = current_entries.remove(&new_entry.path) {
                        if existing.is_equivalent_record(&new_entry) {
                            current_entries.insert(new_entry.path.clone(), existing);
                            continue;
                        }
                        diff.push((Op::Retract, existing));
                    }

                    current_entries.insert(new_entry.path.clone(), new_entry.clone());
                    diff.push((Op::Append, new_entry));
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

                    if old_entry.is_equivalent_record(&new_entry) {
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

    async fn run_retriable_ingest(
        &self,
        target: ResolvedDataset,
        ndjson_batches: Vec<bytes::Bytes>,
        mut expected_head: odf::Multihash,
        should_retry_on_cas_failed: bool,
    ) -> Result<
        std::result::Result<kamu_core::PushIngestResult, odf::dataset::RefCASError>,
        InternalError,
    > {
        const MAX_RETRIES: usize = 3;
        let mut retry_count = 0usize;

        loop {
            let data_sources: Vec<_> = ndjson_batches
                .iter()
                .cloned()
                .map(kamu_core::DataSource::Buffer)
                .collect();

            match self
                .push_ingest_data_use_case
                .execute_multi(
                    target.clone(),
                    data_sources,
                    kamu_core::PushIngestDataUseCaseOptions {
                        source_name: None,
                        source_event_time: None,
                        is_ingest_from_upload: false,
                        media_type: Some(MediaType::NDJSON.to_owned()),
                        expected_head: Some(expected_head.clone()),
                    },
                    None,
                )
                .await
            {
                Ok(res) => return Ok(Ok(res)),
                Err(PushIngestDataError::Planning(PushIngestPlanningError::HeadNotFound(e))) => {
                    return Ok(Err(odf::dataset::RefCASError {
                        reference: odf::BlockRef::Head,
                        expected: Some(e.hash),
                        actual: None,
                    }));
                }
                Err(PushIngestDataError::Execution(PushIngestError::CommitError(
                    odf::dataset::CommitError::MetadataAppendError(
                        odf::dataset::AppendError::RefCASFailed(e),
                    ),
                ))) => {
                    if should_retry_on_cas_failed && retry_count < MAX_RETRIES {
                        tracing::warn!(
                            "RefCASFailed encountered during collection entries update. \
                             Retrying... (attempt #{}), dataset: {}",
                            retry_count + 1,
                            target.get_alias(),
                        );
                        retry_count += 1;
                        expected_head = target
                            .as_metadata_chain()
                            .resolve_ref(&odf::BlockRef::Head)
                            .await
                            .int_err()?;
                        sleep(Duration::from_secs(retry_count as u64)).await;
                        continue;
                    }

                    return Ok(Err(e));
                }
                Err(err) => {
                    return Err(err.int_err());
                }
            }
        }
    }

    fn build_data_batches(
        &self,
        entries: Vec<(Op, CollectionEntryState)>,
    ) -> Result<Vec<bytes::Bytes>, UpdateCollectionEntriesUseCaseError> {
        use std::io::Write;

        entries
            .into_iter()
            .map(|(op, entry)| {
                let mut ndjson = Vec::<u8>::new();
                let mut record = entry.into_record_data();
                record["op"] = u8::from(op).into();
                writeln!(&mut ndjson, "{record}")
                    .int_err()
                    .map_err(UpdateCollectionEntriesUseCaseError::Internal)?;
                Ok(bytes::Bytes::from_owner(ndjson))
            })
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl UpdateCollectionEntriesUseCase for UpdateCollectionEntriesUseCaseImpl {
    #[tracing::instrument(level = "info", name = UpdateCollectionEntriesUseCaseImpl_execute, skip_all, fields(%dataset_handle.id))]
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        operations: Vec<CollectionUpdateOperation>,
        expected_head: Option<odf::Multihash>,
    ) -> Result<UpdateCollectionEntriesResult, UpdateCollectionEntriesUseCaseError> {
        if operations.is_empty() {
            return Ok(UpdateCollectionEntriesResult::UpToDate);
        }

        let target_dataset = self
            .rebac_dataset_registry_facade
            .resolve_dataset_by_handle(dataset_handle, auth::DatasetAction::Write)
            .await
            .map_err(|e| {
                use RebacDatasetIdUnresolvedError as E;
                match e {
                    E::Access(e) => UpdateCollectionEntriesUseCaseError::Access(e),
                    e @ E::Internal(_) => {
                        UpdateCollectionEntriesUseCaseError::Internal(e.int_err())
                    }
                }
            })?;

        let (current_entries, chain_head) =
            self.load_current_entries(target_dataset.clone()).await?;

        let diff = match self.apply_operations(current_entries, operations) {
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

        let should_retry_on_cas_failed = expected_head.is_none();
        let expected_head_value = expected_head.unwrap_or_else(|| chain_head.clone());

        let data_batches = self.build_data_batches(diff)?;

        let ingest_result = match self
            .run_retriable_ingest(
                target_dataset.clone(),
                data_batches,
                expected_head_value,
                should_retry_on_cas_failed,
            )
            .await?
        {
            Ok(res) => res,
            Err(err) => return Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(err)),
        };

        match ingest_result {
            kamu_core::PushIngestResult::Updated {
                old_head,
                new_head,
                num_blocks: _,
            } => Ok(UpdateCollectionEntriesResult::Success(
                UpdateCollectionEntriesSuccess { old_head, new_head },
            )),
            kamu_core::PushIngestResult::UpToDate => unreachable!(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
struct CollectionEntryState {
    path: String,
    reference: odf::DatasetID,
    extra_data: ExtraDataFields,
}

impl CollectionEntryState {
    fn from_new_entry(entry: CollectionEntryUpdate) -> Self {
        Self {
            path: entry.path,
            reference: entry.reference,
            extra_data: entry.extra_data,
        }
    }

    fn is_equivalent_record(&self, other: &Self) -> bool {
        self.path == other.path
            && self.reference == other.reference
            && self.extra_data == other.extra_data
    }

    fn into_record_data(self) -> serde_json::Value {
        serde_json::to_value(CollectionEntryRecord {
            path: self.path,
            reference: self.reference,
            extra_data: self.extra_data.into_inner(),
        })
        .unwrap()
    }

    fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let mut event: CollectionEntryEvent = serde_json::from_value(record).int_err()?;
        let vocab = odf::metadata::DatasetVocabulary::default();
        event.record.extra_data.remove(&vocab.offset_column);
        event.record.extra_data.remove(&vocab.operation_type_column);

        Ok(Self {
            path: event.record.path,
            reference: event.record.reference,
            extra_data: ExtraDataFields::new(event.record.extra_data),
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CollectionEntryRecord {
    pub path: String,
    #[serde(rename = "ref")]
    pub reference: odf::DatasetID,
    #[serde(flatten)]
    pub extra_data: serde_json::Map<String, serde_json::Value>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CollectionEntryEvent {
    #[serde(with = "odf::serde::yaml::datetime_rfc3339")]
    pub system_time: DateTime<Utc>,
    #[serde(with = "odf::serde::yaml::datetime_rfc3339")]
    pub event_time: DateTime<Utc>,
    #[serde(flatten)]
    pub record: CollectionEntryRecord,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
