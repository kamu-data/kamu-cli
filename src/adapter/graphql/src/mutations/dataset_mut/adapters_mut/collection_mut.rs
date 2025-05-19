// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain;
use odf::metadata::OperationType as Op;

use crate::prelude::*;
use crate::queries::CollectionEntry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CollectionMut<'a> {
    dataset: &'a domain::ResolvedDataset,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'a> CollectionMut<'a> {
    pub fn new(dataset: &'a domain::ResolvedDataset) -> Self {
        Self { dataset }
    }

    // Push ingest the new record
    // TODO: Compare and swap current head
    // TODO: Handle errors on invalid extra data columns
    async fn write_records(
        &self,
        ctx: &Context<'_>,
        entries: Vec<(Op, CollectionEntry)>,
        expected_head: Option<odf::Multihash>,
    ) -> Result<CollectionUpdateResult> {
        use std::io::Write;

        let push_ingest_use_case = from_catalog_n!(ctx, dyn domain::PushIngestDataUseCase);

        let mut ndjson = Vec::<u8>::new();
        for (op, entry) in entries {
            let mut record = entry.to_record_data();
            record["op"] = u8::from(op).into();
            writeln!(&mut ndjson, "{record}").int_err()?;
        }

        let ingest_result = match push_ingest_use_case
            .execute(
                self.dataset,
                kamu_core::DataSource::Buffer(bytes::Bytes::from_owner(ndjson)),
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: Some(kamu_core::MediaType::NDJSON.to_owned()),
                    expected_head,
                },
                None,
            )
            .await
        {
            Ok(res) => res,
            Err(domain::PushIngestDataError::Execution(domain::PushIngestError::CommitError(
                odf::dataset::CommitError::MetadataAppendError(
                    odf::dataset::AppendError::RefCASFailed(e),
                ),
            ))) => {
                return Ok(CollectionUpdateResult::CasFailed(
                    CollectionUpdateErrorCasFailed {
                        expected_head: e.expected.unwrap().into(),
                        actual_head: e.actual.unwrap().into(),
                    },
                ))
            }
            Err(err) => {
                return Err(err.int_err().into());
            }
        };

        match ingest_result {
            kamu_core::PushIngestResult::Updated {
                old_head,
                new_head,
                num_blocks: _,
            } => Ok(CollectionUpdateResult::Success(CollectionUpdateSuccess {
                old_head: old_head.into(),
                new_head: new_head.into(),
            })),
            kamu_core::PushIngestResult::UpToDate => unreachable!(),
        }
    }

    pub async fn update_entries_impl(
        &self,
        ctx: &Context<'_>,
        operations: Vec<CollectionUpdateInput>,
        expected_head: Option<odf::Multihash>,
    ) -> Result<CollectionUpdateResult> {
        if operations.is_empty() {
            return Ok(CollectionUpdateResult::UpToDate(CollectionUpdateUpToDate));
        }

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        // Load current state
        // TODO: PERF: Filter paths that are relevant to operations
        let query_res = query_svc
            .get_data(
                &self.dataset.get_handle().as_local_ref(),
                domain::GetDataOptions::default(),
            )
            .await
            .int_err()?;

        let expected_head = expected_head.unwrap_or(query_res.block_hash);

        let mut current_entries: std::collections::BTreeMap<_, _> = match query_res.df {
            None => Default::default(),
            Some(df) => {
                // Project changelog into state
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
                    .map(|record| CollectionEntry::from_json(self.dataset.clone(), record))
                    .map(|entry| (entry.path.clone(), entry))
                    .collect()
            }
        };

        let mut diff = Vec::new();

        for op in operations {
            if let Some(add) = op.add {
                if let Some(existing) = current_entries.remove(&add.entry.path) {
                    diff.push((Op::Retract, existing));
                }

                let new_entry = CollectionEntry::from_input(self.dataset.clone(), add.entry);
                current_entries.insert(new_entry.path.clone(), new_entry.clone());
                diff.push((Op::Append, new_entry));
            } else if let Some(remove) = op.remove {
                if let Some(existing) = current_entries.remove(&remove.path) {
                    diff.push((Op::Retract, existing));
                }
            } else if let Some(mov) = op.r#move {
                let Some(old_entry) = current_entries.remove(&mov.path_from) else {
                    return Ok(CollectionUpdateResult::NotFound(
                        CollectionUpdateErrorNotFound {
                            path: mov.path_from,
                        },
                    ));
                };

                let mut new_entry = old_entry.clone();
                new_entry.path = mov.path_to;
                if let Some(extra_data) = mov.extra_data {
                    new_entry.extra_data = extra_data;
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

        if diff.is_empty() {
            return Ok(CollectionUpdateResult::UpToDate(CollectionUpdateUpToDate));
        }

        let mut expected_head: odf::Multihash = expected_head;
        let mut res = None;

        // TODO: PERF: FIXME: Writing each operation in a different block to work around
        // changelog sorting issue.
        // See: https://github.com/kamu-data/kamu-cli/issues/1228
        for op in diff {
            match self
                .write_records(ctx, vec![op], Some(expected_head.clone()))
                .await?
            {
                CollectionUpdateResult::Success(r) => {
                    expected_head = r.new_head.clone().into();
                    res = Some(CollectionUpdateResult::Success(r));
                }
                r => {
                    return Ok(r);
                }
            }
        }

        Ok(res.unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl CollectionMut<'_> {
    /// Links new entry to this collection
    #[tracing::instrument(level = "info", name = CollectionMut_add_entry, skip_all)]
    pub async fn add_entry(
        &self,
        ctx: &Context<'_>,
        entry: CollectionEntryInput,
        expected_head: Option<Multihash<'static>>,
    ) -> Result<CollectionUpdateResult> {
        self.update_entries_impl(
            ctx,
            vec![CollectionUpdateInput {
                add: Some(CollectionUpdateInputAdd { entry }),
                ..Default::default()
            }],
            expected_head.map(Into::into),
        )
        .await
    }

    /// Moves or renames an entry
    #[tracing::instrument(level = "info", name = CollectionMut_move_entry, skip_all)]
    pub async fn move_entry(
        &self,
        ctx: &Context<'_>,
        path_from: CollectionPath,
        path_to: CollectionPath,
        extra_data: Option<serde_json::Value>,
        expected_head: Option<Multihash<'static>>,
    ) -> Result<CollectionUpdateResult> {
        self.update_entries_impl(
            ctx,
            vec![CollectionUpdateInput {
                r#move: Some(CollectionUpdateInputMove {
                    path_from,
                    path_to,
                    extra_data,
                }),
                ..Default::default()
            }],
            expected_head.map(Into::into),
        )
        .await
    }

    /// Remove an entry from this collection
    #[tracing::instrument(level = "info", name = CollectionMut_remove_entry, skip_all)]
    pub async fn remove_entry(
        &self,
        ctx: &Context<'_>,
        path: CollectionPath,
        expected_head: Option<Multihash<'static>>,
    ) -> Result<CollectionUpdateResult> {
        self.update_entries_impl(
            ctx,
            vec![CollectionUpdateInput {
                remove: Some(CollectionUpdateInputRemove { path }),
                ..Default::default()
            }],
            expected_head.map(Into::into),
        )
        .await
    }

    /// Execute multiple add / move / unlink operations as a single transaction
    #[tracing::instrument(level = "info", name = CollectionMut_update_entries, skip_all)]
    pub async fn update_entries(
        &self,
        ctx: &Context<'_>,
        operations: Vec<CollectionUpdateInput>,
        expected_head: Option<Multihash<'static>>,
    ) -> Result<CollectionUpdateResult> {
        self.update_entries_impl(ctx, operations, expected_head.map(Into::into))
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug)]
pub struct CollectionEntryInput {
    /// Entry path
    pub path: CollectionPath,

    /// DID of the linked dataset
    #[graphql(name = "ref")]
    pub reference: DatasetID<'static>,

    /// Json object containing extra column values
    pub extra_data: Option<serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Default)]
pub struct CollectionUpdateInput {
    /// Inserts new entry under specified path. If an entry at the target path
    /// already exists it will be retracted.
    pub add: Option<CollectionUpdateInputAdd>,

    /// Retracts and appends an entry under the new path. Returns error if from
    /// path does not exist. If an entry at the target path already exists it
    /// will be retracted. Use this to update extra data by specifying same
    /// source and target paths.
    pub r#move: Option<CollectionUpdateInputMove>,

    /// Removes the collection entry. Does nothing if entry does not exist.
    pub remove: Option<CollectionUpdateInputRemove>,
}

#[derive(InputObject, Debug)]
pub struct CollectionUpdateInputAdd {
    pub entry: CollectionEntryInput,
}

#[derive(InputObject, Debug)]
pub struct CollectionUpdateInputMove {
    pub path_from: CollectionPath,
    pub path_to: CollectionPath,

    /// Optionally update the extra data
    pub extra_data: Option<serde_json::Value>,
}

#[derive(InputObject, Debug)]
pub struct CollectionUpdateInputRemove {
    pub path: CollectionPath,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum CollectionUpdateResult {
    Success(CollectionUpdateSuccess),
    UpToDate(CollectionUpdateUpToDate),
    CasFailed(CollectionUpdateErrorCasFailed),
    NotFound(CollectionUpdateErrorNotFound),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CollectionUpdateSuccess {
    pub old_head: Multihash<'static>,
    pub new_head: Multihash<'static>,
}
#[ComplexObject]
impl CollectionUpdateSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn message(&self) -> String {
        String::new()
    }
}

pub struct CollectionUpdateUpToDate;
#[Object]
impl CollectionUpdateUpToDate {
    async fn is_success(&self) -> bool {
        true
    }
    async fn message(&self) -> String {
        String::new()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CollectionUpdateErrorCasFailed {
    expected_head: Multihash<'static>,
    actual_head: Multihash<'static>,
}
#[ComplexObject]
impl CollectionUpdateErrorCasFailed {
    async fn is_success(&self) -> bool {
        false
    }
    async fn message(&self) -> String {
        "Expected head didn't match, dataset was likely updated concurrently".to_string()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CollectionUpdateErrorNotFound {
    pub path: CollectionPath,
}
#[ComplexObject]
impl CollectionUpdateErrorNotFound {
    async fn is_success(&self) -> bool {
        false
    }
    async fn message(&self) -> String {
        format!("Path {} does not exist", self.path)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
