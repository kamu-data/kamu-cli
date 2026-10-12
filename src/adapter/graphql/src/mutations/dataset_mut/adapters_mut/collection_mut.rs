// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::QuotaError;
use kamu_datasets::{
    CollectionEntryAdd,
    CollectionEntryMove,
    CollectionEntryRemove,
    CollectionUpdateOperation,
    UpdateCollectionEntriesResult,
    UpdateCollectionEntriesUseCase,
    UpdateCollectionEntriesUseCaseError,
    WriteCheckedDataset,
};

use crate::prelude::*;
use crate::queries::DatasetRequestState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CollectionMut<'a> {
    writable_state: &'a DatasetRequestState,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'a> CollectionMut<'a> {
    pub fn new(writable_state: &'a DatasetRequestState) -> Self {
        Self { writable_state }
    }

    pub async fn update_entries_impl(
        &self,
        ctx: &Context<'_>,
        operations: Vec<CollectionUpdateInput>,
        expected_head: Option<odf::Multihash>,
    ) -> Result<CollectionUpdateResult> {
        let update_collection_entries = from_catalog_n!(ctx, dyn UpdateCollectionEntriesUseCase);

        let mut mapped_ops = Vec::with_capacity(operations.len());
        for op in operations {
            if let Some(add) = op.add {
                mapped_ops.push(CollectionUpdateOperation::Add(CollectionEntryAdd {
                    record: add.entry.into(),
                }));
            } else if let Some(remove) = op.remove {
                mapped_ops.push(CollectionUpdateOperation::Remove(CollectionEntryRemove {
                    path: remove.path.into(),
                }));
            } else if let Some(mov) = op.r#move {
                mapped_ops.push(CollectionUpdateOperation::Move(CollectionEntryMove {
                    path_from: mov.path_from.into(),
                    path_to: mov.path_to.into(),
                    extra_data: mov.extra_data.map(Into::into),
                }));
            }
        }

        let collection_dataset = self.writable_state.resolved_dataset(ctx).await?;

        match update_collection_entries
            .execute(
                WriteCheckedDataset::from_ref(collection_dataset),
                None,
                mapped_ops,
                expected_head,
            )
            .await
        {
            Ok(UpdateCollectionEntriesResult::Success(res)) => {
                Ok(CollectionUpdateResult::Success(CollectionUpdateSuccess {
                    old_head: res.old_head.into(),
                    new_head: res.new_head.into(),
                }))
            }
            Ok(UpdateCollectionEntriesResult::UpToDate) => {
                Ok(CollectionUpdateResult::UpToDate(CollectionUpdateUpToDate))
            }
            Ok(UpdateCollectionEntriesResult::NotFound(not_found)) => Ok(
                CollectionUpdateResult::NotFound(CollectionUpdateErrorNotFound {
                    path: not_found.path.into(),
                }),
            ),
            Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(err)) => Ok(
                CollectionUpdateResult::CasFailed(CollectionUpdateErrorCasFailed {
                    expected_head: err.expected.unwrap().into(),
                    actual_head: err.actual.map(Into::into),
                }),
            ),
            Err(UpdateCollectionEntriesUseCaseError::Access(err)) => Err(err.int_err().into()),
            Err(UpdateCollectionEntriesUseCaseError::QuotaExceeded(err)) => match err {
                QuotaError::Exceeded(e) => Ok(CollectionUpdateResult::QuotaExceeded(
                    CollectionUpdateErrorQuotaExceeded {
                        used: Some(e.used),
                        incoming: Some(e.incoming),
                        limit: Some(e.limit),
                    },
                )),
                QuotaError::NotConfigured => Ok(CollectionUpdateResult::QuotaExceeded(
                    CollectionUpdateErrorQuotaExceeded {
                        used: None,
                        incoming: None,
                        limit: None,
                    },
                )),
                QuotaError::Internal(e) => Err(e.into()),
            },
            Err(UpdateCollectionEntriesUseCaseError::Internal(err)) => Err(err.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Mutations of a collection dataset
#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
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
        path_from: CollectionPath<'static>,
        path_to: CollectionPath<'static>,
        extra_data: Option<ExtraData>,
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
        path: CollectionPath<'static>,
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

/// Collection entry to add
#[derive(InputObject, Debug)]
pub struct CollectionEntryInput {
    /// Entry path
    pub path: CollectionPath<'static>,

    /// DID of the linked dataset
    #[graphql(name = "ref")]
    pub reference: DatasetID<'static>,

    /// Json object containing extra column values
    pub extra_data: Option<ExtraData>,
}

impl From<CollectionEntryInput> for kamu_datasets::CollectionEntryRecord {
    fn from(value: CollectionEntryInput) -> Self {
        Self {
            path: value.path.into(),
            reference: value.reference.into(),
            extra_data: value.extra_data.unwrap_or_default().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Single collection update operation; exactly one field must be set
#[derive(InputObject, Debug, Default)]
pub struct CollectionUpdateInput {
    /// Inserts a new entry under the specified path. If an entry at the target
    /// path already exists, it will be retracted.
    pub add: Option<CollectionUpdateInputAdd>,

    /// Retracts and appends an entry under the new path. Returns error if from
    /// path does not exist. If an entry at the target path already exists, it
    /// will be retracted. Use this to update extra data by specifying the same
    /// source and target paths.
    pub r#move: Option<CollectionUpdateInputMove>,

    /// Removes the collection entry. Does nothing if entry does not exist.
    pub remove: Option<CollectionUpdateInputRemove>,
}

/// Adds an entry to the collection
#[derive(InputObject, Debug)]
pub struct CollectionUpdateInputAdd {
    /// Entry to add
    pub entry: CollectionEntryInput,
}

/// Moves an entry to a new path
#[derive(InputObject, Debug)]
pub struct CollectionUpdateInputMove {
    /// Current path of the entry
    pub path_from: CollectionPath<'static>,
    /// New path of the entry
    pub path_to: CollectionPath<'static>,

    /// Optionally update the extra data
    pub extra_data: Option<ExtraData>,
}

/// Removes an entry from the collection
#[derive(InputObject, Debug)]
pub struct CollectionUpdateInputRemove {
    /// Path of the entry to remove
    pub path: CollectionPath<'static>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Outcome of updating collection entries
#[derive(Interface)]
#[graphql(
    field(
        name = "is_success",
        ty = "bool",
        desc = "Whether the operation succeeded"
    ),
    field(
        name = "message",
        ty = "String",
        desc = "Human-readable outcome message"
    )
)]
pub enum CollectionUpdateResult {
    Success(CollectionUpdateSuccess),
    UpToDate(CollectionUpdateUpToDate),
    CasFailed(CollectionUpdateErrorCasFailed),
    NotFound(CollectionUpdateErrorNotFound),
    QuotaExceeded(CollectionUpdateErrorQuotaExceeded),
}

/// Collection entries were updated
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CollectionUpdateSuccess {
    /// Hash of the metadata chain head before the update
    pub old_head: Multihash<'static>,
    /// Hash of the metadata chain head after the update
    pub new_head: Multihash<'static>,
}
#[ComplexObject]
impl CollectionUpdateSuccess {
    /// Always true
    async fn is_success(&self) -> bool {
        true
    }
    /// Always empty
    async fn message(&self) -> String {
        String::new()
    }
}

pub struct CollectionUpdateUpToDate;
/// Operations did not change the collection
#[Object]
impl CollectionUpdateUpToDate {
    /// Always true
    async fn is_success(&self) -> bool {
        true
    }
    /// Always empty
    async fn message(&self) -> String {
        String::new()
    }
}

/// Dataset head did not match the expected one, likely due to a concurrent update
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CollectionUpdateErrorCasFailed {
    /// Head block hash the caller expected
    expected_head: Multihash<'static>,
    /// Current head block hash of the dataset, null if it has none
    actual_head: Option<Multihash<'static>>,
}
#[ComplexObject]
impl CollectionUpdateErrorCasFailed {
    /// Always false
    async fn is_success(&self) -> bool {
        false
    }
    /// Human-readable error message
    async fn message(&self) -> String {
        "Expected head didn't match, dataset was likely updated concurrently".to_string()
    }
}

/// Entry referenced by an operation does not exist
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CollectionUpdateErrorNotFound {
    /// Path that was not found
    pub path: CollectionPath<'static>,
}

#[ComplexObject]
impl CollectionUpdateErrorNotFound {
    /// Always false
    async fn is_success(&self) -> bool {
        false
    }
    /// Human-readable error message
    async fn message(&self) -> String {
        format!("Path {} does not exist", self.path)
    }
}

/// Update would exceed the storage quota of the account
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CollectionUpdateErrorQuotaExceeded {
    /// Storage already used, in bytes; null if no quota is configured
    pub used: Option<u64>,
    /// Size of the rejected data, in bytes; null if no quota is configured
    pub incoming: Option<u64>,
    /// Storage limit, in bytes; null if no quota is configured
    pub limit: Option<u64>,
}

#[ComplexObject]
impl CollectionUpdateErrorQuotaExceeded {
    /// Always false
    async fn is_success(&self) -> bool {
        false
    }

    /// Human-readable error message
    async fn message(&self) -> String {
        match (self.used, self.incoming, self.limit) {
            (Some(used), Some(incoming), Some(limit)) => {
                format!("Quota exceeded: used={used}, incoming={incoming}, limit={limit}")
            }
            _ => "Quota exceeded".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
