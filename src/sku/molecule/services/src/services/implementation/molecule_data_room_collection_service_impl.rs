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
use database_common::{BatchLookup, PaginationOpts};
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_datasets::*;
use kamu_molecule_domain::*;

use crate::{
    MoleculeDataRoomCollectionReadError,
    MoleculeDataRoomCollectionService,
    MoleculeDataRoomCollectionWriteError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeDataRoomCollectionService)]
pub struct MoleculeDataRoomCollectionServiceImpl {
    view_collection_entries: Arc<dyn ViewCollectionEntriesUseCase>,
    find_collection_entries: Arc<dyn FindCollectionEntriesUseCase>,
    update_collection_entries: Arc<dyn UpdateCollectionEntriesUseCase>,

    rebac_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

impl MoleculeDataRoomCollectionServiceImpl {
    async fn readable_data_room(
        &self,
        data_room_dataset_id: &odf::DatasetID,
    ) -> Result<ResolvedDataset, MoleculeDataRoomCollectionReadError> {
        let readable_dataset = self
            .rebac_registry_facade
            .resolve_dataset_by_ref(
                &data_room_dataset_id.as_local_ref(),
                kamu_core::auth::DatasetAction::Read,
            )
            .await
            .map_err(|e| match e {
                RebacDatasetRefUnresolvedError::NotFound(e) => {
                    MoleculeDataRoomCollectionReadError::DataRoomNotFound(e)
                }
                RebacDatasetRefUnresolvedError::Access(e) => {
                    MoleculeDataRoomCollectionReadError::Access(e)
                }
                e @ RebacDatasetRefUnresolvedError::Internal(_) => e.int_err().into(),
            })?;

        Ok(readable_dataset)
    }

    async fn writable_data_room(
        &self,
        data_room_dataset_id: &odf::DatasetID,
    ) -> Result<ResolvedDataset, MoleculeDataRoomCollectionWriteError> {
        let writable_dataset = self
            .rebac_registry_facade
            .resolve_dataset_by_ref(
                &data_room_dataset_id.as_local_ref(),
                kamu_core::auth::DatasetAction::Write,
            )
            .await
            .map_err(|e| match e {
                RebacDatasetRefUnresolvedError::NotFound(e) => {
                    MoleculeDataRoomCollectionWriteError::DataRoomNotFound(e)
                }
                RebacDatasetRefUnresolvedError::Access(e) => {
                    MoleculeDataRoomCollectionWriteError::Access(e)
                }
                e @ RebacDatasetRefUnresolvedError::Internal(_) => e.int_err().into(),
            })?;

        Ok(writable_dataset)
    }

    async fn execute_collection_update(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        source_event_time: Option<DateTime<Utc>>,
        operations: Vec<CollectionUpdateOperation>,
        expected_head: Option<odf::Multihash>,
    ) -> Result<MoleculeUpdateDataRoomEntryResult, MoleculeDataRoomCollectionWriteError> {
        let writable_data_room = self.writable_data_room(data_room_dataset_id).await?;

        match self
            .update_collection_entries
            .execute(
                WriteCheckedDataset::from_ref(&writable_data_room),
                source_event_time,
                operations,
                expected_head,
            )
            .await
        {
            Ok(UpdateCollectionEntriesResult::Success(r)) => Ok(
                MoleculeUpdateDataRoomEntryResult::Success(MoleculeUpdateDataRoomEntrySuccess {
                    old_head: r.old_head,
                    new_head: r.new_head,
                    inserted_records: r.inserted_records,
                    system_time: r.system_time,
                }),
            ),
            Ok(UpdateCollectionEntriesResult::UpToDate) => {
                Ok(MoleculeUpdateDataRoomEntryResult::UpToDate)
            }
            Ok(UpdateCollectionEntriesResult::NotFound(e)) => {
                Ok(MoleculeUpdateDataRoomEntryResult::EntryNotFound(e.path))
            }
            Err(UpdateCollectionEntriesUseCaseError::Access(e)) => Err(e.into()),
            Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(e)) => Err(e.into()),
            Err(UpdateCollectionEntriesUseCaseError::QuotaExceeded(e)) => Err(e.into()),
            Err(e @ UpdateCollectionEntriesUseCaseError::Internal(_)) => Err(e.int_err().into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeDataRoomCollectionService for MoleculeDataRoomCollectionServiceImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeDataRoomCollectionServiceImpl_get_data_room_collection_entries,
        skip_all,
        fields(%data_room_dataset_id, ?as_of, ?path_prefix, ?max_depth, ?pagination)
    )]
    async fn get_data_room_collection_entries(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        filters: Option<MoleculeDataRoomEntriesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<CollectionEntryListing, MoleculeDataRoomCollectionReadError> {
        let readable_data_room = self.readable_data_room(data_room_dataset_id).await?;

        let entries_listing = self
            .view_collection_entries
            .execute(
                ReadCheckedDataset::from_ref(&readable_data_room),
                as_of,
                path_prefix,
                max_depth,
                filters.and_then(|f| {
                    utils::molecule_fields_filter(
                        None,
                        f.by_tags,
                        f.by_categories,
                        f.by_access_levels,
                    )
                }),
                pagination,
            )
            .await
            .map_err(|e| {
                use ViewCollectionEntriesError as E;
                match e {
                    E::Access(e) => MoleculeDataRoomCollectionReadError::Access(e),
                    E::UnknownExtraDataFieldFilterNames(_) | E::Internal(_) => e.int_err().into(),
                }
            })?;

        Ok(entries_listing)
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeDataRoomCollectionServiceImpl_find_data_room_collection_entry_by_path,
        skip_all,
        fields(%data_room_dataset_id, ?as_of, %path)
    )]
    async fn find_data_room_collection_entry_by_path(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        path: CollectionPath,
    ) -> Result<Option<CollectionEntry>, MoleculeDataRoomCollectionReadError> {
        let readable_data_room = self.readable_data_room(data_room_dataset_id).await?;

        let maybe_entry = self
            .find_collection_entries
            .execute_find_by_path(
                ReadCheckedDataset::from_ref(&readable_data_room),
                as_of,
                path,
            )
            .await
            .map_err(|e| match e {
                e @ FindCollectionEntriesError::Internal(_) => e.int_err(),
            })?;

        Ok(maybe_entry)
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeDataRoomCollectionServiceImpl_find_data_room_collection_entry_by_ref,
        skip_all,
        fields(%data_room_dataset_id, ?as_of, %r#ref)
    )]
    async fn find_data_room_collection_entry_by_ref(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        r#ref: &odf::DatasetID,
    ) -> Result<Option<CollectionEntry>, MoleculeDataRoomCollectionReadError> {
        let readable_data_room = self.readable_data_room(data_room_dataset_id).await?;

        let maybe_entry = self
            .find_collection_entries
            .execute_find_by_ref(
                ReadCheckedDataset::from_ref(&readable_data_room),
                as_of,
                r#ref,
            )
            .await
            .map_err(|e| match e {
                e @ FindCollectionEntriesError::Internal(_) => e.int_err(),
            })?;

        Ok(maybe_entry)
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeDataRoomCollectionServiceImpl_find_data_room_collection_entries_by_refs,
        skip_all,
        fields(%data_room_dataset_id, ?as_of, refs = %format_utils::format_collection(refs))
    )]
    async fn find_data_room_collection_entries_by_refs(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        refs: &[&odf::DatasetID],
    ) -> Result<
        BatchLookup<CollectionEntry, odf::DatasetID, MoleculeDataRoomCollectionReadError>,
        InternalError,
    > {
        let _ = (data_room_dataset_id, as_of, refs);
        todo!()
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeDataRoomCollectionServiceImpl_upsert_data_room_collection_entry,
        skip_all,
        fields(%data_room_dataset_id, %path, %r#ref)
    )]
    async fn upsert_data_room_collection_entry(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        source_event_time: Option<DateTime<Utc>>,
        path: CollectionPath,
        r#ref: odf::DatasetID,
        extra_data: kamu_datasets::ExtraDataFields,
    ) -> Result<CollectionEntry, MoleculeDataRoomCollectionWriteError> {
        let result = self
            .execute_collection_update(
                data_room_dataset_id,
                source_event_time,
                vec![CollectionUpdateOperation::add(path, r#ref, extra_data)],
                None,
            )
            .await?;

        match result {
            MoleculeUpdateDataRoomEntryResult::Success(mut success) => {
                assert!(!success.inserted_records.is_empty());

                let (op, last_inserted_record) = success.inserted_records.pop().unwrap();
                assert_eq!(op, odf::metadata::OperationType::Append);

                Ok(CollectionEntry {
                    system_time: success.system_time,
                    event_time: source_event_time.unwrap_or(success.system_time),
                    path: last_inserted_record.path,
                    reference: last_inserted_record.reference,
                    extra_data: last_inserted_record.extra_data,
                })
            }
            MoleculeUpdateDataRoomEntryResult::UpToDate
            | MoleculeUpdateDataRoomEntryResult::EntryNotFound(_) => {
                unreachable!()
            }
        }
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeDataRoomCollectionServiceImpl_move_data_room_collection_entry_by_path,
        skip_all,
        fields(%data_room_dataset_id, %path_from, %path_to, ?expected_head)
    )]
    async fn move_data_room_collection_entry_by_path(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        source_event_time: Option<DateTime<Utc>>,
        path_from: CollectionPath,
        path_to: CollectionPath,
        extra_data: Option<kamu_datasets::ExtraDataFields>,
        expected_head: Option<odf::Multihash>,
    ) -> Result<MoleculeUpdateDataRoomEntryResult, MoleculeDataRoomCollectionWriteError> {
        self.execute_collection_update(
            data_room_dataset_id,
            source_event_time,
            vec![CollectionUpdateOperation::r#move(
                path_from, path_to, extra_data,
            )],
            expected_head,
        )
        .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeDataRoomCollectionServiceImpl_remove_data_room_collection_entry_by_path,
        skip_all,
        fields(%data_room_dataset_id, %path, ?expected_head)
    )]
    async fn remove_data_room_collection_entry_by_path(
        &self,
        data_room_dataset_id: &odf::DatasetID,
        source_event_time: Option<DateTime<Utc>>,
        path: CollectionPath,
        expected_head: Option<odf::Multihash>,
    ) -> Result<MoleculeUpdateDataRoomEntryResult, MoleculeDataRoomCollectionWriteError> {
        self.execute_collection_update(
            data_room_dataset_id,
            source_event_time,
            vec![CollectionUpdateOperation::remove(path)],
            expected_head,
        )
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
