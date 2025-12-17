// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_datasets::CollectionPath;
use kamu_molecule_domain::{
    molecule_data_room_entry_full_text_search_schema as data_room_entry_schema,
    *,
};
use kamu_search::*;

use crate::{
    MoleculeDataRoomCollectionReadError,
    MoleculeDataRoomCollectionService,
    map_molecule_data_room_entries_filters_to_search,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewDataRoomEntriesUseCase)]
pub struct MoleculeViewDataRoomEntriesUseCaseImpl {
    catalog: dill::Catalog,
    data_room_collection_service: Arc<dyn MoleculeDataRoomCollectionService>,
    full_text_search_service: Arc<dyn FullTextSearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeViewDataRoomEntriesUseCaseImpl {
    async fn list_entries_via_collection(
        &self,
        molecule_project: &MoleculeProject,
        as_of: Option<odf::Multihash>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        filters: Option<MoleculeDataRoomEntriesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeDataRoomEntriesListing, MoleculeViewDataRoomEntriesError> {
        let entries_listing = self
            .data_room_collection_service
            .get_data_room_collection_entries(
                &molecule_project.data_room_dataset_id,
                as_of,
                path_prefix,
                max_depth,
                filters,
                pagination,
            )
            .await
            .map_err(|e| -> MoleculeViewDataRoomEntriesError {
                use MoleculeDataRoomCollectionReadError as E;
                match e {
                    E::Access(e) => e.into(),
                    E::DataRoomNotFound(_) | E::Internal(_) => e.int_err().into(),
                }
            })?;

        let molecule_entries = entries_listing
            .list
            .into_iter()
            .map(MoleculeDataRoomEntry::from_collection_entry)
            .collect();

        Ok(MoleculeDataRoomEntriesListing {
            total_count: entries_listing.total_count,
            list: molecule_entries,
        })
    }

    async fn list_latest_entries_via_search(
        &self,
        molecule_project: &MoleculeProject,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        filters: Option<MoleculeDataRoomEntriesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeDataRoomEntriesListing, MoleculeViewDataRoomEntriesError> {
        let ctx = FullTextSearchContext {
            catalog: &self.catalog,
        };

        let filter = Self::prepare_full_text_search_filter(
            &molecule_project.ipnft_uid,
            path_prefix.as_ref(),
            max_depth,
            filters,
        );

        let search_results = self
            .full_text_search_service
            .search(
                ctx,
                FullTextSearchRequest {
                    query: None, // no textual query, just filtering
                    entity_schemas: vec![data_room_entry_schema::SCHEMA_NAME],
                    source: FullTextSearchRequestSourceSpec::All,
                    filter: Some(filter),
                    sort: sort!(data_room_entry_schema::FIELD_PATH),
                    page: pagination.into(),
                    options: FullTextSearchOptions::default(),
                },
            )
            .await?;

        Ok(MoleculeDataRoomEntriesListing {
            total_count: usize::try_from(search_results.total_hits).unwrap(),
            list: search_results
                .hits
                .into_iter()
                .map(|hit| MoleculeDataRoomEntry::from_data_room_entry_json(hit.source))
                .collect::<Result<Vec<_>, InternalError>>()?,
        })
    }

    fn prepare_full_text_search_filter(
        ipnft_uid: &str,
        path_prefix: Option<&CollectionPath>,
        max_depth: Option<usize>,
        filters: Option<MoleculeDataRoomEntriesFilters>,
    ) -> FullTextSearchFilterExpr {
        let mut and_clauses = vec![];

        // ipnft_uid equality
        and_clauses.push(field_eq_str(
            data_room_entry_schema::FIELD_IPNFT_UID,
            ipnft_uid,
        ));

        // path prefix
        if let Some(path_prefix) = path_prefix {
            and_clauses.push(field_prefix(
                data_room_entry_schema::FIELD_PATH,
                path_prefix.as_str(),
            ));
        }

        // max depth
        if let Some(max_depth) = max_depth {
            and_clauses.push(field_lte_num(
                data_room_entry_schema::FIELD_DEPTH,
                i64::try_from(max_depth).unwrap(),
            ));
        }

        // filters by categories, tags, access levels
        if let Some(filters) = filters {
            and_clauses.extend(map_molecule_data_room_entries_filters_to_search(filters));
        }

        FullTextSearchFilterExpr::and_clauses(and_clauses)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeViewDataRoomEntriesUseCase for MoleculeViewDataRoomEntriesUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeViewDataRoomEntriesUseCaseImpl_execute,
        skip_all,
        fields(
            ipnft_uid = %molecule_project.ipnft_uid,
            mode = ?mode,
            path_prefix = ?path_prefix,
            max_depth = ?max_depth,
            pagination = ?pagination
        )
    )]
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        mode: MoleculeViewDataRoomEntriesMode,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        filters: Option<MoleculeDataRoomEntriesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeDataRoomEntriesListing, MoleculeViewDataRoomEntriesError> {
        match mode {
            // When historical view is requested, use slower collection-based method
            MoleculeViewDataRoomEntriesMode::Historical(as_of) => {
                self.list_entries_via_collection(
                    molecule_project,
                    Some(as_of),
                    path_prefix,
                    max_depth,
                    filters,
                    pagination,
                )
                .await
            }

            // Latest state, but it is requested to be read from source collection explicitly
            MoleculeViewDataRoomEntriesMode::LatestSource => {
                self.list_entries_via_collection(
                    molecule_project,
                    None,
                    path_prefix,
                    max_depth,
                    filters,
                    pagination,
                )
                .await
            }

            // As for the latest projection, use faster search-based method
            MoleculeViewDataRoomEntriesMode::LatestProjection => {
                self.list_latest_entries_via_search(
                    molecule_project,
                    path_prefix,
                    max_depth,
                    filters,
                    pagination,
                )
                .await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
