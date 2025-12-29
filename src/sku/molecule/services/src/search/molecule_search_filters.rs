// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_molecule_domain::{
    MoleculeActivitiesFilters,
    MoleculeAnnouncementsFilters,
    MoleculeDataRoomEntriesFilters,
    MoleculeSearchFilters,
    molecule_search_schema_common as molecule_schema,
};
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_molecule_data_room_entries_filters_to_search(
    filters: MoleculeDataRoomEntriesFilters,
) -> Vec<SearchFilterExpr> {
    let mut search_filters = vec![];

    if let Some(by_access_levels) = filters.by_access_levels
        && !by_access_levels.is_empty()
    {
        search_filters.push(field_in_str(
            molecule_schema::fields::ACCESS_LEVEL,
            &by_access_levels,
        ));
    }

    if let Some(by_categories) = filters.by_categories
        && !by_categories.is_empty()
    {
        search_filters.push(field_in_str(
            molecule_schema::fields::CATEGORIES,
            &by_categories,
        ));
    }

    if let Some(by_tags) = filters.by_tags
        && !by_tags.is_empty()
    {
        search_filters.push(field_in_str(molecule_schema::fields::TAGS, &by_tags));
    }

    search_filters
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_molecule_announcements_filters_to_search(
    filters: MoleculeAnnouncementsFilters,
) -> Vec<SearchFilterExpr> {
    let mut search_filters = vec![];

    if let Some(by_access_levels) = filters.by_access_levels
        && !by_access_levels.is_empty()
    {
        search_filters.push(field_in_str(
            molecule_schema::fields::ACCESS_LEVEL,
            &by_access_levels,
        ));
    }

    if let Some(by_categories) = filters.by_categories
        && !by_categories.is_empty()
    {
        search_filters.push(field_in_str(
            molecule_schema::fields::CATEGORIES,
            &by_categories,
        ));
    }

    if let Some(by_tags) = filters.by_tags
        && !by_tags.is_empty()
    {
        search_filters.push(field_in_str(molecule_schema::fields::TAGS, &by_tags));
    }

    search_filters
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_molecule_activities_filters_to_search(
    filters: MoleculeActivitiesFilters,
) -> Vec<SearchFilterExpr> {
    let mut search_filters = vec![];

    if let Some(by_access_levels) = filters.by_access_levels
        && !by_access_levels.is_empty()
    {
        search_filters.push(field_in_str(
            molecule_schema::fields::ACCESS_LEVEL,
            &by_access_levels,
        ));
    }

    if let Some(by_categories) = filters.by_categories
        && !by_categories.is_empty()
    {
        search_filters.push(field_in_str(
            molecule_schema::fields::CATEGORIES,
            &by_categories,
        ));
    }

    if let Some(by_tags) = filters.by_tags
        && !by_tags.is_empty()
    {
        search_filters.push(field_in_str(molecule_schema::fields::TAGS, &by_tags));
    }

    search_filters
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_molecule_search_filters(filters: MoleculeSearchFilters) -> Vec<SearchFilterExpr> {
    // Note: kinds are schema choice, not filter

    let mut search_filters = vec![];

    if let Some(by_ipnft_uids) = filters.by_ipnft_uids
        && !by_ipnft_uids.is_empty()
    {
        search_filters.push(field_in_str(
            molecule_schema::fields::IPNFT_UID,
            &by_ipnft_uids,
        ));
    }

    if let Some(by_access_levels) = filters.by_access_levels
        && !by_access_levels.is_empty()
    {
        search_filters.push(field_in_str(
            molecule_schema::fields::ACCESS_LEVEL,
            &by_access_levels,
        ));
    }

    if let Some(by_categories) = filters.by_categories
        && !by_categories.is_empty()
    {
        search_filters.push(field_in_str(
            molecule_schema::fields::CATEGORIES,
            &by_categories,
        ));
    }

    if let Some(by_tags) = filters.by_tags
        && !by_tags.is_empty()
    {
        search_filters.push(field_in_str(molecule_schema::fields::TAGS, &by_tags));
    }

    search_filters
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
