// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_molecule_domain::{
    MoleculeAccessLevelRule,
    MoleculeActivitiesFilters,
    MoleculeActivityKindSelection,
    MoleculeAnnouncementsFilters,
    MoleculeDataRoomEntriesFilters,
    MoleculeDataRoomFileActivityType,
    MoleculeSearchFilters,
    molecule_activity_search_schema as activity_schema,
    molecule_announcement_search_schema as announcement_schema,
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

    if let Some(by_access_level_rules) = filters.by_access_level_rules
        && !by_access_level_rules.is_empty()
    {
        search_filters.push(map_molecule_access_level_rules_to_search(
            &by_access_level_rules,
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

    if let Some(by_access_level_rules) = filters.by_access_level_rules
        && !by_access_level_rules.is_empty()
    {
        search_filters.push(map_molecule_access_level_rules_to_search(
            &by_access_level_rules,
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

pub(crate) fn map_molecule_activity_kinds_to_search_schemas(
    kind_selection: &MoleculeActivityKindSelection,
) -> Vec<&'static str> {
    let mut schemas = vec![];
    if kind_selection.includes_data_room_activities() {
        schemas.push(activity_schema::SCHEMA_NAME);
    }
    if kind_selection.includes_announcements() {
        schemas.push(announcement_schema::SCHEMA_NAME);
    }
    schemas
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_molecule_activities_filters_to_search(
    filters: MoleculeActivitiesFilters,
) -> Vec<SearchFilterExpr> {
    let mut search_filters = vec![];

    // Excluding the unselected types rather than matching the selected ones keeps
    // announcements, whose index has no `activity_type` field
    let excluded_activity_types = MoleculeActivityKindSelection::from_filters(Some(&filters))
        .excluded_data_room_activity_types();
    if !excluded_activity_types.is_empty() {
        search_filters.push(filter_not!(field_in_str(
            activity_schema::fields::ACTIVITY_TYPE,
            excluded_activity_types
                .into_iter()
                .map(MoleculeDataRoomFileActivityType::stored_value),
        )));
    }

    if let Some(by_access_levels) = filters.by_access_levels
        && !by_access_levels.is_empty()
    {
        search_filters.push(field_in_str(
            molecule_schema::fields::ACCESS_LEVEL,
            &by_access_levels,
        ));
    }

    if let Some(by_access_level_rules) = filters.by_access_level_rules
        && !by_access_level_rules.is_empty()
    {
        search_filters.push(map_molecule_access_level_rules_to_search(
            &by_access_level_rules,
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

    if let Some(by_ocl_ids) = filters.by_ocl_ids
        && !by_ocl_ids.is_empty()
    {
        search_filters.push(field_in_str(molecule_schema::fields::OCL_ID, &by_ocl_ids));
    }

    if let Some(by_access_levels) = filters.by_access_levels
        && !by_access_levels.is_empty()
    {
        search_filters.push(field_in_str(
            molecule_schema::fields::ACCESS_LEVEL,
            &by_access_levels,
        ));
    }

    if let Some(by_access_level_rules) = filters.by_access_level_rules
        && !by_access_level_rules.is_empty()
    {
        search_filters.push(map_molecule_access_level_rules_to_search(
            &by_access_level_rules,
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

fn map_molecule_access_level_rules_to_search(
    access_level_rules: &[MoleculeAccessLevelRule],
) -> SearchFilterExpr {
    let mut or_clauses = Vec::with_capacity(access_level_rules.len());

    for rule in access_level_rules {
        or_clauses.push(filter_and!(
            // Match access levels
            field_in_str(molecule_schema::fields::ACCESS_LEVEL, &rule.access_levels),
            // Match ocl_id
            match &rule.ocl_id {
                Some(ocl_id) => field_eq_str(molecule_schema::fields::OCL_ID, ocl_id.as_ref()),
                None => unreachable!(),
            }
        ));
    }

    SearchFilterExpr::or_clauses(or_clauses)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
