// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use strum::IntoEnumIterator;

use crate::{MoleculeAccessLevelRule, MoleculeDataRoomFileActivityType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MoleculeActivityKind {
    DataRoomActivity(MoleculeDataRoomFileActivityType),
    Announcement,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct MoleculeActivitiesFilters {
    pub by_tags: Option<Vec<String>>,
    pub by_categories: Option<Vec<String>>,
    pub by_access_levels: Option<Vec<String>>,
    pub by_access_level_rules: Option<Vec<MoleculeAccessLevelRule>>,
    pub by_kinds: Option<Vec<MoleculeActivityKind>>,
    /// Only activity of the data room entries that link these datasets; leaves
    /// out announcements, which are not linked to an entry
    pub by_refs: Option<Vec<odf::DatasetID>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Activity kinds a listing includes; an absent or empty `by_kinds` filter
/// includes every kind, except announcements when `by_refs` is not empty
#[derive(Clone, Debug)]
pub struct MoleculeActivityKindSelection {
    data_room_activity_types: HashSet<MoleculeDataRoomFileActivityType>,
    announcements: bool,
}

impl MoleculeActivityKindSelection {
    pub fn from_filters(filters: Option<&MoleculeActivitiesFilters>) -> Self {
        let by_kinds = filters.and_then(|f| f.by_kinds.as_deref()).unwrap_or(&[]);
        let by_refs = filters
            .and_then(|f| f.by_refs.as_deref())
            .is_some_and(|by_refs| !by_refs.is_empty());

        if by_kinds.is_empty() {
            return Self {
                data_room_activity_types: MoleculeDataRoomFileActivityType::iter().collect(),
                announcements: !by_refs,
            };
        }

        let mut selection = Self {
            data_room_activity_types: HashSet::new(),
            announcements: false,
        };
        for kind in by_kinds {
            match kind {
                MoleculeActivityKind::DataRoomActivity(activity_type) => {
                    selection.data_room_activity_types.insert(*activity_type);
                }
                MoleculeActivityKind::Announcement => selection.announcements = !by_refs,
            }
        }
        selection
    }

    pub fn includes_data_room_activities(&self) -> bool {
        !self.data_room_activity_types.is_empty()
    }

    pub fn includes_data_room_activity_type(
        &self,
        activity_type: MoleculeDataRoomFileActivityType,
    ) -> bool {
        self.data_room_activity_types.contains(&activity_type)
    }

    /// Data room activity types left out; empty when all of them are included
    pub fn excluded_data_room_activity_types(&self) -> Vec<MoleculeDataRoomFileActivityType> {
        MoleculeDataRoomFileActivityType::iter()
            .filter(|activity_type| !self.data_room_activity_types.contains(activity_type))
            .collect()
    }

    pub fn includes_announcements(&self) -> bool {
        self.announcements
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MoleculeSearchEntityKind {
    DataRoomActivity,
    Announcement,
}

impl MoleculeSearchEntityKind {
    pub fn default_kinds() -> HashSet<MoleculeSearchEntityKind> {
        [Self::DataRoomActivity, Self::Announcement].into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
