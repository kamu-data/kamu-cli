// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use crate::MoleculeAccessLevelRule;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MoleculeActivityKind {
    DataRoomActivity,
    Announcement,
}

impl MoleculeActivityKind {
    pub fn default_kinds() -> HashSet<MoleculeActivityKind> {
        [Self::DataRoomActivity, Self::Announcement].into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct MoleculeActivitiesFilters {
    pub by_tags: Option<Vec<String>>,
    pub by_categories: Option<Vec<String>>,
    pub by_access_levels: Option<Vec<String>>,
    pub by_access_level_rules: Option<Vec<MoleculeAccessLevelRule>>,
    pub by_kinds: Option<Vec<MoleculeActivityKind>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Backward-compatible alias for search filters
pub type MoleculeSearchEntityKind = MoleculeActivityKind;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
