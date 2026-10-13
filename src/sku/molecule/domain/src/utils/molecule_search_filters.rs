// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use crate::{MoleculeSearchEntityKind, MoleculeSearchFilters};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_search_entity_kinds(
    filters: Option<&MoleculeSearchFilters>,
) -> HashSet<MoleculeSearchEntityKind> {
    filters
        .and_then(|f| {
            f.by_kinds
                .as_ref()
                .map(|kinds_as_vec| kinds_as_vec.iter().copied().collect())
        })
        .unwrap_or_else(MoleculeSearchEntityKind::default_kinds)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
