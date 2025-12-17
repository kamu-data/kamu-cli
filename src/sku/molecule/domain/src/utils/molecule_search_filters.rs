// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use crate::{MoleculeSearchFilters, MoleculeSearchType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_search_types(filters: Option<&MoleculeSearchFilters>) -> HashSet<MoleculeSearchType> {
    filters
        .and_then(|f| {
            f.by_types
                .as_ref()
                .map(|types_as_vec| types_as_vec.iter().copied().collect())
        })
        .unwrap_or_else(MoleculeSearchType::default_types)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
