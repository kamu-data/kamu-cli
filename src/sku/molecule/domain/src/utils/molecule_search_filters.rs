// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{MoleculeSearchFilters, MoleculeSearchResultType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_search_result_type(filters: Option<&MoleculeSearchFilters>) -> MoleculeSearchResultType {
    filters
        .and_then(|f| f.by_type)
        .unwrap_or(MoleculeSearchResultType::DataRoomEntriesAndAnnouncements)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
