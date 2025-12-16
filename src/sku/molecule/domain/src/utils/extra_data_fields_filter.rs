// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use nonempty::NonEmpty;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn molecule_fields_filter(
    by_ipnft_uids: Option<Vec<String>>,
    by_tags: Option<Vec<String>>,
    by_categories: Option<Vec<String>>,
    by_access_levels: Option<Vec<String>>,
) -> Option<kamu_datasets::ExtraDataFieldsFilter> {
    use kamu_datasets::ExtraDataFieldFilter as Filter;

    let maybe_ipnft_uids_filter = by_ipnft_uids.and_then(|values| {
        NonEmpty::from_vec(values).map(|values| Filter {
            field_name: "ipnft_uid".to_string(),
            values,
            is_array: true,
        })
    });
    let maybe_tags_filter = by_tags.and_then(|values| {
        NonEmpty::from_vec(values).map(|values| Filter {
            field_name: "tags".to_string(),
            values,
            is_array: true,
        })
    });
    let maybe_categories_filter = by_categories.and_then(|values| {
        NonEmpty::from_vec(values).map(|values| Filter {
            field_name: "categories".to_string(),
            values,
            is_array: true,
        })
    });
    let maybe_access_levels_filter = by_access_levels.and_then(|values| {
        NonEmpty::from_vec(values).map(|values| Filter {
            field_name: "molecule_access_level".to_string(),
            values,
            is_array: false,
        })
    });

    let filters = maybe_ipnft_uids_filter
        .into_iter()
        .chain(maybe_tags_filter)
        .chain(maybe_categories_filter)
        .chain(maybe_access_levels_filter)
        .collect::<Vec<_>>();

    NonEmpty::from_vec(filters)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
