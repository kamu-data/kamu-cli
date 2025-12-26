// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::SearchFieldPath;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub enum SearchSortSpec {
    #[default]
    Relevance,

    ByField {
        field: SearchFieldPath,
        direction: SearchSortDirection,
        nulls_first: bool,
    },
}

#[derive(Debug)]
pub enum SearchSortDirection {
    Ascending,
    Descending,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! sort_by {
    // field
    ($field:expr) => {
        $crate::SearchSortSpec::ByField {
            field: $field.into(),
            direction: $crate::SearchSortDirection::Ascending,
            nulls_first: false,
        }
    };

    // field, asc
    ($field:expr, asc $(,)?) => {
        $crate::SearchSortSpec::ByField {
            field: $field.into(),
            direction: $crate::SearchSortDirection::Ascending,
            nulls_first: false,
        }
    };

    // field, desc
    ($field:expr, desc $(,)?) => {
        $crate::SearchSortSpec::ByField {
            field: $field.into(),
            direction: $crate::SearchSortDirection::Descending,
            nulls_first: false,
        }
    };

    // field, asc, nulls_first
    ($field:expr, asc, nulls_first $(,)?) => {
        $crate::SearchSortSpec::ByField {
            field: $field.into(),
            direction: $crate::SearchSortDirection::Ascending,
            nulls_first: true,
        }
    };

    // field, desc, nulls_first
    ($field:expr, desc, nulls_first $(,)?) => {
        $crate::SearchSortSpec::ByField {
            field: $field.into(),
            direction: $crate::SearchSortDirection::Descending,
            nulls_first: true,
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! sort {
    () => {
        compile_error!("sort!() requires at least one sort specification")
    };

    // At least one item required
    ( $( $field:expr $(, $($opts:tt)+ )? );+ $(;)? ) => {
        vec![
            $(
                $crate::sort_by!($field $(, $($opts)+ )? )
            ),+
        ]
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
