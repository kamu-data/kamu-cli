// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::FullTextSearchFieldPath;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub enum FullTextSortSpec {
    #[default]
    Relevance,

    ByField {
        field: FullTextSearchFieldPath,
        direction: FullTextSortDirection,
        nulls_first: bool,
    },
}

#[derive(Debug)]
pub enum FullTextSortDirection {
    Ascending,
    Descending,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! sort_by {
    // field
    ($field:expr) => {
        $crate::FullTextSortSpec::ByField {
            field: $field.into(),
            direction: $crate::FullTextSortDirection::Ascending,
            nulls_first: false,
        }
    };

    // field, asc
    ($field:expr, asc $(,)?) => {
        $crate::FullTextSortSpec::ByField {
            field: $field.into(),
            direction: $crate::FullTextSortDirection::Ascending,
            nulls_first: false,
        }
    };

    // field, desc
    ($field:expr, desc $(,)?) => {
        $crate::FullTextSortSpec::ByField {
            field: $field.into(),
            direction: $crate::FullTextSortDirection::Descending,
            nulls_first: false,
        }
    };

    // field, asc, nulls_first
    ($field:expr, asc, nulls_first $(,)?) => {
        $crate::FullTextSortSpec::ByField {
            field: $field.into(),
            direction: $crate::FullTextSortDirection::Ascending,
            nulls_first: true,
        }
    };

    // field, desc, nulls_first
    ($field:expr, desc, nulls_first $(,)?) => {
        $crate::FullTextSortSpec::ByField {
            field: $field.into(),
            direction: $crate::FullTextSortDirection::Descending,
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
