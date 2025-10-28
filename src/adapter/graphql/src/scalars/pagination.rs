// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Page-based finite connection
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Public only for tests
#[macro_export]
macro_rules! page_based_connection {
    ($node_type:ident, $connection_type:ident, $edge_type:ident) => {
        #[derive(async_graphql::SimpleObject)]
        #[graphql(complex)]
        pub struct $connection_type {
            /// A shorthand for `edges { node { ... } }`
            pub nodes: Vec<$node_type>,

            /// Approximate number of total nodes
            pub total_count: usize,

            /// Page information
            pub page_info: $crate::scalars::PageBasedInfo,
        }

        #[async_graphql::ComplexObject]
        impl $connection_type {
            #[graphql(skip)]
            pub fn new(
                nodes: Vec<$node_type>,
                current_page: usize,
                per_page: usize,
                total_count: usize,
            ) -> Self {
                let (total_pages, has_next_page) = match total_count {
                    0 => (Some(0), false),
                    tc => {
                        if per_page == 0 {
                            (Some(0), false)
                        } else {
                            (
                                Some(tc.div_ceil(per_page)),
                                (tc.div_ceil(per_page) - 1) > current_page,
                            )
                        }
                    }
                };

                Self {
                    nodes,
                    total_count,
                    page_info: $crate::scalars::PageBasedInfo {
                        has_previous_page: current_page > 0,
                        has_next_page,
                        current_page,
                        total_pages,
                    },
                }
            }

            async fn edges(&self) -> Vec<$edge_type<'_>> {
                self.nodes.iter().map(|node| $edge_type { node }).collect()
            }
        }

        #[derive(async_graphql::SimpleObject)]
        pub struct $edge_type<'a> {
            pub node: &'a $node_type,
        }
    };
}

pub(crate) use page_based_connection;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct PageBasedInfo {
    /// When paginating backwards, are there more items?
    pub has_previous_page: bool,

    /// When paginating forwards, are there more items?
    pub has_next_page: bool,

    /// Index of the current page
    pub current_page: usize,

    /// Approximate number of total pages assuming number of nodes per page
    /// stays the same
    pub total_pages: Option<usize>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
