// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;

///////////////////////////////////////////////////////////////////////////////
// Page-based connection
///////////////////////////////////////////////////////////////////////////////

macro_rules! page_based_connection {
    ($node_type:ident, $connection_type:ident, $edge_type:ident) => {
        #[derive(SimpleObject)]
        #[graphql(complex)]
        pub(crate) struct $connection_type {
            /// A shorthand for `edges { node { ... } }`
            pub nodes: Vec<$node_type>,

            /// Approximate number of total nodes
            pub total_count: Option<usize>,

            /// Page information
            pub page_info: crate::scalars::PageBasedInfo,
        }

        #[ComplexObject]
        impl $connection_type {
            #[graphql(skip)]
            pub fn new(
                nodes: Vec<$node_type>,
                current_page: usize,
                per_page: usize,
                total_count: Option<usize>,
            ) -> Self {
                let (total_pages, has_next_page) = match total_count {
                    None => (None, nodes.len() != per_page),
                    Some(0) => (Some(0), false),
                    Some(tc) => (
                        Some(tc.unstable_div_ceil(per_page)),
                        (tc.unstable_div_ceil(per_page) - 1) > current_page,
                    ),
                };

                Self {
                    nodes,
                    total_count,
                    page_info: crate::scalars::PageBasedInfo {
                        has_previous_page: current_page > 0,
                        has_next_page,
                        current_page,
                        total_pages,
                    },
                }
            }

            async fn edges(&self) -> Vec<$edge_type> {
                self.nodes
                    .iter()
                    .map(|node| $edge_type { node: node.clone() })
                    .collect()
            }
        }

        #[derive(SimpleObject)]
        pub(crate) struct $edge_type {
            pub node: $node_type,
        }
    };
}

pub(crate) use page_based_connection;

#[derive(SimpleObject)]
pub(crate) struct PageBasedInfo {
    /// When paginating backwards, are there more items?
    pub has_previous_page: bool,

    /// When paginating forwards, are there more items?
    pub has_next_page: bool,

    /// Index of the current page
    pub current_page: usize,

    /// Approximate number of total pages assuming number of nodes per page stays the same
    pub total_pages: Option<usize>,
}
