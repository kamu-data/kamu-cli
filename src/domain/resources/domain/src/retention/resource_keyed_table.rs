// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A table with a column holding a resource ID that no foreign key with
/// `ON DELETE CASCADE` ties to the `resources` table, and how its rows go when
/// a resource is purged
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceKeyedTable {
    pub table: &'static str,
    pub purge: ResourceKeyedTablePurge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKeyedTablePurge {
    /// The purge deletes the rows itself, before the resource's row
    ByFramework,
    /// A registered [`crate::ResourceRemovalListener`] deletes them in the
    /// purge transaction
    ByRemovalListener,
    /// A consumer of the `Purged` lifecycle message deletes them
    ByPurgedConsumer,
    /// The rows are not a projection of a resource and stay, for the reason
    /// given
    Outlives(&'static str),
}

/// Every table keyed by a resource ID, other than `resources`, without a
/// cascading foreign key to it. The purge contract test fails for a table
/// keyed by a resource ID that has neither.
pub const RESOURCE_KEYED_TABLES_WITHOUT_CASCADE: &[ResourceKeyedTable] = &[
    ResourceKeyedTable {
        table: "resource_events",
        purge: ResourceKeyedTablePurge::ByFramework,
    },
    ResourceKeyedTable {
        table: "resource_controller_state",
        purge: ResourceKeyedTablePurge::ByFramework,
    },
    ResourceKeyedTable {
        table: "accounts",
        purge: ResourceKeyedTablePurge::Outlives(
            "the permanent resource ID of an account, which is not stored as a resource",
        ),
    },
    ResourceKeyedTable {
        table: "dataset_entries",
        purge: ResourceKeyedTablePurge::Outlives(
            "the permanent resource ID of a dataset, which is not stored as a resource",
        ),
    },
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
