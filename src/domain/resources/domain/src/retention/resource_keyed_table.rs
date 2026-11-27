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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKeyedTablePurge {
    /// The purge deletes the rows itself, before the resource's row
    ByFramework,
    /// The rows are not a projection of a resource and stay, for the reason
    /// given
    Outlives(&'static str),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

/// A column holding a resource ID under a name other than `resource_id` or
/// `*_resource_id`, which the purge contract test cannot recognize by name
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceIdColumn {
    pub table: &'static str,
    pub column: &'static str,
    pub purge: ResourceIdColumnPurge,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceIdColumnPurge {
    /// A foreign key with `ON DELETE CASCADE` ties it to `resources`
    Cascades,
    /// Purging the resource it names leaves the rows, for the reason given
    Kept(&'static str),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Every column holding a resource ID under another name. The purge contract
/// test checks each one as it checks the columns it recognizes by name, and
/// fails for an entry the schema does not have.
pub const RESOURCE_ID_COLUMNS_OF_OTHER_NAMES: &[ResourceIdColumn] = &[ResourceIdColumn {
    table: "resource_owners_projection",
    column: "owner_id",
    purge: ResourceIdColumnPurge::Kept(
        "an owner may be purged before the resources it owns; the row goes with the owned \
         resource, by the cascade on `resource_id`",
    ),
}];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
