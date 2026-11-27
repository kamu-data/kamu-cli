// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::RESOURCE_KEYED_TABLES_WITHOUT_CASCADE;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A column of a migrated schema holding a resource ID: one named
/// `resource_id` or ending in `_resource_id`, outside the `resources` table
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceKeyedColumn {
    pub table: String,
    pub column: String,
    /// Whether a foreign key with `ON DELETE CASCADE` ties it to `resources`
    pub cascades: bool,
}

impl ResourceKeyedColumn {
    pub fn new(table: String, column: String, cascades: bool) -> Self {
        Self {
            table,
            column,
            cascades,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Checks the purge contract over the resource-keyed columns of a schema:
/// each one cascades from `resources`, or its table is declared in
/// [`RESOURCE_KEYED_TABLES_WITHOUT_CASCADE`], and each declared table exists
/// and has a column without a cascade. Returns the violations, empty when the
/// contract holds.
pub fn purge_contract_violations(columns: &[ResourceKeyedColumn]) -> Vec<String> {
    let is_declared = |table: &str| {
        RESOURCE_KEYED_TABLES_WITHOUT_CASCADE
            .iter()
            .any(|t| t.table == table)
    };

    let mut violations: Vec<String> = columns
        .iter()
        .filter(|column| !column.cascades && !is_declared(&column.table))
        .map(|column| {
            format!(
                "{}.{} holds a resource ID with neither a cascading foreign key to resources nor \
                 a declaration in RESOURCE_KEYED_TABLES_WITHOUT_CASCADE",
                column.table, column.column
            )
        })
        .collect();

    for declared in RESOURCE_KEYED_TABLES_WITHOUT_CASCADE {
        if !columns
            .iter()
            .any(|column| column.table == declared.table && !column.cascades)
        {
            violations.push(format!(
                "{} is declared in RESOURCE_KEYED_TABLES_WITHOUT_CASCADE, but has no resource ID \
                 column without a cascading foreign key",
                declared.table
            ));
        }
    }

    violations
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
