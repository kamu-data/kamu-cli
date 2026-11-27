// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    RESOURCE_ID_COLUMNS_OF_OTHER_NAMES,
    RESOURCE_KEYED_TABLES_WITHOUT_CASCADE,
    ResourceIdColumn,
    ResourceIdColumnPurge,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A column of a migrated schema, outside the `resources` table
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaColumn {
    pub table: String,
    pub column: String,
    /// Whether a foreign key with `ON DELETE CASCADE` ties it to `resources`
    pub cascades: bool,
}

impl SchemaColumn {
    pub fn new(table: String, column: String, cascades: bool) -> Self {
        Self {
            table,
            column,
            cascades,
        }
    }

    fn other_name_declaration(&self) -> Option<&'static ResourceIdColumn> {
        RESOURCE_ID_COLUMNS_OF_OTHER_NAMES
            .iter()
            .find(|declared| declared.table == self.table && declared.column == self.column)
    }

    /// Named `resource_id`, ending in `_resource_id`, or declared in
    /// [`RESOURCE_ID_COLUMNS_OF_OTHER_NAMES`]
    fn holds_resource_id(&self) -> bool {
        self.column == "resource_id"
            || self.column.ends_with("_resource_id")
            || self.other_name_declaration().is_some()
    }

    fn is_kept_on_purge(&self) -> bool {
        match self.other_name_declaration().map(|declared| declared.purge) {
            Some(ResourceIdColumnPurge::Kept(_)) => true,
            Some(ResourceIdColumnPurge::Cascades) | None => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Checks the purge contract over the columns of a schema that hold resource
/// IDs: each one cascades from `resources`, is declared kept in
/// [`RESOURCE_ID_COLUMNS_OF_OTHER_NAMES`], or its table is declared in
/// [`RESOURCE_KEYED_TABLES_WITHOUT_CASCADE`]. Each declared table must exist
/// and have a column without a cascade, and each declared column must exist.
/// Returns the violations, empty when the contract holds.
pub fn purge_contract_violations(columns: &[SchemaColumn]) -> Vec<String> {
    let is_declared = |table: &str| {
        RESOURCE_KEYED_TABLES_WITHOUT_CASCADE
            .iter()
            .any(|t| t.table == table)
    };
    let resource_keyed_columns: Vec<&SchemaColumn> = columns
        .iter()
        .filter(|column| column.holds_resource_id())
        .collect();

    let mut violations: Vec<String> = resource_keyed_columns
        .iter()
        .filter(|column| {
            !column.cascades && !column.is_kept_on_purge() && !is_declared(&column.table)
        })
        .map(|column| {
            format!(
                "{}.{} holds a resource ID with neither a cascading foreign key to resources nor \
                 a declaration in RESOURCE_KEYED_TABLES_WITHOUT_CASCADE or \
                 RESOURCE_ID_COLUMNS_OF_OTHER_NAMES",
                column.table, column.column
            )
        })
        .collect();

    for declared in RESOURCE_KEYED_TABLES_WITHOUT_CASCADE {
        if !resource_keyed_columns
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

    for declared in RESOURCE_ID_COLUMNS_OF_OTHER_NAMES {
        if !columns
            .iter()
            .any(|column| column.table == declared.table && column.column == declared.column)
        {
            violations.push(format!(
                "{}.{} is declared in RESOURCE_ID_COLUMNS_OF_OTHER_NAMES, but does not exist",
                declared.table, declared.column
            ));
        }
    }

    violations
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    fn column(table: &str, column: &str, cascades: bool) -> SchemaColumn {
        SchemaColumn::new(table.to_string(), column.to_string(), cascades)
    }

    /// The declared tables and columns, as a schema that honours the contract
    fn compliant_schema() -> Vec<SchemaColumn> {
        RESOURCE_KEYED_TABLES_WITHOUT_CASCADE
            .iter()
            .map(|declared| column(declared.table, "resource_id", false))
            .chain(
                RESOURCE_ID_COLUMNS_OF_OTHER_NAMES
                    .iter()
                    .map(|declared| column(declared.table, declared.column, false)),
            )
            .collect()
    }

    #[test]
    fn test_columns_of_other_names_follow_their_declarations() {
        // A kept column of another name passes without a cascade
        let schema = compliant_schema();
        assert_eq!(purge_contract_violations(&schema), Vec::<String>::new());

        let without_declared_columns: Vec<SchemaColumn> = schema
            .into_iter()
            .filter(|column| column.other_name_declaration().is_none())
            .collect();
        assert_eq!(
            purge_contract_violations(&without_declared_columns).len(),
            RESOURCE_ID_COLUMNS_OF_OTHER_NAMES.len()
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
