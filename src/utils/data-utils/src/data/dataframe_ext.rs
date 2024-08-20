// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::error::Result;
use datafusion::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DataFrameExt
where
    Self: Sized,
{
    fn columns_to_front(self, front_cols: &[&str]) -> Result<Self>;

    fn without_columns(self, cols: &[&str]) -> Result<Self>;

    fn column_names_to_lowercase(self) -> Result<DataFrame, datafusion::error::DataFusionError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DataFrameExt for DataFrame {
    fn columns_to_front(self, front_cols: &[&str]) -> Result<Self> {
        let mut columns: Vec<_> = front_cols.iter().map(|s| col(*s)).collect();

        columns.extend(
            self.schema()
                .fields()
                .iter()
                .filter(|f| !front_cols.contains(&f.name().as_str()))
                .map(|f| col(Column::from_name(f.name()))),
        );

        assert_eq!(columns.len(), self.schema().fields().len());

        self.select(columns)
    }

    fn without_columns(self, cols: &[&str]) -> Result<Self> {
        let columns: Vec<_> = self
            .schema()
            .fields()
            .iter()
            .filter(|f| !cols.contains(&f.name().as_str()))
            .map(|f| col(Column::from_name(f.name())))
            .collect();

        self.select(columns)
    }

    // Case-sensitivity in DataFusion is following Postgres which is not very
    // intuitive to use with identifiers containing upper case letters.
    //
    // See: https://github.com/apache/datafusion/issues/7460
    //
    // In SQL, DF uses `enable_ident_normalization` by default to transform all
    // identifiers to lowercase.
    //
    // See: https://datafusion.apache.org/user-guide/configs.html
    //
    // When reading data from CSV, JSON and other sources, however, they return
    // column names as-is, so upper case letters become very common.
    //
    // If JSON has `someField` column you'll have to use quoted identifiers to refer
    // to it:
    //
    //   select "someFiled" -- works
    //   select somefield   -- doesn't
    //
    // This is very painful when reading files with multiple columns as you'll have
    // to quote every field appearance in schema, sql, merge, strategy, and
    // vocab.
    //
    // Ideal solution would make DataFusion respect case-insensitivity unless there
    // is an obvious ambiguity, but alas this is too intrusive. Instead using
    // this function we normalize all identifiers to lowercase, unless this
    // causes a collision between two columns.
    //
    // This approach is more in line with `enable_ident_normalization` config
    // option.
    //
    // TODO: Support nested structs
    fn column_names_to_lowercase(self) -> Result<DataFrame, datafusion::error::DataFusionError> {
        let mut names_lower = std::collections::HashSet::<String>::new();
        let mut names_lower_collisions = std::collections::HashSet::<String>::new();
        let mut to_rename = 0;

        for field in self.schema().fields() {
            let name_lower = field.name().to_lowercase();
            if name_lower != *field.name() {
                to_rename += 1;
            }
            let collided = !names_lower.insert(name_lower);
            if collided {
                names_lower_collisions.insert(field.name().to_lowercase());
            }
        }

        if to_rename == 0 {
            return Ok(self);
        }

        let (session_state, plan) = self.into_parts();
        let schema_before = plan.schema().clone();

        let projection = plan
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                let name_lower = field.name().to_lowercase();

                if names_lower_collisions.contains(&name_lower) {
                    // Propagate as-is
                    col(Column::from((qualifier, field)))
                } else {
                    // Rename
                    tracing::debug!(field = field.name(), "Renaming!!!!!!!!!!!!!!!");
                    col(Column::from((qualifier, field))).alias(name_lower)
                }
            })
            .collect::<Vec<_>>();

        let project_plan = datafusion::logical_expr::LogicalPlanBuilder::from(plan)
            .project(projection)?
            .build()?;

        tracing::debug!(?schema_before, schema_after = ?project_plan.schema(), "Normalizing field names on read");

        Ok(DataFrame::new(session_state, project_plan))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
