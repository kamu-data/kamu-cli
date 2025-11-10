// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::common::DFSchema;
use datafusion::logical_expr::{LogicalPlanBuilder, Operator, SortExpr};
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use internal_error::*;
use odf::utils::data::DataFrameExt;

use crate::*;

type Op = odf::metadata::OperationType;

/// Snapshot merge strategy.
///
/// See [`odf::MergeStrategySnapshot`] for details.
pub struct MergeStrategySnapshot {
    vocab: odf::metadata::DatasetVocabulary,
    primary_key: Vec<String>,
    compare_columns: Option<Vec<String>>,
}

impl MergeStrategySnapshot {
    pub fn new(
        vocab: odf::metadata::DatasetVocabulary,
        cfg: odf::metadata::MergeStrategySnapshot,
    ) -> Self {
        assert!(!cfg.primary_key.is_empty());
        if let Some(c) = &cfg.compare_columns {
            assert!(!c.is_empty());
        }
        Self {
            vocab,
            primary_key: cfg.primary_key,
            compare_columns: cfg.compare_columns,
        }
    }

    /// Projects the CDC ledger into a state snapshot.
    ///
    /// Implementation is mostly equivalent to this example (using
    /// datafusion-cli):
    ///
    /// ```text
    /// create or replace table ledger (
    ///     offset bigint not null,
    ///     op string not null,
    ///     city string not null, -- PK
    ///     population int not null
    /// ) as values
    ///   (0, '+A', 'a', 1000),
    ///   (1, '+A', 'b', 2000),
    ///   (2, '+A', 'c', 3000),
    ///   (3, '-C', 'b', 2000),
    ///   (4, '+C', 'b', 2500),
    ///   (5, '-C', 'a', 1000),
    ///   (6, '+C', 'a', 1500),
    ///   (7, '-R', 'a', 1500);
    ///
    /// select * from (
    ///    select
    ///        *,
    ///        row_number() over (
    ///            partition by city
    ///            order by offset desc
    ///        ) as __rank
    ///     from ledger
    /// ) where __rank = 1 and op != '-R';
    /// ```
    ///
    /// Which should output:
    ///
    /// ```text
    /// +--------+----+------+------------+--------+
    /// | offset | op | city | population | __rank |
    /// +--------+----+------+------------+--------+
    /// | 2      | +I | c    | 3000       | 1      |
    /// | 3      | +C | b    | 2500       | 1      |
    /// +--------+----+------+------------+--------+
    /// ```
    pub fn project(&self, ledger: DataFrameExt) -> Result<DataFrameExt, InternalError> {
        odf::utils::data::changelog::project(ledger, &self.primary_key, &self.vocab).int_err()
    }

    /// Returns a filter like:
    ///
    /// ```text
    /// where
    ///   ((new.event_time is not null) and (old.event_time is distinct from new.event_time))
    ///   or (old.x is distinct from new.x)
    ///   or (old.y is distinct from new.y)
    ///   or ...
    fn get_cdc_filter(
        &self,
        new_schema: &DFSchema,
        old_qual: &TableReference,
        new_qual: &TableReference,
    ) -> Expr {
        let old_qual = old_qual.clone();
        let new_qual = new_qual.clone();

        let columns: Vec<_> = if let Some(compare_columns) = &self.compare_columns {
            compare_columns.iter().map(String::as_str).collect()
        } else {
            new_schema
                .fields()
                .iter()
                .filter(|f| !self.primary_key.contains(f.name()))
                .map(|f| f.name().as_str())
                .collect()
        };

        columns
            .into_iter()
            .map(move |c| {
                let distinct = binary_expr(
                    col(Column::new(Some(old_qual.clone()), c)),
                    Operator::IsDistinctFrom,
                    col(Column::new(Some(new_qual.clone()), c)),
                );

                // Event time in `new` can be null and this alone should not be the reason to
                // consider the row changed
                if c != self.vocab.event_time_column {
                    distinct
                } else {
                    and(
                        col(Column::new(Some(new_qual.clone()), c)).is_not_null(),
                        distinct,
                    )
                }
            })
            .reduce(Expr::or)
            .unwrap_or(lit(false))
    }

    /// Performs Change Data Capture diff between old and new states.
    ///
    /// It is mostly equivalent to this query (try in datafusion-cli):
    ///
    /// ```text
    /// create or replace table old (
    ///     year int,
    ///     city string not null,
    ///     population int not null
    /// ) as values
    /// (2020, 'vancouver', 1),
    /// (2020, 'seattle', 2),
    /// (2020, 'kyiv', 3);
    ///
    /// create or replace table new (
    ///     year int,
    ///     city string not null,
    ///     population int not null
    /// ) as values
    /// (null, 'seattle', 2),
    /// (null, 'kyiv', 4),
    /// (null, 'odessa', 5);
    ///
    /// with cdc as (
    ///   select
    ///     old.year as old_year,
    ///     old.city as old_city,
    ///     old.population as old_population,
    ///     new.year as new_year,
    ///     new.city as new_city,
    ///     new.population as new_population
    ///   from old
    ///   full outer join new
    ///     on old.city = new.city
    ///   where
    ///     -- Note the special treatment of event time to ignore nulls in `new`
    ///     ((new.year is not null) and (old.year is distinct from new.year))
    ///     or (old.population is distinct from new.population)
    /// )
    ///
    /// select * from (
    ///   select
    ///     case
    ///       when old_city is null then '+A'
    ///       when new_city is null then '-R'
    ///       else '+C'
    ///     end as op,
    ///     case
    ///       when new_city is null then old_year
    ///       else new_year
    ///     end as year,
    ///     case
    ///       when new_city is null then old_city
    ///       else new_city
    ///     end as city,
    ///     case
    ///       when new_city is null then old_population
    ///       else new_population
    ///     end as population
    ///   from cdc
    ///   union all
    ///   select
    ///     '-C' as op,
    ///     old_year as year,
    ///     old_city as city,
    ///     old_population as population
    ///   from cdc
    ///   where
    ///     old_city is not null and new_city is not null
    /// )
    /// order by city, op;
    /// ```
    ///
    /// The complexity of this query is mostly caused by the need to emit two
    /// events (correct-from and correct-to) for records that were modified,
    /// necessitating UNION ALL, and by requirement that correction events
    /// must appear side by side, necessitating ORDER BY.
    fn cdc_diff(
        &self,
        old: DataFrameExt,
        new: DataFrameExt,
    ) -> Result<DataFrameExt, DataFusionErrorWrapped> {
        // NOTE: We use `old` schema as presumably it will be read according to
        // canonical schema of the whole dataset and will represent the expected
        // nullability, while `new` schema may be inferred and needs only to be
        // coercible into `old`.
        let non_null_columns: std::collections::HashSet<String> = old
            .schema()
            .fields()
            .iter()
            .filter(|f| !f.is_nullable())
            .filter(|f| *f.name() != self.vocab.event_time_column)
            .map(|f| f.name().clone())
            .collect();

        // TODO: Schema evolution
        let a_old = TableReference::bare("old");
        let a_new = TableReference::bare("new");
        let old_col = |name: &str| -> Expr { Expr::Column(Column::new(Some(a_old.clone()), name)) };
        let new_col = |name: &str| -> Expr { Expr::Column(Column::new(Some(a_new.clone()), name)) };

        // Select expression for +A, -R, +C part of UNION ALL
        let pk = self.primary_key.first().unwrap().as_str();
        let mut select_app_retr_correct_to = Vec::new();
        select_app_retr_correct_to.push(
            // TODO: Cast to `u8` after Spark is updated
            // See: https://github.com/kamu-data/kamu-cli/issues/445
            when(old_col(pk).is_null(), lit(Op::Append as i32))
                .when(new_col(pk).is_null(), lit(Op::Retract as i32))
                .otherwise(lit(Op::CorrectTo as i32))?
                .alias(&self.vocab.operation_type_column),
        );
        select_app_retr_correct_to.extend(new.schema().fields().iter().map(|f| {
            when(new_col(pk).is_null(), old_col(f.name()))
                .otherwise(new_col(f.name()))
                .unwrap()
                .alias(f.name())
        }));

        // Select expression for -C part of UNION ALL
        let mut select_correct_from = Vec::new();
        // TODO: Cast to `u8` after Spark is updated
        // See: https://github.com/kamu-data/kamu-cli/issues/445
        select_correct_from
            .push(lit(Op::CorrectFrom as i32).alias(&self.vocab.operation_type_column));
        select_correct_from.extend(
            new.schema()
                .fields()
                .iter()
                .map(|f| old_col(f.name()).alias(f.name())),
        );

        let (session_state, old) = old.into_parts();
        let (_, new) = new.into_parts();
        let old = LogicalPlanBuilder::from(old).alias(a_old.clone())?;
        let new = LogicalPlanBuilder::from(new).alias(a_new.clone())?;

        let filter = self.get_cdc_filter(new.schema().as_ref(), &a_old, &a_new);

        let cdc = old
            .join(
                new.build()?,
                JoinType::Full,
                (
                    self.primary_key
                        .iter()
                        .map(|s| Column::new(Some(a_old.clone()), s))
                        .collect(),
                    self.primary_key
                        .iter()
                        .map(|s| Column::new(Some(a_new.clone()), s))
                        .collect(),
                ),
                None,
            )?
            .filter(filter)?
            .build()?;

        // TODO: PERF: Currently DataFusion will perform full join twice, although it
        // would likely be more performant to reuse the result of `cdc` sub-query.
        // See: https://github.com/apache/arrow-datafusion/issues/8777
        let plan = LogicalPlanBuilder::from(cdc.clone())
            .project(select_app_retr_correct_to)?
            .union(
                LogicalPlanBuilder::from(cdc)
                    .filter(and(old_col(pk).is_not_null(), new_col(pk).is_not_null()))?
                    .project(select_correct_from)?
                    .build()?,
            )?
            .build()?;

        // Note: Final sorting will be done by the caller using `sort_order()`
        // expression.
        let df: DataFrameExt = DataFrame::new(session_state, plan).into();

        // Perform nullability correction, as all columns become nullable after join
        let df = df.assert_collumns_not_null(|f| non_null_columns.contains(f.name()))?;

        Ok(df)
    }
}

impl MergeStrategy for MergeStrategySnapshot {
    fn merge(
        &self,
        prev: Option<DataFrameExt>,
        new: DataFrameExt,
    ) -> Result<DataFrameExt, MergeError> {
        if prev.is_none() {
            // Validate PK columns exist
            new.clone()
                .select(
                    self.primary_key
                        .iter()
                        .map(|name| col(Column::from_name(name)))
                        .collect(),
                )
                .int_err()?;

            // Consider all records as appends
            let df = new
                .with_column(
                    &self.vocab.operation_type_column,
                    // TODO: Cast to `u8` after Spark is updated
                    // See: https://github.com/kamu-data/kamu-cli/issues/445
                    lit(odf::metadata::OperationType::Append as i32),
                )
                .int_err()?
                .columns_to_front(&[&self.vocab.operation_type_column])
                .int_err()?;

            return Ok(df);
        }

        // Project existing CDC ledger into a state
        let proj = self
            .project(prev.unwrap())?
            .without_columns(&[&self.vocab.offset_column, &self.vocab.operation_type_column])
            .int_err()?;

        // Diff state with new data
        let res = self.cdc_diff(proj, new)?;

        Ok(res)
    }

    fn sort_order(&self) -> Vec<SortExpr> {
        // Main goal here is to establish correct order of -C / +C corrections, so we
        // sort records by primary key and then by operation type
        self.primary_key
            .iter()
            .map(|c| col(Column::from_name(c)).sort(true, true))
            .chain(std::iter::once(
                col(Column::from_name(&self.vocab.operation_type_column)).sort(true, true),
            ))
            .collect()
    }
}
