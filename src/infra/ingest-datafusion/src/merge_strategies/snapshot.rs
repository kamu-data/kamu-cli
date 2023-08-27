// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::common::{DFSchema, OwnedTableReference};
use datafusion::logical_expr::{LogicalPlanBuilder, Operator};
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use internal_error::*;
use kamu_data_utils::data::dataframe_ext::*;
use opendatafabric as odf;

use crate::*;

/// Snapshot merge strategy.
///
/// See [odf::MergeStrategySnapshot] for details.
pub struct MergeStrategySnapshot {
    primary_key: Vec<String>,
    compare_columns: Option<Vec<String>>,
    order_column: String,
    obsv_column: String,
    obsv_added: String,
    obsv_changed: String,
    obsv_removed: String,
}

impl MergeStrategySnapshot {
    pub fn new(order_column: String, cfg: odf::MergeStrategySnapshot) -> Self {
        assert!(cfg.primary_key.len() != 0);
        if let Some(c) = &cfg.compare_columns {
            assert!(c.len() != 0);
        }
        Self {
            primary_key: cfg.primary_key,
            compare_columns: cfg.compare_columns,
            order_column,
            obsv_column: cfg.observation_column.unwrap_or_else(|| {
                odf::MergeStrategySnapshot::DEFAULT_OBSV_COLUMN_NAME.to_string()
            }),
            obsv_added: cfg
                .obsv_added
                .unwrap_or_else(|| odf::MergeStrategySnapshot::DEFAULT_OBSV_ADDED.to_string()),
            obsv_changed: cfg
                .obsv_changed
                .unwrap_or_else(|| odf::MergeStrategySnapshot::DEFAULT_OBSV_CHANGED.to_string()),
            obsv_removed: cfg
                .obsv_removed
                .unwrap_or_else(|| odf::MergeStrategySnapshot::DEFAULT_OBSV_REMOVED.to_string()),
        }
    }

    /// Projects the CDC ledger into a state snapshot.
    ///
    /// Implementation is mostly equivalent to this example (using
    /// datafusion-cli):
    ///
    ///   create or replace table ledger (
    ///       offset bigint not null,
    ///       obsv string not null,
    ///       city string not null,
    ///       population int not null
    ///   ) as values
    ///   (0, '+', 'a', 1000),
    ///   (1, '+', 'b', 2000),
    ///   (2, '+', 'c', 3000),
    ///   (3, 'u', 'b', 2500),
    ///   (4, 'u', 'a', 1500),
    ///   (5, '-', 'a', 1500);
    ///
    ///   select * from (
    ///      select
    ///          *,
    ///          row_number() over (
    ///              partition by city
    ///              order by offset desc) as __row_num
    ///       from ledger
    ///   ) where __row_num = 1 and obsv != '-';
    ///
    /// Which should output:
    ///
    ///   +--------+------+------+------------+-----------+
    ///   | offset | obsv | city | population | __row_num |
    ///   +--------+------+------+------------+-----------+
    ///   | 2      | +    | c    | 3000       | 1         |
    ///   | 3      | u    | b    | 2500       | 1         |
    ///   +--------+------+------+------------+-----------+
    pub fn project(&self, ledger: DataFrame) -> Result<DataFrame, InternalError> {
        // TODO: PERF: Re-assess implementation as it may be sub-optimal
        //

        let rank_col = "__rank";

        let state = ledger
            .window(vec![Expr::WindowFunction(
                datafusion::logical_expr::expr::WindowFunction {
                    fun: datafusion::logical_expr::WindowFunction::BuiltInWindowFunction(
                        datafusion::logical_expr::BuiltInWindowFunction::RowNumber,
                    ),
                    args: Vec::new(),
                    partition_by: self.primary_key.iter().map(|c| col(c)).collect(),
                    order_by: vec![col(&self.order_column).sort(false, false)],
                    window_frame: datafusion::logical_expr::WindowFrame::new(true),
                },
            )
            .alias(rank_col)])
            .int_err()?
            .filter(
                col(rank_col)
                    .eq(lit(1))
                    .and(col(&self.obsv_column).not_eq(lit(&self.obsv_removed))),
            )
            .int_err()?
            .without_columns(&[rank_col])
            .int_err()?;

        Ok(state)
    }

    /// Returns filter like:
    ///   a.x is distinct from b.x OR a.y is distinct from b.y OR ...
    fn get_cdc_filter(
        &self,
        schema: &DFSchema,
        left_qual: TableReference<'static>,
        right_qual: TableReference<'static>,
    ) -> Result<Expr, InternalError> {
        let columns: Vec<_> = if let Some(compare_columns) = &self.compare_columns {
            compare_columns.iter().map(|s| s.as_str()).collect()
        } else {
            schema
                .fields()
                .iter()
                .filter(|f| !self.primary_key.contains(f.name()))
                .map(|f| f.name().as_str())
                .collect()
        };

        let expr = columns
            .into_iter()
            .map(move |c| {
                binary_expr(
                    col(Column::new(Some(left_qual.clone()), c)),
                    Operator::IsDistinctFrom,
                    col(Column::new(Some(right_qual.clone()), c)),
                )
            })
            .reduce(Expr::or)
            .unwrap_or(lit(true));

        Ok(expr)
    }

    /// Returns select expression like:
    ///   SELECT
    ///     CASE
    ///       WHEN a.pk is null THEN 'I'
    ///       WHEN b.pk is null THEN 'D'
    ///       ELSE 'U'
    ///     END as observed,
    ///     a.x,
    ///     a.y
    ///   FROM ...
    fn get_cdc_select(
        &self,
        schema: &DFSchema,
        left_qual: TableReference<'static>,
        right_qual: TableReference<'static>,
    ) -> Result<Vec<Expr>, InternalError> {
        let pk = self.primary_key.first().unwrap();

        let case = when(
            col(Column::new(Some(left_qual.clone()), pk)).is_null(),
            lit(&self.obsv_added),
        )
        .when(
            col(Column::new(Some(right_qual.clone()), pk)).is_null(),
            lit(&self.obsv_removed),
        )
        .otherwise(lit(&self.obsv_changed))
        .int_err()?
        .alias(&self.obsv_column);

        let mut select = vec![case];
        select.extend(schema.fields().iter().map(|f| {
            coalesce(vec![
                col(Column::new(Some(right_qual.clone()), f.name())),
                col(Column::new(Some(left_qual.clone()), f.name())),
            ])
            .alias(f.name())
        }));

        Ok(select)
    }

    /// Performs Change Data Capture diff between old and new state.
    ///
    /// It is mostly equivalent to this query (using datafusion-cli):
    ///
    ///   select
    ///     case
    ///       when old.city is null then '+'
    ///       when new.city is null then '-'
    ///       else 'u'
    ///     end as obsv,
    ///     coalesce(old.city, new.city) as city,
    ///     coalesce(old.population, new.population) as population
    ///   from old
    ///   full join new
    ///   on old.city = new.city
    ///   where old.population is distinct from new.population;
    fn cdc_diff(&self, old: DataFrame, new: DataFrame) -> Result<DataFrame, InternalError> {
        let a_old: OwnedTableReference = "old".into();
        let a_new: OwnedTableReference = "new".into();

        let (session_state, old) = old.into_parts();
        let (_, new) = new.into_parts();

        let old = LogicalPlanBuilder::from(old)
            .alias(a_old.clone())
            .int_err()?;
        let new = LogicalPlanBuilder::from(new)
            .alias(a_new.clone())
            .int_err()?;

        // TODO: Schema evolution
        // Filters out values that didn't change
        let filter = self.get_cdc_filter(new.schema().as_ref(), a_old.clone(), a_new.clone())?;

        let select: Vec<Expr> = self.get_cdc_select(new.schema(), a_old.clone(), a_new.clone())?;

        let plan = old
            .join(
                new.build().int_err()?,
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
            )
            .int_err()?
            .filter(filter)
            .int_err()?
            .project(select)
            .int_err()?
            .build()
            .int_err()?;

        Ok(DataFrame::new(session_state, plan))
    }
}

impl MergeStrategy for MergeStrategySnapshot {
    fn merge(&self, prev: Option<DataFrame>, new: DataFrame) -> Result<DataFrame, MergeError> {
        if prev.is_none() {
            // Validate PK columns exist
            new.clone()
                .select(self.primary_key.iter().map(|c| col(c)).collect())
                .int_err()?;

            let df = new
                .with_column(&self.obsv_column, lit(&self.obsv_added))
                .int_err()?
                .columns_to_front(&[&self.obsv_column])
                .int_err()?;

            return Ok(df);
        }

        // Project existing CDC ledger into a state
        let proj = self
            .project(prev.unwrap())?
            .without_columns(&[&self.obsv_column, &self.order_column])
            .int_err()?;

        // Diff state with new data
        let res = self.cdc_diff(proj, new).int_err()?;

        Ok(res)
    }
}
