// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::common::DFSchema;
use datafusion::logical_expr::Operator;
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
    pub fn project(&self, ledger: DataFrame) -> Result<DataFrame, MergeError> {
        // TODO: Re-assess implementation
        //
        // CDC projection requires retrieving last records per primary key.
        //
        // In Spark it would look something like:
        //
        //   select x, last(y) from table group by x
        //
        // But in DataFusion last_value() aggregate function panics:
        //
        //   select x, last_value(y) from table group by x
        //
        // The postgres-like DISTINCT ON returns "not implemented":
        //
        //   select distinct on (x) x, y from table order by date desc
        //
        // Self-join did not complete on 1M row table:
        //
        //   select a.* from table a
        //   left join table b on a.x = b.x and a.date < b.date
        //   where b.x is null
        //
        // The window function approach below works, but might be sub-optimal:
        //
        //   select * from (
        //     select
        //       *,
        //       row_number() over (partition by x order by date desc) as rank
        //     from table
        //   ) where rank = 1
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
            .filter(col(rank_col).eq(lit(1)))
            .int_err()?
            .without_columns(&[rank_col])
            .int_err()?
            .filter(col(&self.obsv_column).not_eq(lit(&self.obsv_removed)))
            .int_err()?;

        Ok(state)
    }

    /// Returns table name ususe to disambiguate fields
    fn get_table_qualifier(
        &self,
        df: &DataFrame,
    ) -> Result<TableReference<'static>, InternalError> {
        Ok(df
            .schema()
            .field_with_unqualified_name(self.primary_key.first().unwrap())
            .int_err()?
            .qualifier()
            .unwrap()
            .clone())
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

        let new_qual = self.get_table_qualifier(&new)?;
        let proj_qual = self.get_table_qualifier(&proj)?;

        let pk: Vec<_> = self.primary_key.iter().map(|s| s.as_str()).collect();
        let cdc_filter = self.get_cdc_filter(new.schema(), proj_qual.clone(), new_qual.clone())?;
        let cdc_select = self.get_cdc_select(new.schema(), proj_qual.clone(), new_qual.clone())?;

        // Diff old and new states by performing full join and detecting changes
        let res = proj
            .join(new, JoinType::Full, &pk, &pk, None)
            .int_err()?
            // TODO: PERF: This condition ideally should be in join filter, but at the time of
            // writing it was producing incorrect results
            .filter(cdc_filter)
            .int_err()?
            .select(cdc_select)
            .int_err()?;

        tracing::info!(
            schema = ?res.schema(),
            logical_plan = ?res.logical_plan(),
            "Performing merge",
        );

        Ok(res)
    }
}
