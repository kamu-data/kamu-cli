// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::DFSchema;
use datafusion::logical_expr::{LogicalPlanBuilder, Operator, SortExpr};
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use internal_error::*;
use odf::utils::data::DataFrameExt;

use crate::*;

type Op = odf::metadata::OperationType;

/// Ledger merge strategy.
///
/// See [`odf_metadata::MergeStrategyUpsertStream`] for details.
pub struct MergeStrategyUpsertStream {
    vocab: odf::metadata::DatasetVocabulary,
    primary_key: Vec<String>,
}

impl MergeStrategyUpsertStream {
    pub fn new(
        vocab: odf::metadata::DatasetVocabulary,
        cfg: odf::metadata::MergeStrategyUpsertStream,
    ) -> Self {
        Self {
            vocab,
            primary_key: cfg.primary_key,
        }
    }

    /// Projects the changelog stream into latest values observed by PK (see
    /// full SQL example below).
    fn latest_by_pk(&self, changelog: DataFrameExt) -> Result<DataFrameExt, MergeError> {
        let rank_col = "__rank";

        let proj = changelog
            .window(vec![datafusion::functions_window::row_number::row_number()
                .partition_by(
                    self.primary_key
                        .iter()
                        .map(|name| col(Column::from_name(name)))
                        .collect(),
                )
                .order_by(vec![
                    col(Column::from_name(&self.vocab.offset_column)).sort(false, false)
                ])
                .build()
                .int_err()?
                .alias(rank_col)])
            .int_err()?
            .filter(
                col(Column::from_name(rank_col)).eq(lit(1)).and(
                    // TODO: Cast to `u8` after Spark is updated
                    // See: https://github.com/kamu-data/kamu-cli/issues/445
                    col(Column::from_name(&self.vocab.operation_type_column))
                        .not_eq(lit(Op::Retract as i32)),
                ),
            )
            .int_err()?
            .without_columns(&[
                rank_col,
                &self.vocab.offset_column,
                &self.vocab.operation_type_column,
            ])
            .int_err()?;

        Ok(proj)
    }

    /// Eliminates redundant updates within the input batch (see full SQL
    /// example below).
    fn without_intermediate_updates(
        &self,
        upserts: DataFrameExt,
    ) -> Result<DataFrameExt, DataFusionErrorWrapped> {
        let rank_col = "__rank";

        let with_offsets = upserts.with_column(
            &self.vocab.offset_column,
            datafusion::functions_window::row_number::row_number(),
        )?;

        let proj = with_offsets
            .window(vec![datafusion::functions_window::row_number::row_number()
                .partition_by(
                    self.primary_key
                        .iter()
                        .map(|name| col(Column::from_name(name)))
                        .collect(),
                )
                .order_by(vec![
                    col(Column::from_name(&self.vocab.offset_column)).sort(false, false)
                ])
                .build()?
                .alias(rank_col)])?
            .filter(col(Column::from_name(rank_col)).eq(lit(1)))?
            .without_columns(&[rank_col, &self.vocab.offset_column])?;

        Ok(proj)
    }

    /// Returns a filter indicating a change (see SQL below)
    fn get_diff_dedupe_filter(
        &self,
        new_schema: &DFSchema,
        old_qual: &TableReference,
        new_qual: &TableReference,
    ) -> Expr {
        let a_old = old_qual.clone();
        let a_new = new_qual.clone();
        let old_col = |name: &str| -> Expr { Expr::Column(Column::new(Some(a_old.clone()), name)) };
        let new_col = |name: &str| -> Expr { Expr::Column(Column::new(Some(a_new.clone()), name)) };

        let pk = self.primary_key.first().unwrap().as_str();

        let ordinary_columns = new_schema
            .fields()
            .iter()
            .filter(|f| {
                !self.primary_key.contains(f.name())
                    && *f.name() != self.vocab.operation_type_column
            })
            .map(|f| f.name().as_str());

        or(
            // Retraction and previous state exists
            new_col(&self.vocab.operation_type_column)
                .eq(lit(Op::Retract as i32))
                .and(old_col(pk).is_not_null()),
            // Upsert
            new_col(&self.vocab.operation_type_column)
                .not_eq(lit(Op::Retract as i32))
                .and(
                    ordinary_columns
                        .map(move |c| {
                            let distinct =
                                binary_expr(old_col(c), Operator::IsDistinctFrom, new_col(c));

                            // Event time in `new` can be null and this alone should not be the
                            // reason to consider the row changed
                            if c == self.vocab.event_time_column {
                                and(new_col(c).is_not_null(), distinct)
                            } else {
                                distinct
                            }
                        })
                        .reduce(Expr::or)
                        .unwrap_or(lit(false)),
                ),
        )
    }

    /// Returns a diff between the `latest_by_pk` projection and an upsert
    /// stream (see SQL below).
    fn diff(
        &self,
        old: DataFrameExt,
        new: DataFrameExt,
    ) -> Result<DataFrameExt, DataFusionErrorWrapped> {
        // TODO: Schema evolution
        let a_old = TableReference::bare("old");
        let a_new = TableReference::bare("new");

        let (session_state, old) = old.into_parts();
        let (_, new) = new.into_parts();
        let old = LogicalPlanBuilder::from(old).alias(a_old.clone())?;
        let new = LogicalPlanBuilder::from(new).alias(a_new.clone())?;

        let filter = self.get_diff_dedupe_filter(new.schema().as_ref(), &a_old, &a_new);

        let diff = new
            .join(
                old.build()?,
                JoinType::Left,
                (
                    self.primary_key
                        .iter()
                        .map(|s| Column::new(Some(a_new.clone()), s))
                        .collect(),
                    self.primary_key
                        .iter()
                        .map(|s| Column::new(Some(a_old.clone()), s))
                        .collect(),
                ),
                None,
            )?
            .filter(filter)?
            .build()?;

        Ok(DataFrame::new(session_state, diff).into())
    }

    /// Converts an upsert stream into changelog stream.
    ///
    /// It is mostly equivalent to this query (try in datafusion-cli):
    ///
    /// ```text
    /// create or replace table old (
    ///     offset int,
    ///     op string,
    ///     year int,
    ///     city string not null,
    ///     population int not null
    /// ) as values
    /// (0, '+A', 2020, 'vancouver', 1),
    /// (1, '+A', 2020, 'seattle', 2),
    /// (2, '+A', 2020, 'kyiv', 3),
    /// (3, '+A', 2020, 'bakhmut', 4),
    /// (4, '-R', 2020, 'bakhmut', 4),
    /// (5, '-C', 2020, 'kyiv', 3),
    /// (6, '+C', 2020, 'kyiv', 4);
    ///
    /// create or replace table new (
    ///     op string,
    ///     year int,
    ///     city string not null,
    ///     population int
    /// ) as values
    /// ('+A', 2020, 'odessa', 5),     -- append
    /// ('+A', 2020, 'kyiv', 4),       -- no-op
    /// ('+A', 2021, 'vancouver', 2),  -- correction
    /// ('+A', 2021, 'bakhmut', 1),    -- append after retract
    /// ('-R', 2020, 'seattle', 2);    -- retract
    ///
    /// -- Assuming PK is ['city'], creating a changelog of a state table
    /// -- It could be ['year', 'city'] to get a changelog of a ledger table
    ///
    /// with latest_by_pk as (
    ///     select
    ///         year,
    ///         city,
    ///         population
    ///     from (
    ///         select
    ///             *,
    ///             row_number() over (
    ///                 partition by city order by offset desc
    ///             ) as __rank
    ///         from old
    ///     )
    ///     where __rank == 1 and op != '-R'
    /// ),
    ///
    /// without_intermediate_updates as (
    ///     select
    ///         op,
    ///         year,
    ///         city,
    ///         population
    ///     from (
    ///         select
    ///             *,
    ///             row_number() over (
    ///                 partition by city order by offset desc
    ///             ) as __rank
    ///         from (
    ///             select
    ///                 *,
    ///                 row_number() over () as offset
    ///             from new
    ///         )
    ///     )
    ///     where __rank == 1
    /// ),
    ///
    /// diff as (
    ///   select
    ///     new.op as op,
    ///     old.year as old_year,
    ///     old.city as old_city,
    ///     old.population as old_population,
    ///     new.year as new_year,
    ///     new.city as new_city,
    ///     new.population as new_population
    ///   from without_intermediate_updates as new
    ///   left join latest_by_pk as old
    ///     on old.city = new.city
    ///   where
    ///     (
    ///         new.op == '-R' and old.city is not null
    ///     )
    ///     or
    ///     (
    ///         new.op != '-R'
    ///         and (
    ///             (old.year is distinct from new.year)
    ///             or (old.population is distinct from new.population)
    ///         )
    ///     )
    /// )
    ///
    /// select * from (
    ///   select
    ///     case
    ///       when op == '-R' then '-R'
    ///       when old_city is null then '+A'
    ///       else '+C'
    ///     end as op,
    ///     case
    ///       when op == '-R' then old_year
    ///       when new_city is null then old_year
    ///       else new_year
    ///     end as year,
    ///     case
    ///       when op == '-R' then old_city
    ///       else new_city
    ///     end as city,
    ///     case
    ///       when op == '-R' then old_population
    ///       else new_population
    ///     end as population
    ///   from diff
    ///   union all
    ///   select
    ///     '-C' as op,
    ///     old_year as year,
    ///     old_city as city,
    ///     old_population as population
    ///   from diff
    ///   where
    ///     op != '-R' and old_city is not null and new_city is not null
    /// )
    /// order by city, op;
    /// ```
    ///
    /// Should produce:
    /// +----+------+-----------+------------+
    /// | op | year | city      | population |
    /// +----+------+-----------+------------+
    /// | +A | 2021 | bakhmut   | 1          |
    /// | +A | 2020 | odessa    | 5          |
    /// | -R | 2020 | seattle   | 2          |
    /// | -C | 2020 | vancouver | 1          |
    /// | +C | 2021 | vancouver | 2          |
    /// +----+------+-----------+------------+
    fn upsert_to_changelog_stream(
        &self,
        old: DataFrameExt,
        new: DataFrameExt,
    ) -> Result<DataFrameExt, DataFusionErrorWrapped> {
        let a_old = TableReference::bare("old");
        let a_new = TableReference::bare("new");
        let old_col = |name: &str| -> Expr { Expr::Column(Column::new(Some(a_old.clone()), name)) };
        let new_col = |name: &str| -> Expr { Expr::Column(Column::new(Some(a_new.clone()), name)) };

        let data_fields: Vec<String> = new
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|c| *c != self.vocab.operation_type_column)
            .collect();

        let without_intermediate_updates = self.without_intermediate_updates(new)?;

        let diff = self.diff(old, without_intermediate_updates)?;
        let (session_state, diff) = diff.into_parts();

        // Select expression for +A, -R, +C part of UNION ALL
        let pk = self.primary_key.first().unwrap().as_str();
        let mut select_app_retr_correct_to = Vec::new();
        select_app_retr_correct_to.push(
            // TODO: Cast to `u8` after Spark is updated
            // See: https://github.com/kamu-data/kamu-cli/issues/445
            when(
                new_col(&self.vocab.operation_type_column).eq(lit(Op::Retract as i32)),
                lit(Op::Retract as i32),
            )
            .when(old_col(pk).is_null(), lit(Op::Append as i32))
            .otherwise(lit(Op::CorrectTo as i32))?
            .alias(&self.vocab.operation_type_column),
        );
        select_app_retr_correct_to.extend(data_fields.iter().map(|c| {
            when(
                new_col(&self.vocab.operation_type_column).eq(lit(Op::Retract as i32)),
                old_col(c),
            )
            .otherwise(new_col(c))
            .unwrap()
            .alias(c)
        }));

        // Select expression for -C part of UNION ALL
        let mut select_correct_from = Vec::new();
        // TODO: Cast to `u8` after Spark is updated
        // See: https://github.com/kamu-data/kamu-cli/issues/445
        select_correct_from
            .push(lit(Op::CorrectFrom as i32).alias(&self.vocab.operation_type_column));
        select_correct_from.extend(data_fields.iter().map(|c| old_col(c).alias(c)));

        // TODO: PERF: Currently DataFusion will perform full join twice, although it
        // would likely be more performant to reuse the result of `diff` sub-query.
        // See: https://github.com/apache/arrow-datafusion/issues/8777
        let plan = LogicalPlanBuilder::from(diff.clone())
            .project(select_app_retr_correct_to)?
            .union(
                LogicalPlanBuilder::from(diff)
                    .filter(
                        new_col(&self.vocab.operation_type_column)
                            .not_eq(lit(Op::Retract as i32))
                            .and(old_col(pk).is_not_null())
                            .and(new_col(pk).is_not_null()),
                    )?
                    .project(select_correct_from)?
                    .build()?,
            )?
            .build()?;

        // Note: Final sorting will be done by the caller using `sort_order()`
        // expression.
        Ok(DataFrame::new(session_state, plan).into())
    }

    fn normalize_input(&self, new: DataFrameExt) -> Result<DataFrameExt, MergeError> {
        // Validate PK
        new.clone()
            .select(
                self.primary_key
                    .iter()
                    .map(|name| col(Column::from_name(name)))
                    .collect(),
            )
            .int_err()?;

        // Add op column if does not exist
        let new = if new
            .schema()
            .has_column_with_unqualified_name(&self.vocab.operation_type_column)
        {
            new
        } else {
            new.with_column(&self.vocab.operation_type_column, lit(Op::Append as i32))
                .int_err()?
        };

        Ok(new)
    }

    /// Create an empty prev dataset based on input schema
    fn empty_prev(&self, new: &DataFrameExt) -> Result<DataFrameExt, DataFusionErrorWrapped> {
        let mut fields: Vec<FieldRef> = new.schema().fields().iter().cloned().collect();
        fields.push(Field::new(&self.vocab.offset_column, DataType::Int64, false).into());

        let schema = DFSchema::from_unqualified_fields(Fields::from(fields), Default::default())?;

        let state = new.clone().into_parts().0;
        let plan = datafusion::logical_expr::LogicalPlan::EmptyRelation(
            datafusion::logical_expr::EmptyRelation {
                produce_one_row: false,
                schema: datafusion::common::DFSchemaRef::new(schema),
            },
        );

        Ok(DataFrame::new(state, plan).into())
    }
}

// TODO: Validate op codes
// TODO: Handle in-batch changes?
// TODO: Return BadInput error instead of internal errors
// TODO: Configure whether to tolerate retractions for non-observed records
impl MergeStrategy for MergeStrategyUpsertStream {
    fn merge(
        &self,
        prev: Option<DataFrameExt>,
        new: DataFrameExt,
    ) -> Result<DataFrameExt, MergeError> {
        let new = self.normalize_input(new)?;

        let prev = if let Some(prev) = prev {
            prev
        } else {
            self.empty_prev(&new)?
        };

        let proj = self.latest_by_pk(prev)?;

        let df = self.upsert_to_changelog_stream(proj, new)?;

        Ok(df)
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
