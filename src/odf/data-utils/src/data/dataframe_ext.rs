// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use arrow::array::{Array, ArrowPrimitiveType, AsArray, RecordBatch};
use datafusion::catalog::TableProvider;
use datafusion::common::DFSchema;
use datafusion::config::{CsvOptions, JsonOptions, TableParquetOptions};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, SortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use tracing::Instrument as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Tracing and extensions wrapper for [`DataFrame`]
#[derive(Clone)]
pub struct DataFrameExt(DataFrame);

impl From<DataFrame> for DataFrameExt {
    fn from(value: DataFrame) -> Self {
        Self(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Wrapper methods
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataFrameExt {
    #[tracing::instrument(level = "info", name = "DataFrame::cache", skip_all)]
    pub async fn cache(self) -> Result<Self, DataFusionError> {
        self.0.cache().await.map(Self)
    }

    #[tracing::instrument(level = "info", name = "DataFrame::collect", skip_all)]
    pub async fn collect(self) -> Result<Vec<RecordBatch>, DataFusionError> {
        let task_ctx = Arc::new(self.0.task_ctx());

        let plan = self
            .0
            .create_physical_plan()
            .instrument(tracing::debug_span!("DataFrame::create_physical_plan").or_current())
            .await?;

        datafusion::physical_plan::collect(plan, task_ctx)
            .instrument(tracing::debug_span!("PhysicalPlan::collect").or_current())
            .await
    }

    #[tracing::instrument(level = "info", name = "DataFrame::count", skip_all)]
    pub async fn count(self) -> Result<usize, DataFusionError> {
        self.0.count().await
    }

    #[tracing::instrument(level = "info", name = "DataFrame::create_physical_plan", skip_all)]
    pub async fn create_physical_plan(self) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.0.create_physical_plan().await
    }

    pub fn distinct(self) -> Result<Self, DataFusionError> {
        self.0.distinct().map(Self)
    }

    pub fn filter(self, predicate: Expr) -> Result<Self, DataFusionError> {
        self.0.filter(predicate).map(Self)
    }

    pub fn into_inner(self) -> DataFrame {
        self.0
    }

    pub fn into_parts(self) -> (SessionState, LogicalPlan) {
        self.0.into_parts()
    }

    pub fn into_view(self) -> Arc<dyn TableProvider> {
        self.0.into_view()
    }

    pub fn join(
        self,
        right: DataFrameExt,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
        filter: Option<Expr>,
    ) -> Result<Self, DataFusionError> {
        self.0
            .join(right.into_inner(), join_type, left_cols, right_cols, filter)
            .map(Self)
    }

    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<Self, DataFusionError> {
        self.0.limit(skip, fetch).map(Self)
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        self.0.logical_plan()
    }

    pub fn repartition(self, partitioning_scheme: Partitioning) -> Result<Self, DataFusionError> {
        self.0.repartition(partitioning_scheme).map(Self)
    }

    pub fn schema(&self) -> &DFSchema {
        self.0.schema()
    }

    pub fn select(self, expr_list: Vec<Expr>) -> Result<Self, DataFusionError> {
        self.0.select(expr_list).map(Self)
    }

    pub fn select_columns(self, columns: &[&str]) -> Result<Self, DataFusionError> {
        self.0.select_columns(columns).map(Self)
    }

    #[tracing::instrument(level = "info", name = "DataFrame::show", skip_all)]
    pub async fn show(self) -> Result<(), DataFusionError> {
        self.0.show().await
    }

    pub fn sort(self, expr: Vec<SortExpr>) -> Result<Self, DataFusionError> {
        self.0.sort(expr).map(Self)
    }

    pub fn with_column(self, name: &str, expr: Expr) -> Result<Self, DataFusionError> {
        self.0.with_column(name, expr).map(Self)
    }

    pub fn window(self, window_exprs: Vec<Expr>) -> Result<Self, DataFusionError> {
        self.0.window(window_exprs).map(Self)
    }

    #[tracing::instrument(level = "info", name = "DataFrame::write_csv", skip_all)]
    pub async fn write_csv(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_options: Option<CsvOptions>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        self.0.write_csv(path, options, writer_options).await
    }

    #[tracing::instrument(level = "info", name = "DataFrame::write_json", skip_all)]
    pub async fn write_json(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_options: Option<JsonOptions>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        self.0.write_json(path, options, writer_options).await
    }

    #[tracing::instrument(level = "info", name = "DataFrame::write_parquet", skip_all)]
    pub async fn write_parquet(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_options: Option<TableParquetOptions>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        self.0.write_parquet(path, options, writer_options).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Extension methods
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataFrameExt {
    pub fn columns_to_front(self, front_cols: &[&str]) -> Result<Self> {
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

    pub fn without_columns(self, cols: &[&str]) -> Result<Self> {
        let columns: Vec<_> = self
            .schema()
            .fields()
            .iter()
            .filter(|f| !cols.contains(&f.name().as_str()))
            .map(|f| col(Column::from_name(f.name())))
            .collect();

        self.select(columns)
    }

    /// Collects and serializes records into Json array-of-structures format
    pub async fn collect_json_aos(self) -> Result<Vec<serde_json::Value>> {
        use crate::data::format::{JsonArrayOfStructsWriter, RecordsWriter as _};

        let record_batches = self.collect().await?;

        let mut json = Vec::new();
        let mut writer = JsonArrayOfStructsWriter::new(&mut json);
        writer.write_batches(&record_batches).unwrap();
        writer.finish().unwrap();

        let serde_json::Value::Array(records) = serde_json::from_slice(&json).unwrap() else {
            unreachable!()
        };

        Ok(records)
    }

    /// Given a data frame with a zero or one row and one column extracts the
    /// typed scalar value
    pub async fn collect_scalar<T: ArrowPrimitiveType>(self) -> Result<Option<T::Native>> {
        let batches = self.collect().await?;
        if batches.is_empty() {
            return Ok(None);
        }
        if batches.len() > 1 || batches[0].num_rows() > 1 || batches[0].num_columns() != 1 {
            return Err(DataFusionError::Internal(format!(
                "collect_scalar expected 1x1 result shape but got {}x{}",
                batches[0].num_rows(),
                batches[0].num_columns()
            )));
        }

        let batch = batches.into_iter().next().unwrap();

        let Some(column) = batch.column(0).as_primitive_opt::<T>() else {
            return Err(DataFusionError::Internal(format!(
                "collect_scalar expected column type {} but got {}",
                T::DATA_TYPE,
                batch.column(0).data_type()
            )));
        };

        if column.is_null(0) {
            Ok(None)
        } else {
            Ok(Some(column.value(0)))
        }
    }

    pub fn union_all(dfs: Vec<DataFrameExt>) -> Result<Option<DataFrameExt>> {
        let mut iter = dfs.into_iter();
        let Some(df) = iter.next() else {
            return Ok(None);
        };

        let (state, plan) = df.into_parts();
        let mut builder = datafusion::logical_expr::LogicalPlanBuilder::new(plan);

        for df in iter {
            let (_, plan) = df.into_parts();
            builder = builder.union(plan)?;
        }

        let plan = builder.build()?;

        Ok(Some(Self(DataFrame::new(state, plan))))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Debug for DataFrameExt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
