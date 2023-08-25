// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use datafusion::error::Result;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::*;

#[async_trait::async_trait]
pub trait DataFrameExt
where
    Self: Sized,
{
    fn columns_to_front(self, front_cols: &[&str]) -> Result<Self>;
    fn without_columns(self, cols: &[&str]) -> Result<Self>;
    async fn write_parquet_single_file(
        self,
        path: &Path,
        writer_properties: Option<WriterProperties>,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl DataFrameExt for DataFrame {
    fn columns_to_front(self, front_cols: &[&str]) -> Result<Self> {
        let mut columns: Vec<_> = front_cols.into_iter().map(|s| col(*s)).collect();

        columns.extend(
            self.schema()
                .fields()
                .iter()
                .filter(|f| !front_cols.contains(&f.name().as_str()))
                .map(|f| col(f.unqualified_column())),
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
            .map(|f| col(f.unqualified_column()))
            .collect();

        self.select(columns)
    }

    async fn write_parquet_single_file(
        self,
        path: &Path,
        writer_properties: Option<WriterProperties>,
    ) -> Result<()> {
        tracing::info!(?path, "Writing result to parquet");

        // Reprartition to only produce a single file
        let df = self.repartition(Partitioning::RoundRobinBatch(1))?;

        // Produces a directory of "part-X.parquet" files
        df.write_parquet(path.as_os_str().to_str().unwrap(), writer_properties)
            .await?;

        // Ensure only produced one file
        assert_eq!(
            1,
            path.read_dir().unwrap().into_iter().count(),
            "write_parquet produced more than one file"
        );

        let tmp_path = path.with_extension(format!(
            "{}tmp",
            path.extension()
                .unwrap_or_default()
                .to_str()
                .unwrap_or_default()
        ));
        std::fs::rename(path.join("part-0.parquet"), &tmp_path)?;
        std::fs::remove_dir(path)?;
        std::fs::rename(tmp_path, path)?;
        Ok(())
    }
}
