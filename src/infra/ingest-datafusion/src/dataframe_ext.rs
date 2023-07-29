// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::prelude::*;

pub trait DataFrameExt
where
    Self: Sized,
{
    fn columns_to_front(self, front_cols: &[&str]) -> datafusion::error::Result<Self>;
    fn without_columns(self, cols: &[&str]) -> datafusion::error::Result<Self>;
}

impl DataFrameExt for DataFrame {
    fn columns_to_front(self, front_cols: &[&str]) -> datafusion::error::Result<Self> {
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

    fn without_columns(self, cols: &[&str]) -> datafusion::error::Result<Self> {
        let columns: Vec<_> = self
            .schema()
            .fields()
            .iter()
            .filter(|f| !cols.contains(&f.name().as_str()))
            .map(|f| col(f.unqualified_column()))
            .collect();

        self.select(columns)
    }
}
