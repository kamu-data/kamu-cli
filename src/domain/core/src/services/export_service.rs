// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use internal_error::InternalError;
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ExportService: Send + Sync {
    async fn export_to_fs(
        &self,
        sql_query: &String,
        path: &String,
        format: ExportFormat,
        partition_row_count: Option<usize>,
    ) -> Result<u64, InternalError>; // todo switch to specific ExportService errors
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ExportFormat {
    Parquet,
    Csv,
    NdJson,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
