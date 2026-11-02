// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use internal_error::InternalError;

use crate::{DatasetBlock, DatasetBlocksFilter};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Queries indexed metadata blocks of a dataset, both key and data blocks
#[async_trait::async_trait]
pub trait DatasetBlockQueryRepository: Send + Sync {
    /// Lists blocks matching the filter, newest first (by sequence number)
    async fn list_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        filter: &DatasetBlocksFilter,
        pagination: PaginationOpts,
    ) -> Result<Vec<DatasetBlock>, InternalError>;

    /// Counts blocks matching the filter
    async fn count_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        filter: &DatasetBlocksFilter,
    ) -> Result<usize, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
