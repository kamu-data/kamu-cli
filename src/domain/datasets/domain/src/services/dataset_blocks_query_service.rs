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

use crate::{DatasetBlocksFilter, ResolvedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetBlocksQueryService: Send + Sync {
    /// Lists blocks reachable from the head of the dataset that match the
    /// filter, newest first (by sequence number), with the total number of
    /// matching blocks
    async fn list_blocks(
        &self,
        dataset: &ResolvedDataset,
        filter: &DatasetBlocksFilter,
        pagination: PaginationOpts,
    ) -> Result<DatasetBlocksListing, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DatasetBlocksListing {
    pub blocks: Vec<(odf::Multihash, odf::MetadataBlock)>,
    pub total_count: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
