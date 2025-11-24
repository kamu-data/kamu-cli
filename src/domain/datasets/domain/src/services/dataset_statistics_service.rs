// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{DatasetStatistics, TotalStatistic};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetStatisticsService: Sync + Send {
    async fn get_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<DatasetStatistics, InternalError>;

    async fn get_total_statistic_by_account_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<TotalStatistic, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
