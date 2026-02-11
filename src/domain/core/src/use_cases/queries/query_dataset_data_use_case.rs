// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{GetChangelogProjectionOptions, GetDataOptions, GetDataResponse, QueryError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait QueryDatasetDataUseCase: Send + Sync {
    async fn tail(
        &self,
        dataset_ref: &odf::DatasetRef,
        skip: u64,
        limit: u64,
        options: GetDataOptions,
    ) -> Result<GetDataResponse, QueryError>;

    // TODO: Introduce additional options that could be used to narrow down the
    //       number of files we collect to construct the dataframe.
    ///
    /// Returns a `DataFrame` representing the contents of an entire dataset
    async fn get_data(
        &self,
        dataset_ref: &odf::DatasetRef,
        options: GetDataOptions,
    ) -> Result<GetDataResponse, QueryError>;

    // TODO: Consider replacing this function with a more sophisticated session
    //       context builder that can be reused for multiple queries
    /// Returns [`DataFrameExt`]s representing the contents of multiple datasets
    /// in a batch
    async fn get_data_multi(
        &self,
        dataset_refs: &[odf::DatasetRef],
        skip_if_missing_or_inaccessible: bool,
    ) -> Result<Vec<GetDataResponse>, QueryError>;

    // TODO: Consider replacing this function with a more sophisticated session
    //       context builder that can be reused for multiple queries
    /// Projects the CDC ledger into a state snapshot.
    /// Uses [`odf::utils::data::changelog::project`] function internally.
    async fn get_changelog_projection(
        &self,
        dataset_ref: &odf::DatasetRef,
        options: GetChangelogProjectionOptions,
    ) -> Result<GetDataResponse, QueryError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
