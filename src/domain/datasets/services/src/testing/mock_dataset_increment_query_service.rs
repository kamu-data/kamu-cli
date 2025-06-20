// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::{DatasetIncrementQueryService, DatasetIntervalIncrement, GetIncrementError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub DatasetIncrementQueryService {}

    #[async_trait::async_trait]
    impl DatasetIncrementQueryService for DatasetIncrementQueryService {
        #[allow(clippy::ref_option_ref)]
        async fn get_increment_between<'a>(
            &'a self,
            dataset_id: &'a odf::DatasetID,
            old_head: Option<&'a odf::Multihash>,
            new_head: &'a odf::Multihash,
        ) -> Result<DatasetIntervalIncrement, GetIncrementError>;

        #[allow(clippy::ref_option_ref)]
        async fn get_increment_since<'a>(
            &'a self,
            dataset_id: &'a odf::DatasetID,
            old_head: Option<&'a odf::Multihash>,
        ) -> Result<DatasetIntervalIncrement, GetIncrementError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockDatasetIncrementQueryService {
    pub fn with_increment_between(increment: DatasetIntervalIncrement) -> Self {
        let mut dataset_changes_mock = MockDatasetIncrementQueryService::default();
        dataset_changes_mock
            .expect_get_increment_between()
            .returning(move |_, _, _| Ok(increment));
        dataset_changes_mock
    }

    pub fn with_increment_since(increment: DatasetIntervalIncrement) -> Self {
        let mut dataset_changes_mock = MockDatasetIncrementQueryService::default();
        dataset_changes_mock
            .expect_get_increment_since()
            .returning(move |_, _| Ok(increment));
        dataset_changes_mock
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
