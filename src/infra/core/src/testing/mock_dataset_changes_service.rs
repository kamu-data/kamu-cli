// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{DatasetChangesService, DatasetIntervalIncrement, GetIncrementError};
use opendatafabric::{DatasetID, Multihash};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub DatasetChangesService {}
    #[async_trait::async_trait]
    impl DatasetChangesService for DatasetChangesService {
        #[allow(clippy::ref_option_ref)]
        async fn get_increment_between<'a>(
            &'a self,
            dataset_id: &'a DatasetID,
            old_head: Option<&'a Multihash>,
            new_head: &'a Multihash,
        ) -> Result<DatasetIntervalIncrement, GetIncrementError>;

        #[allow(clippy::ref_option_ref)]
        async fn get_increment_since<'a>(
            &'a self,
            dataset_id: &'a DatasetID,
            old_head: Option<&'a Multihash>,
        ) -> Result<DatasetIntervalIncrement, GetIncrementError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockDatasetChangesService {
    pub fn with_increment_between(increment: DatasetIntervalIncrement) -> Self {
        let mut dataset_changes_mock = MockDatasetChangesService::default();
        dataset_changes_mock
            .expect_get_increment_between()
            .returning(move |_, _, _| Ok(increment));
        dataset_changes_mock
    }

    pub fn with_increment_since(increment: DatasetIntervalIncrement) -> Self {
        let mut dataset_changes_mock = MockDatasetChangesService::default();
        dataset_changes_mock
            .expect_get_increment_since()
            .returning(move |_, _| Ok(increment));
        dataset_changes_mock
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
