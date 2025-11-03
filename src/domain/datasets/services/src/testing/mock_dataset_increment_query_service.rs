// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::{DatasetIncrementQueryService, GetIncrementError};
use odf::dataset::MetadataChainIncrementInterval;

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
        ) -> Result<MetadataChainIncrementInterval, GetIncrementError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockDatasetIncrementQueryService {
    pub fn with_increment_between(increment: MetadataChainIncrementInterval) -> Self {
        let mut dataset_changes_mock = MockDatasetIncrementQueryService::default();
        dataset_changes_mock
            .expect_get_increment_between()
            .returning(move |_, _, _| Ok(increment));
        dataset_changes_mock
    }

    pub fn with_increment_between_for_args(
        dataset_id: odf::DatasetID,
        old_head: Option<odf::Multihash>,
        new_head: odf::Multihash,
        increment: MetadataChainIncrementInterval,
    ) -> Self {
        let mut dataset_changes_mock = MockDatasetIncrementQueryService::default();
        dataset_changes_mock
            .expect_get_increment_between()
            .withf(move |id, old, new| {
                *id == dataset_id && *old == old_head.as_ref() && *new == new_head
            })
            .returning(move |_, _, _| Ok(increment));
        dataset_changes_mock
    }

    pub fn with_increment_error_invalid_interval(
        dataset_id: odf::DatasetID,
        old_head: odf::Multihash,
        new_head: odf::Multihash,
    ) -> Self {
        let new_head_2 = new_head.clone();
        let old_head_2 = old_head.clone();

        let mut dataset_changes_mock = MockDatasetIncrementQueryService::default();
        dataset_changes_mock
            .expect_get_increment_between()
            .withf(move |id, old, new| {
                *id == dataset_id && *old == Some(&old_head_2) && *new == new_head_2
            })
            .returning(move |_, _, _| {
                Err(GetIncrementError::InvalidInterval(
                    odf::dataset::InvalidIntervalError {
                        head: new_head.clone(),
                        tail: old_head.clone(),
                    },
                ))
            });
        dataset_changes_mock
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
