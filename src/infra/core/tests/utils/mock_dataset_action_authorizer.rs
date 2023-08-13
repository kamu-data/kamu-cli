// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::auth::{
    self,
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetActionNotEnoughPermissionsError,
    DatasetActionUnauthorizedError,
};
use kamu_core::{AccessError, TEST_ACCOUNT_NAME};
use mockall::predicate::{always, eq, function};
use mockall::Predicate;
use opendatafabric::{AccountName, DatasetAlias, DatasetHandle};

/////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub DatasetActionAuthorizer {}
    #[async_trait::async_trait]
    impl DatasetActionAuthorizer for DatasetActionAuthorizer {
        async fn check_action_allowed(
            &self,
            dataset_handle: &DatasetHandle,
            action: DatasetAction,
        ) -> Result<(), DatasetActionUnauthorizedError>;
    }
}

impl MockDatasetActionAuthorizer {
    pub fn denying() -> Self {
        let account_name = AccountName::new_unchecked(TEST_ACCOUNT_NAME);
        let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
        mock_dataset_action_authorizer
            .expect_check_action_allowed()
            .return_once(|dataset_handle, action| {
                Err(DatasetActionUnauthorizedError::Access(
                    AccessError::Forbidden(
                        DatasetActionNotEnoughPermissionsError {
                            account_name,
                            action,
                            dataset_ref: dataset_handle.as_local_ref(),
                        }
                        .into(),
                    ),
                ))
            });
        mock_dataset_action_authorizer
    }

    pub fn expect_check_read_dataset(self, dataset_alias: DatasetAlias, times: usize) -> Self {
        self.expect_check_action_allowed_internal(
            function(move |dh: &DatasetHandle| dh.alias == dataset_alias),
            DatasetAction::Read,
            times,
        )
    }

    pub fn expect_check_write_dataset(self, dataset_alias: DatasetAlias, times: usize) -> Self {
        self.expect_check_action_allowed_internal(
            function(move |dh: &DatasetHandle| dh.alias == dataset_alias),
            DatasetAction::Write,
            times,
        )
    }

    pub fn expect_check_read_a_dataset(self, times: usize) -> Self {
        self.expect_check_action_allowed_internal(always(), DatasetAction::Read, times)
    }

    pub fn expect_check_write_a_dataset(self, times: usize) -> Self {
        self.expect_check_action_allowed_internal(always(), DatasetAction::Write, times)
    }

    fn expect_check_action_allowed_internal<P>(
        mut self,
        dataset_handle_predicate: P,
        action: auth::DatasetAction,
        times: usize,
    ) -> Self
    where
        P: Predicate<DatasetHandle> + Sync + Send + 'static,
    {
        self.expect_check_action_allowed()
            .with(dataset_handle_predicate, eq(action))
            .times(times)
            .returning(|_, _| Ok(()));

        self
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
