// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use kamu_core::auth::{
    self,
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetActionNotEnoughPermissionsError,
    DatasetActionUnauthorizedError,
};
use kamu_core::AccessError;
use mockall::predicate::{always, eq, function};
use mockall::Predicate;
use opendatafabric::{DatasetAlias, DatasetHandle};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub DatasetActionAuthorizer {}
    #[async_trait::async_trait]
    impl DatasetActionAuthorizer for DatasetActionAuthorizer {
        async fn check_action_allowed(
            &self,
            dataset_handle: &DatasetHandle,
            action: DatasetAction,
        ) -> Result<(), DatasetActionUnauthorizedError>;

        async fn get_allowed_actions(&self, dataset_handle: &DatasetHandle) -> HashSet<DatasetAction>;
    }
}

impl MockDatasetActionAuthorizer {
    pub fn denying_error(
        dataset_handle: &DatasetHandle,
        action: DatasetAction,
    ) -> DatasetActionUnauthorizedError {
        DatasetActionUnauthorizedError::Access(AccessError::Forbidden(
            DatasetActionNotEnoughPermissionsError {
                action,
                dataset_ref: dataset_handle.as_local_ref(),
            }
            .into(),
        ))
    }

    pub fn denying() -> Self {
        let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
        mock_dataset_action_authorizer
            .expect_check_action_allowed()
            .returning(|dataset_handle, action| Err(Self::denying_error(dataset_handle, action)));
        mock_dataset_action_authorizer
    }

    pub fn allowing() -> Self {
        let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
        mock_dataset_action_authorizer
            .expect_check_action_allowed()
            .returning(|_, _| Ok(()));
        mock_dataset_action_authorizer
    }

    pub fn expect_check_read_dataset(
        self,
        dataset_alias: &DatasetAlias,
        times: usize,
        success: bool,
    ) -> Self {
        let dataset_alias = dataset_alias.clone();
        self.expect_check_action_allowed_internal(
            function(move |dh: &DatasetHandle| dh.alias == dataset_alias),
            DatasetAction::Read,
            times,
            success,
        )
    }

    pub fn expect_check_write_dataset(
        self,
        dataset_alias: &DatasetAlias,
        times: usize,
        success: bool,
    ) -> Self {
        let dataset_alias = dataset_alias.clone();
        self.expect_check_action_allowed_internal(
            function(move |dh: &DatasetHandle| dh.alias == dataset_alias),
            DatasetAction::Write,
            times,
            success,
        )
    }

    pub fn expect_check_read_a_dataset(self, times: usize, success: bool) -> Self {
        self.expect_check_action_allowed_internal(always(), DatasetAction::Read, times, success)
    }

    pub fn expect_check_write_a_dataset(self, times: usize, success: bool) -> Self {
        self.expect_check_action_allowed_internal(always(), DatasetAction::Write, times, success)
    }

    fn expect_check_action_allowed_internal<P>(
        mut self,
        dataset_handle_predicate: P,
        action: auth::DatasetAction,
        times: usize,
        success: bool,
    ) -> Self
    where
        P: Predicate<DatasetHandle> + Sync + Send + 'static,
    {
        if times > 0 {
            self.expect_check_action_allowed()
                .with(dataset_handle_predicate, eq(action))
                .times(times)
                .returning(move |hdl, action| {
                    if success {
                        Ok(())
                    } else {
                        Err(Self::denying_error(hdl, action))
                    }
                });
        } else {
            self.expect_check_action_allowed()
                .with(dataset_handle_predicate, eq(action))
                .never();
        }

        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
