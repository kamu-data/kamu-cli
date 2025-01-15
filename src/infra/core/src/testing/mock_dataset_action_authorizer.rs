// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use internal_error::InternalError;
use kamu_core::auth::{
    ClassifyByAllowanceIdsResponse,
    ClassifyByAllowanceResponse,
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetActionNotEnoughPermissionsError,
    DatasetActionUnauthorizedError,
};
use kamu_core::AccessError;
use mockall::predicate::{always, eq, function};
use mockall::Predicate;
use opendatafabric as odf;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub DatasetActionAuthorizer {}

    #[async_trait::async_trait]
    impl DatasetActionAuthorizer for DatasetActionAuthorizer {
        async fn check_action_allowed(
            &self,
            dataset_id: &odf::DatasetID,
            action: DatasetAction,
        ) -> Result<(), DatasetActionUnauthorizedError>;

        async fn get_allowed_actions(
            &self,
            dataset_handle: &odf::DatasetHandle,
        ) -> Result<HashSet<DatasetAction>, InternalError>;

        async fn filter_datasets_allowing(
            &self,
            dataset_handles: Vec<odf::DatasetHandle>,
            action: DatasetAction,
        ) -> Result<Vec<odf::DatasetHandle>, InternalError>;

        async fn classify_dataset_handles_by_allowance(
            &self,
            dataset_handles: Vec<odf::DatasetHandle>,
            action: DatasetAction,
        ) -> Result<ClassifyByAllowanceResponse, InternalError>;

        async fn classify_dataset_ids_by_allowance(
            &self,
            dataset_ids: Vec<odf::DatasetID>,
            action: DatasetAction,
        ) -> Result<ClassifyByAllowanceIdsResponse, InternalError>;
    }
}

impl MockDatasetActionAuthorizer {
    pub fn denying_error(
        dataset_ref: odf::DatasetRef,
        action: DatasetAction,
    ) -> DatasetActionUnauthorizedError {
        DatasetActionUnauthorizedError::Access(AccessError::Forbidden(
            DatasetActionNotEnoughPermissionsError {
                action,
                dataset_ref,
            }
            .into(),
        ))
    }

    pub fn denying() -> Self {
        let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
        mock_dataset_action_authorizer
            .expect_check_action_allowed()
            .returning(|dataset_id, action| {
                Err(Self::denying_error(dataset_id.as_local_ref(), action))
            });
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
        expected_dataset_id: &odf::DatasetID,
        times: usize,
        success: bool,
    ) -> Self {
        let expected_dataset_id = expected_dataset_id.clone();
        self.expect_check_action_allowed_internal(
            function(move |dataset_id| *dataset_id == expected_dataset_id),
            DatasetAction::Read,
            times,
            success,
        )
    }

    pub fn expect_check_write_dataset(
        self,
        expected_dataset_id: &odf::DatasetID,
        times: usize,
        success: bool,
    ) -> Self {
        let expected_dataset_id = expected_dataset_id.clone();
        self.expect_check_action_allowed_internal(
            function(move |dataset_id| *dataset_id == expected_dataset_id),
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
        action: DatasetAction,
        times: usize,
        success: bool,
    ) -> Self
    where
        P: Predicate<odf::DatasetID> + Sync + Send + 'static,
    {
        if times > 0 {
            self.expect_check_action_allowed()
                .with(dataset_handle_predicate, eq(action))
                .times(times)
                .returning(move |dataset_id, action| {
                    if success {
                        Ok(())
                    } else {
                        Err(Self::denying_error(dataset_id.as_local_ref(), action))
                    }
                });
        } else {
            self.expect_check_action_allowed()
                .with(dataset_handle_predicate, eq(action))
                .never();
        }

        self
    }

    pub fn make_expect_classify_datasets_by_allowance(
        mut self,
        action: DatasetAction,
        times: usize,
        authorized: HashSet<odf::DatasetAlias>,
    ) -> Self {
        self.expect_classify_dataset_handles_by_allowance()
            .with(always(), eq(action))
            .times(times)
            .returning(move |handles, action| {
                let mut good = Vec::new();
                let mut bad = Vec::new();

                for handle in handles {
                    if authorized.contains(&handle.alias) {
                        good.push(handle);
                    } else {
                        let error = Self::denying_error(handle.as_local_ref(), action);
                        bad.push((handle, error));
                    }
                }

                Ok(ClassifyByAllowanceResponse {
                    authorized_handles: good,
                    unauthorized_handles_with_errors: bad,
                })
            });

        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
