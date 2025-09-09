// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashSet;

use internal_error::InternalError;
use kamu_core::auth::{
    ClassifyByAllowanceIdsResponse,
    ClassifyByAllowanceResponse,
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetActionUnauthorizedError,
    MultipleDatasetActionUnauthorizedError,
};
use mockall::Predicate;
use mockall::predicate::{always, eq, function};

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
            dataset_id: &odf::DatasetID,
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

        async fn classify_dataset_ids_by_allowance<'a>(
            &'a self,
            dataset_ids: &[Cow<'a, odf::DatasetID>],
            action: DatasetAction,
        ) -> Result<ClassifyByAllowanceIdsResponse, InternalError>;
    }
}

impl MockDatasetActionAuthorizer {
    pub fn denying() -> Self {
        Self::denying_with_not_found(HashSet::new())
    }

    pub fn denying_with_not_found(missing_ids: HashSet<odf::DatasetID>) -> Self {
        let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
        mock_dataset_action_authorizer
            .expect_check_action_allowed()
            .returning(|dataset_id, action| {
                Err(DatasetActionUnauthorizedError::not_enough_permissions(
                    dataset_id.as_local_ref(),
                    action,
                ))
            });
        let missing_ids_clone = missing_ids.clone();
        mock_dataset_action_authorizer
            .expect_classify_dataset_ids_by_allowance()
            .returning(move |ids, action| {
                Ok(ClassifyByAllowanceIdsResponse {
                    authorized_ids: Vec::new(),
                    unauthorized_ids_with_errors: ids
                        .iter()
                        .map(|id| {
                            if missing_ids_clone.contains(id.as_ref()) {
                                (
                                    id.as_ref().clone(),
                                    odf::DatasetNotFoundError::new(id.as_local_ref()).into(),
                                )
                            } else {
                                (
                                    id.as_ref().clone(),
                                    MultipleDatasetActionUnauthorizedError::not_enough_permissions(
                                        id.as_local_ref(),
                                        action,
                                    ),
                                )
                            }
                        })
                        .collect(),
                })
            });
        mock_dataset_action_authorizer
            .expect_classify_dataset_handles_by_allowance()
            .returning(move |handles, action| {
                Ok(ClassifyByAllowanceResponse {
                    authorized_handles: Vec::new(),
                    unauthorized_handles_with_errors: handles
                        .into_iter()
                        .map(|hdl| {
                            if missing_ids.contains(&hdl.id) {
                                (
                                    hdl.clone(),
                                    odf::DatasetNotFoundError::new(hdl.id.as_local_ref()).into(),
                                )
                            } else {
                                (
                                    hdl.clone(),
                                    MultipleDatasetActionUnauthorizedError::not_enough_permissions(
                                        hdl.id.as_local_ref(),
                                        action,
                                    ),
                                )
                            }
                        })
                        .collect(),
                })
            });
        mock_dataset_action_authorizer
    }

    pub fn allowing() -> Self {
        let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
        mock_dataset_action_authorizer
            .expect_check_action_allowed()
            .returning(|_, _| Ok(()));
        mock_dataset_action_authorizer
            .expect_classify_dataset_ids_by_allowance()
            .returning(|ids, _| {
                Ok(ClassifyByAllowanceIdsResponse {
                    authorized_ids: ids.iter().map(|id| id.as_ref().clone()).collect(),
                    unauthorized_ids_with_errors: Vec::new(),
                })
            });
        mock_dataset_action_authorizer
            .expect_classify_dataset_handles_by_allowance()
            .returning(|handles, _| {
                Ok(ClassifyByAllowanceResponse {
                    authorized_handles: handles,
                    unauthorized_handles_with_errors: Vec::new(),
                })
            });
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

    pub fn expect_check_maintain_dataset(
        self,
        expected_dataset_id: &odf::DatasetID,
        times: usize,
        success: bool,
    ) -> Self {
        let expected_dataset_id = expected_dataset_id.clone();
        self.expect_check_action_allowed_internal(
            function(move |dataset_id| *dataset_id == expected_dataset_id),
            DatasetAction::Maintain,
            times,
            success,
        )
    }

    pub fn expect_check_own_dataset(
        self,
        expected_dataset_id: &odf::DatasetID,
        times: usize,
        success: bool,
    ) -> Self {
        let expected_dataset_id = expected_dataset_id.clone();
        self.expect_check_action_allowed_internal(
            function(move |dataset_id| *dataset_id == expected_dataset_id),
            DatasetAction::Own,
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
        dataset_id_predicate: P,
        action: DatasetAction,
        times: usize,
        success: bool,
    ) -> Self
    where
        P: Predicate<odf::DatasetID> + Sync + Send + 'static,
    {
        if times > 0 {
            self.expect_check_action_allowed()
                .with(dataset_id_predicate, eq(action))
                .times(times)
                .returning(move |dataset_id, action| {
                    if success {
                        Ok(())
                    } else {
                        Err(DatasetActionUnauthorizedError::not_enough_permissions(
                            dataset_id.as_local_ref(),
                            action,
                        ))
                    }
                });
        } else {
            self.expect_check_action_allowed()
                .with(dataset_id_predicate, eq(action))
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
                        let error = MultipleDatasetActionUnauthorizedError::not_enough_permissions(
                            handle.as_local_ref(),
                            action,
                        );
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

    pub fn make_expect_classify_dataset_ids_by_allowance(
        mut self,
        action: DatasetAction,
        times: usize,
        authorized: HashSet<odf::DatasetID>,
    ) -> Self {
        self.expect_classify_dataset_ids_by_allowance()
            .with(always(), eq(action))
            .times(times)
            .returning(move |dataset_ids, action| {
                let res = dataset_ids.iter().fold(
                    ClassifyByAllowanceIdsResponse {
                        authorized_ids: Vec::new(),
                        unauthorized_ids_with_errors: Vec::new(),
                    },
                    |mut acc, dataset_id| {
                        if authorized.contains(dataset_id) {
                            acc.authorized_ids.push(dataset_id.as_ref().clone());
                        } else {
                            let error =
                                MultipleDatasetActionUnauthorizedError::not_enough_permissions(
                                    dataset_id.as_local_ref(),
                                    action,
                                );
                            acc.unauthorized_ids_with_errors
                                .push((dataset_id.as_ref().clone(), error));
                        }
                        acc
                    },
                );
                Ok(res)
            });

        self
    }

    pub fn make_expect_get_allowed_actions(
        mut self,
        allowed_actions: &[DatasetAction],
        times: usize,
    ) -> Self {
        let res = allowed_actions.iter().copied().collect::<HashSet<_>>();
        self.expect_get_allowed_actions()
            .times(times)
            .returning(move |_| Ok(res.clone()));
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
