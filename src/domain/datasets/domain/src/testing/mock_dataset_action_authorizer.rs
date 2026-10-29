// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use mockall::Predicate;
use mockall::predicate::{always, eq, function};

use crate::{
    ClassifyByAllowanceDatasetActionUnauthorizedError,
    ClassifyByAllowanceIdsResponse,
    ClassifyByAllowanceResponse,
    DatasetAction,
    DatasetActionUnauthorizedError,
    MockDatasetActionAuthorizer,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        let missing_ids_for = missing_ids.clone();
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
                                    ClassifyByAllowanceDatasetActionUnauthorizedError::not_enough_permissions(
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
                                    ClassifyByAllowanceDatasetActionUnauthorizedError::not_enough_permissions(
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
            .expect_classify_dataset_handles_by_allowance_for()
            .returning(move |_, handles, action| {
                Ok(Self::classify_handles(handles, |hdl| {
                    Some(if missing_ids_for.contains(&hdl.id) {
                        odf::DatasetNotFoundError::new(hdl.id.as_local_ref()).into()
                    } else {
                        ClassifyByAllowanceDatasetActionUnauthorizedError::not_enough_permissions(
                            hdl.id.as_local_ref(),
                            action,
                        )
                    })
                }))
            });
        mock_dataset_action_authorizer
    }

    /// Allows every action on every dataset except the specified ones
    pub fn denying_ids(denied_ids: HashSet<odf::DatasetID>) -> Self {
        let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();

        let denied = denied_ids.clone();
        mock_dataset_action_authorizer
            .expect_check_action_allowed()
            .returning(move |dataset_id, action| {
                if denied.contains(dataset_id) {
                    Err(DatasetActionUnauthorizedError::not_enough_permissions(
                        dataset_id.as_local_ref(),
                        action,
                    ))
                } else {
                    Ok(())
                }
            });

        let denied = denied_ids.clone();
        mock_dataset_action_authorizer
            .expect_classify_dataset_ids_by_allowance()
            .returning(move |ids, action| {
                let (unauthorized, authorized): (Vec<_>, Vec<_>) =
                    ids.iter().partition(|id| denied.contains(id.as_ref()));
                Ok(ClassifyByAllowanceIdsResponse {
                    authorized_ids: authorized.into_iter().map(|id| id.as_ref().clone()).collect(),
                    unauthorized_ids_with_errors: unauthorized
                        .into_iter()
                        .map(|id| {
                            (
                                id.as_ref().clone(),
                                ClassifyByAllowanceDatasetActionUnauthorizedError::not_enough_permissions(
                                    id.as_local_ref(),
                                    action,
                                ),
                            )
                        })
                        .collect(),
                })
            });

        let denied = denied_ids.clone();
        mock_dataset_action_authorizer
            .expect_classify_dataset_handles_by_allowance()
            .returning(move |handles, action| {
                let (unauthorized, authorized): (Vec<_>, Vec<_>) = handles
                    .into_iter()
                    .partition(|hdl| denied.contains(&hdl.id));
                Ok(ClassifyByAllowanceResponse {
                    authorized_handles: authorized,
                    unauthorized_handles_with_errors: unauthorized
                        .into_iter()
                        .map(|hdl| {
                            let err =
                                ClassifyByAllowanceDatasetActionUnauthorizedError::not_enough_permissions(
                                    hdl.id.as_local_ref(),
                                    action,
                                );
                            (hdl, err)
                        })
                        .collect(),
                })
            });

        let denied = denied_ids.clone();
        mock_dataset_action_authorizer
            .expect_classify_dataset_handles_by_allowance_for()
            .returning(move |_, handles, action| {
                Ok(Self::classify_handles(handles, |hdl| {
                    denied.contains(&hdl.id).then(|| {
                        ClassifyByAllowanceDatasetActionUnauthorizedError::not_enough_permissions(
                            hdl.id.as_local_ref(),
                            action,
                        )
                    })
                }))
            });

        mock_dataset_action_authorizer
            .expect_filter_datasets_allowing()
            .returning(move |handles, _| {
                Ok(handles
                    .into_iter()
                    .filter(|hdl| !denied_ids.contains(&hdl.id))
                    .collect())
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
            .expect_classify_dataset_handles_by_allowance_for()
            .returning(|_, handles, _| Ok(Self::classify_handles(handles, |_| None)));
        mock_dataset_action_authorizer
            .expect_filter_datasets_allowing()
            .returning(|handles, _| Ok(handles));
        mock_dataset_action_authorizer
    }

    pub fn expect_check_read_dataset(
        self,
        expected_dataset_id: &odf::DatasetID,
        times: usize,
        success: bool,
    ) -> Self {
        let expected_dataset_id = expected_dataset_id.clone();
        self.expect_check_action_allowed_unauthorized_internal(
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
        self.expect_check_action_allowed_unauthorized_internal(
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
        self.expect_check_action_allowed_unauthorized_internal(
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
        self.expect_check_action_allowed_unauthorized_internal(
            function(move |dataset_id| *dataset_id == expected_dataset_id),
            DatasetAction::Own,
            times,
            success,
        )
    }

    pub fn expect_check_read_a_dataset(self, times: usize, success: bool) -> Self {
        self.expect_check_action_allowed_unauthorized_internal(
            always(),
            DatasetAction::Read,
            times,
            success,
        )
    }

    pub fn expect_check_write_a_dataset(self, times: usize, success: bool) -> Self {
        self.expect_check_action_allowed_unauthorized_internal(
            always(),
            DatasetAction::Write,
            times,
            success,
        )
    }

    pub fn expect_check_action_allowed_unauthorized_internal<P>(
        self,
        dataset_id_predicate: P,
        action: DatasetAction,
        times: usize,
        success: bool,
    ) -> Self
    where
        P: Predicate<odf::DatasetID> + Sync + Send + 'static,
    {
        self.expect_check_action_allowed_internal(
            dataset_id_predicate,
            |dataset_id, action| {
                DatasetActionUnauthorizedError::not_enough_permissions(
                    dataset_id.as_local_ref(),
                    action,
                )
            },
            action,
            times,
            success,
        )
    }

    fn expect_check_action_allowed_internal<P, E>(
        mut self,
        dataset_id_predicate: P,
        error_callback: E,
        action: DatasetAction,
        times: usize,
        success: bool,
    ) -> Self
    where
        P: Predicate<odf::DatasetID> + Sync + Send + 'static,
        E: Fn(&odf::DatasetID, DatasetAction) -> DatasetActionUnauthorizedError
            + Sync
            + Send
            + 'static,
    {
        if times > 0 {
            self.expect_check_action_allowed()
                .with(dataset_id_predicate, eq(action))
                .times(times)
                .returning(move |dataset_id, action| {
                    if success {
                        Ok(())
                    } else {
                        Err(error_callback(dataset_id, action))
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
                        let error = ClassifyByAllowanceDatasetActionUnauthorizedError::not_enough_permissions(
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
                                ClassifyByAllowanceDatasetActionUnauthorizedError::not_enough_permissions(
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

    /// Expects the principal-explicit classification of `action` to include
    /// the given dataset; other datasets in the same call are authorized
    pub fn expect_authorize_for(
        self,
        action: DatasetAction,
        expected_dataset_id: &odf::DatasetID,
        times: usize,
        success: bool,
    ) -> Self {
        self.expect_authorize_for_internal(action, expected_dataset_id, times, move |hdl| {
            (!success).then(|| {
                ClassifyByAllowanceDatasetActionUnauthorizedError::not_enough_permissions(
                    hdl.as_local_ref(),
                    action,
                )
            })
        })
    }

    /// Like [`Self::expect_authorize_for`], answering that the dataset is not
    /// found
    pub fn expect_authorize_for_not_found(
        self,
        action: DatasetAction,
        expected_dataset_id: &odf::DatasetID,
        times: usize,
    ) -> Self {
        self.expect_authorize_for_internal(action, expected_dataset_id, times, |hdl| {
            Some(odf::DatasetNotFoundError::new(hdl.as_local_ref()).into())
        })
    }

    fn expect_authorize_for_internal<E>(
        mut self,
        action: DatasetAction,
        expected_dataset_id: &odf::DatasetID,
        times: usize,
        error_callback: E,
    ) -> Self
    where
        E: Fn(&odf::DatasetHandle) -> Option<ClassifyByAllowanceDatasetActionUnauthorizedError>
            + Sync
            + Send
            + 'static,
    {
        let matched_id = expected_dataset_id.clone();
        let answered_id = expected_dataset_id.clone();
        let expectation = self
            .expect_classify_dataset_handles_by_allowance_for()
            .withf(move |_, handles, a| {
                *a == action && handles.iter().any(|hdl| hdl.id == matched_id)
            });

        if times > 0 {
            expectation.times(times).returning(move |_, handles, _| {
                Ok(Self::classify_handles(handles, |hdl| {
                    if hdl.id == answered_id {
                        error_callback(hdl)
                    } else {
                        None
                    }
                }))
            });
        } else {
            expectation.never();
        }

        self
    }

    /// Principal-explicit counterpart of
    /// [`Self::make_expect_classify_datasets_by_allowance`]
    pub fn make_expect_classify_datasets_by_allowance_for(
        mut self,
        action: DatasetAction,
        times: usize,
        authorized: HashSet<odf::DatasetAlias>,
    ) -> Self {
        self.expect_classify_dataset_handles_by_allowance_for()
            .withf(move |_, _, a| *a == action)
            .times(times)
            .returning(move |_, handles, action| {
                Ok(Self::classify_handles(handles, |hdl| {
                    (!authorized.contains(&hdl.alias)).then(|| {
                        ClassifyByAllowanceDatasetActionUnauthorizedError::not_enough_permissions(
                            hdl.as_local_ref(),
                            action,
                        )
                    })
                }))
            });

        self
    }

    fn classify_handles(
        handles: Vec<odf::DatasetHandle>,
        error_for: impl Fn(
            &odf::DatasetHandle,
        ) -> Option<ClassifyByAllowanceDatasetActionUnauthorizedError>,
    ) -> ClassifyByAllowanceResponse {
        let mut response = ClassifyByAllowanceResponse {
            authorized_handles: Vec::new(),
            unauthorized_handles_with_errors: Vec::new(),
        };
        for hdl in handles {
            match error_for(&hdl) {
                Some(error) => response.unauthorized_handles_with_errors.push((hdl, error)),
                None => response.authorized_handles.push(hdl),
            }
        }
        response
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
