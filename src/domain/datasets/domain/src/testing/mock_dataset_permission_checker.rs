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
    ClassifyByAllowanceDatasetPermissionError,
    ClassifyByAllowanceIdsResponse,
    ClassifyByAllowanceResponse,
    DatasetPermission,
    DatasetPermissionError,
    MockDatasetPermissionChecker,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockDatasetPermissionChecker {
    pub fn denying() -> Self {
        Self::denying_with_not_found(HashSet::new())
    }

    pub fn denying_with_not_found(missing_ids: HashSet<odf::DatasetID>) -> Self {
        let mut mock_dataset_permission_checker = MockDatasetPermissionChecker::new();
        mock_dataset_permission_checker
            .expect_check_permission()
            .returning(|dataset_id, permission| {
                Err(DatasetPermissionError::not_enough_permissions(
                    dataset_id.as_local_ref(),
                    permission,
                ))
            });
        let missing_ids_clone = missing_ids.clone();
        mock_dataset_permission_checker
            .expect_classify_dataset_ids_by_allowance()
            .returning(move |ids, permission| {
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
                                    ClassifyByAllowanceDatasetPermissionError::not_enough_permissions(
                                        id.as_local_ref(),
                                        permission,
                                    ),
                                )
                            }
                        })
                        .collect(),
                })
            });
        let missing_ids_for = missing_ids.clone();
        mock_dataset_permission_checker
            .expect_classify_dataset_handles_by_allowance()
            .returning(move |handles, permission| {
                Ok(Self::classify_handles(handles, |hdl| {
                    Some(Self::denial(&missing_ids, hdl, permission))
                }))
            });
        mock_dataset_permission_checker
            .expect_classify_dataset_handles_by_allowance_for()
            .returning(move |_, handles, permission| {
                Ok(Self::classify_handles(handles, |hdl| {
                    Some(Self::denial(&missing_ids_for, hdl, permission))
                }))
            });
        mock_dataset_permission_checker
    }

    /// Allows every permission on every dataset except the specified ones
    pub fn denying_ids(denied_ids: HashSet<odf::DatasetID>) -> Self {
        let mut mock_dataset_permission_checker = MockDatasetPermissionChecker::new();

        let denied = denied_ids.clone();
        mock_dataset_permission_checker
            .expect_check_permission()
            .returning(move |dataset_id, permission| {
                if denied.contains(dataset_id) {
                    Err(DatasetPermissionError::not_enough_permissions(
                        dataset_id.as_local_ref(),
                        permission,
                    ))
                } else {
                    Ok(())
                }
            });

        let denied = denied_ids.clone();
        mock_dataset_permission_checker
            .expect_classify_dataset_ids_by_allowance()
            .returning(move |ids, permission| {
                let (unauthorized, authorized): (Vec<_>, Vec<_>) =
                    ids.iter().partition(|id| denied.contains(id.as_ref()));
                Ok(ClassifyByAllowanceIdsResponse {
                    authorized_ids: authorized
                        .into_iter()
                        .map(|id| id.as_ref().clone())
                        .collect(),
                    unauthorized_ids_with_errors: unauthorized
                        .into_iter()
                        .map(|id| {
                            (
                                id.as_ref().clone(),
                                ClassifyByAllowanceDatasetPermissionError::not_enough_permissions(
                                    id.as_local_ref(),
                                    permission,
                                ),
                            )
                        })
                        .collect(),
                })
            });

        let denied = denied_ids.clone();
        mock_dataset_permission_checker
            .expect_classify_dataset_handles_by_allowance()
            .returning(move |handles, permission| {
                Ok(Self::classify_handles(handles, |hdl| {
                    denied.contains(&hdl.id).then(|| {
                        ClassifyByAllowanceDatasetPermissionError::not_enough_permissions(
                            hdl.id.as_local_ref(),
                            permission,
                        )
                    })
                }))
            });

        let denied = denied_ids.clone();
        mock_dataset_permission_checker
            .expect_classify_dataset_handles_by_allowance_for()
            .returning(move |_, handles, permission| {
                Ok(Self::classify_handles(handles, |hdl| {
                    denied.contains(&hdl.id).then(|| {
                        ClassifyByAllowanceDatasetPermissionError::not_enough_permissions(
                            hdl.id.as_local_ref(),
                            permission,
                        )
                    })
                }))
            });

        mock_dataset_permission_checker
            .expect_filter_datasets_allowing()
            .returning(move |handles, _| {
                Ok(handles
                    .into_iter()
                    .filter(|hdl| !denied_ids.contains(&hdl.id))
                    .collect())
            });

        mock_dataset_permission_checker
    }

    pub fn allowing() -> Self {
        let mut mock_dataset_permission_checker = MockDatasetPermissionChecker::new();
        mock_dataset_permission_checker
            .expect_check_permission()
            .returning(|_, _| Ok(()));
        mock_dataset_permission_checker
            .expect_classify_dataset_ids_by_allowance()
            .returning(|ids, _| {
                Ok(ClassifyByAllowanceIdsResponse {
                    authorized_ids: ids.iter().map(|id| id.as_ref().clone()).collect(),
                    unauthorized_ids_with_errors: Vec::new(),
                })
            });
        mock_dataset_permission_checker
            .expect_classify_dataset_handles_by_allowance()
            .returning(|handles, _| Ok(Self::classify_handles(handles, |_| None)));
        mock_dataset_permission_checker
            .expect_classify_dataset_handles_by_allowance_for()
            .returning(|_, handles, _| Ok(Self::classify_handles(handles, |_| None)));
        mock_dataset_permission_checker
            .expect_filter_datasets_allowing()
            .returning(|handles, _| Ok(handles));
        mock_dataset_permission_checker
    }

    pub fn expect_check_read_dataset(
        self,
        expected_dataset_id: &odf::DatasetID,
        times: usize,
        success: bool,
    ) -> Self {
        let expected_dataset_id = expected_dataset_id.clone();
        self.expect_check_permission_unauthorized_internal(
            function(move |dataset_id| *dataset_id == expected_dataset_id),
            DatasetPermission::Read,
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
        self.expect_check_permission_unauthorized_internal(
            function(move |dataset_id| *dataset_id == expected_dataset_id),
            DatasetPermission::Write,
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
        self.expect_check_permission_unauthorized_internal(
            function(move |dataset_id| *dataset_id == expected_dataset_id),
            DatasetPermission::Maintain,
            times,
            success,
        )
    }

    pub fn expect_check_read_a_dataset(self, times: usize, success: bool) -> Self {
        self.expect_check_permission_unauthorized_internal(
            always(),
            DatasetPermission::Read,
            times,
            success,
        )
    }

    pub fn expect_check_permission_unauthorized_internal<P>(
        self,
        dataset_id_predicate: P,
        permission: DatasetPermission,
        times: usize,
        success: bool,
    ) -> Self
    where
        P: Predicate<odf::DatasetID> + Sync + Send + 'static,
    {
        self.expect_check_permission_internal(
            dataset_id_predicate,
            |dataset_id, permission| {
                DatasetPermissionError::not_enough_permissions(
                    dataset_id.as_local_ref(),
                    permission,
                )
            },
            permission,
            times,
            success,
        )
    }

    fn expect_check_permission_internal<P, E>(
        mut self,
        dataset_id_predicate: P,
        error_callback: E,
        permission: DatasetPermission,
        times: usize,
        success: bool,
    ) -> Self
    where
        P: Predicate<odf::DatasetID> + Sync + Send + 'static,
        E: Fn(&odf::DatasetID, DatasetPermission) -> DatasetPermissionError + Sync + Send + 'static,
    {
        if times > 0 {
            self.expect_check_permission()
                .with(dataset_id_predicate, eq(permission))
                .times(times)
                .returning(move |dataset_id, permission| {
                    if success {
                        Ok(())
                    } else {
                        Err(error_callback(dataset_id, permission))
                    }
                });
        } else {
            self.expect_check_permission()
                .with(dataset_id_predicate, eq(permission))
                .never();
        }

        self
    }

    pub fn make_expect_classify_datasets_by_allowance(
        mut self,
        permission: DatasetPermission,
        times: usize,
        authorized: HashSet<odf::DatasetAlias>,
    ) -> Self {
        self.expect_classify_dataset_handles_by_allowance()
            .with(always(), eq(permission))
            .times(times)
            .returning(move |handles, permission| {
                Ok(Self::classify_handles(handles, |handle| {
                    (!authorized.contains(&handle.alias)).then(|| {
                        ClassifyByAllowanceDatasetPermissionError::not_enough_permissions(
                            handle.as_local_ref(),
                            permission,
                        )
                    })
                }))
            });

        self
    }

    pub fn make_expect_classify_dataset_ids_by_allowance(
        mut self,
        permission: DatasetPermission,
        times: usize,
        authorized: HashSet<odf::DatasetID>,
    ) -> Self {
        self.expect_classify_dataset_ids_by_allowance()
            .with(always(), eq(permission))
            .times(times)
            .returning(move |dataset_ids, permission| {
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
                                ClassifyByAllowanceDatasetPermissionError::not_enough_permissions(
                                    dataset_id.as_local_ref(),
                                    permission,
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

    /// Expects the principal-explicit classification of `permission` to include
    /// the given dataset; other datasets in the same call are authorized
    pub fn expect_authorize_for(
        self,
        permission: DatasetPermission,
        expected_dataset_id: &odf::DatasetID,
        times: usize,
        success: bool,
    ) -> Self {
        self.expect_authorize_for_internal(permission, expected_dataset_id, times, move |hdl| {
            (!success).then(|| {
                ClassifyByAllowanceDatasetPermissionError::not_enough_permissions(
                    hdl.as_local_ref(),
                    permission,
                )
            })
        })
    }

    /// Like [`Self::expect_authorize_for`], answering that the dataset is not
    /// found
    pub fn expect_authorize_for_not_found(
        self,
        permission: DatasetPermission,
        expected_dataset_id: &odf::DatasetID,
        times: usize,
    ) -> Self {
        self.expect_authorize_for_internal(permission, expected_dataset_id, times, |hdl| {
            Some(odf::DatasetNotFoundError::new(hdl.as_local_ref()).into())
        })
    }

    fn expect_authorize_for_internal<E>(
        mut self,
        permission: DatasetPermission,
        expected_dataset_id: &odf::DatasetID,
        times: usize,
        error_callback: E,
    ) -> Self
    where
        E: Fn(&odf::DatasetHandle) -> Option<ClassifyByAllowanceDatasetPermissionError>
            + Sync
            + Send
            + 'static,
    {
        let matched_id = expected_dataset_id.clone();
        let answered_id = expected_dataset_id.clone();
        let expectation = self
            .expect_classify_dataset_handles_by_allowance_for()
            .withf(move |_, handles, a| {
                *a == permission && handles.iter().any(|hdl| hdl.id == matched_id)
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
        permission: DatasetPermission,
        times: usize,
        authorized: HashSet<odf::DatasetAlias>,
    ) -> Self {
        self.expect_classify_dataset_handles_by_allowance_for()
            .withf(move |_, _, a| *a == permission)
            .times(times)
            .returning(move |_, handles, permission| {
                Ok(Self::classify_handles(handles, |hdl| {
                    (!authorized.contains(&hdl.alias)).then(|| {
                        ClassifyByAllowanceDatasetPermissionError::not_enough_permissions(
                            hdl.as_local_ref(),
                            permission,
                        )
                    })
                }))
            });

        self
    }

    fn classify_handles(
        handles: Vec<odf::DatasetHandle>,
        error_for: impl Fn(&odf::DatasetHandle) -> Option<ClassifyByAllowanceDatasetPermissionError>,
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

    /// Not found for the missing datasets, a missing permission for the rest
    fn denial(
        missing_ids: &HashSet<odf::DatasetID>,
        hdl: &odf::DatasetHandle,
        permission: DatasetPermission,
    ) -> ClassifyByAllowanceDatasetPermissionError {
        if missing_ids.contains(&hdl.id) {
            odf::DatasetNotFoundError::new(hdl.id.as_local_ref()).into()
        } else {
            ClassifyByAllowanceDatasetPermissionError::not_enough_permissions(
                hdl.id.as_local_ref(),
                permission,
            )
        }
    }

    pub fn make_expect_get_granted_permissions(
        mut self,
        granted_permissions: &[DatasetPermission],
        times: usize,
    ) -> Self {
        let res = granted_permissions.iter().copied().collect::<HashSet<_>>();
        self.expect_get_granted_permissions()
            .times(times)
            .returning(move |_| Ok(res.clone()));
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
