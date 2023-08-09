// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::auth::{
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetActionNotEnoughPermissionsError,
    DatasetActionUnauthorizedError,
};
use kamu_core::AccessError;
use opendatafabric::{AccountName, DatasetHandle};

/////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub DatasetActionAuthorizer {}
    #[async_trait::async_trait]
    impl DatasetActionAuthorizer for DatasetActionAuthorizer {
        async fn check_action_allowed(
            &self,
            dataset_handle: &DatasetHandle,
            account_name: &AccountName,
            action: DatasetAction,
        ) -> Result<(), DatasetActionUnauthorizedError>;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub fn expecting_write_mock() -> MockDatasetActionAuthorizer {
    let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
    mock_dataset_action_authorizer
        .expect_check_action_allowed()
        .return_once(|_, _, action| match action {
            DatasetAction::Write => Ok(()),
            _ => panic!("Expected Write action"),
        });
    mock_dataset_action_authorizer
}

/////////////////////////////////////////////////////////////////////////////////////////

pub fn expecting_read_mock() -> MockDatasetActionAuthorizer {
    let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
    mock_dataset_action_authorizer
        .expect_check_action_allowed()
        .return_once(|_, _, action| match action {
            DatasetAction::Read => Ok(()),
            _ => panic!("Expected Read action"),
        });
    mock_dataset_action_authorizer
}

/////////////////////////////////////////////////////////////////////////////////////////

pub fn denying_mock() -> MockDatasetActionAuthorizer {
    let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
    mock_dataset_action_authorizer
        .expect_check_action_allowed()
        .return_once(|dataset_handle, account_name, action| {
            Err(DatasetActionUnauthorizedError::Access(
                AccessError::Forbidden(
                    DatasetActionNotEnoughPermissionsError {
                        account_name: account_name.clone(),
                        action,
                        dataset_ref: dataset_handle.as_local_ref(),
                    }
                    .into(),
                ),
            ))
        });
    mock_dataset_action_authorizer
}

/////////////////////////////////////////////////////////////////////////////////////////
