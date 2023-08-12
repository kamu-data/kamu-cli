// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use kamu::testing::MetadataFactory;
use kamu::DatasetRepositoryLocalFs;
use kamu_adapter_auth_oso::{KamuAuthOso, OsoDatasetAuthorizer};
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer, DatasetActionUnauthorizedError};
use kamu_core::{AccessError, CurrentAccountSubject, DatasetRepository};
use opendatafabric::{DatasetAlias, DatasetHandle, DatasetKind};
use tempfile::TempDir;

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write() {
    let harness = DatasetAuthorizerHarness::new("john");
    let dataset_handle = harness
        .create_dataset(&DatasetAlias::try_from("john/foo").unwrap())
        .await;

    let read_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_handle, DatasetAction::Read)
        .await;

    let write_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_handle, DatasetAction::Write)
        .await;

    assert_matches!(read_result, Ok(()));
    assert_matches!(write_result, Ok(()));
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_guest_can_read_but_not_write() {
    let harness = DatasetAuthorizerHarness::new("kate");
    let dataset_handle = harness
        .create_dataset(&DatasetAlias::try_from("john/foo").unwrap())
        .await;

    let read_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_handle, DatasetAction::Read)
        .await;

    let write_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_handle, DatasetAction::Write)
        .await;

    assert_matches!(read_result, Ok(()));
    assert_matches!(
        write_result,
        Err(DatasetActionUnauthorizedError::Access(
            AccessError::Forbidden(_)
        ))
    );
}

///////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub struct DatasetAuthorizerHarness {
    tempdir: TempDir,
    dataset_repository: Arc<dyn DatasetRepository>,
    dataset_authorizer: Arc<OsoDatasetAuthorizer>,
}

impl DatasetAuthorizerHarness {
    pub fn new(current_account_name: &str) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let current_account_subject = Arc::new(CurrentAccountSubject::new(current_account_name));

        let dataset_authorizer = Arc::new(OsoDatasetAuthorizer::new(
            Arc::new(KamuAuthOso::new()),
            current_account_subject.clone(),
        ));

        let dataset_repository = Arc::new(DatasetRepositoryLocalFs::new(
            datasets_dir,
            current_account_subject.clone(),
            dataset_authorizer.clone(),
            true,
        ));

        Self {
            tempdir,
            dataset_repository,
            dataset_authorizer,
        }
    }

    pub async fn create_dataset(&self, alias: &DatasetAlias) -> DatasetHandle {
        self.dataset_repository
            .create_dataset(
                alias,
                MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                    .build_typed(),
            )
            .await
            .unwrap()
            .dataset_handle
    }
}

///////////////////////////////////////////////////////////////////////////////
