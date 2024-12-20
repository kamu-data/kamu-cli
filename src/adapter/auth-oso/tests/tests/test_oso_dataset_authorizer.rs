// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::HashSet;
use std::sync::Arc;

use dill::{Catalog, Component};
use kamu::testing::MetadataFactory;
use kamu::{CreateDatasetUseCaseImpl, DatasetRepositoryLocalFs, DatasetRepositoryWriter};
use kamu_accounts::CurrentAccountSubject;
use kamu_adapter_auth_oso::{KamuAuthOso, OsoDatasetAuthorizer};
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer, DatasetActionUnauthorizedError};
use kamu_core::{AccessError, CreateDatasetUseCase, DatasetRepository, TenancyConfig};
use messaging_outbox::DummyOutboxImpl;
use opendatafabric::{AccountID, AccountName, DatasetAlias, DatasetHandle, DatasetKind};
use tempfile::TempDir;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    let allowed_actions = harness
        .dataset_authorizer
        .get_allowed_actions(&dataset_handle)
        .await;

    assert_matches!(read_result, Ok(()));
    assert_matches!(write_result, Ok(()));

    assert_eq!(
        allowed_actions,
        HashSet::from([DatasetAction::Read, DatasetAction::Write])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    let allowed_actions = harness
        .dataset_authorizer
        .get_allowed_actions(&dataset_handle)
        .await;

    assert_matches!(read_result, Ok(()));
    assert_matches!(
        write_result,
        Err(DatasetActionUnauthorizedError::Access(
            AccessError::Forbidden(_)
        ))
    );

    assert_eq!(allowed_actions, HashSet::from([DatasetAction::Read]));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub struct DatasetAuthorizerHarness {
    tempdir: TempDir,
    catalog: Catalog,
    dataset_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

impl DatasetAuthorizerHarness {
    pub fn new(current_account_name: &str) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<SystemTimeSourceDefault>()
            .add::<DummyOutboxImpl>()
            .add_value(CurrentAccountSubject::logged(
                AccountID::new_seeded_ed25519(current_account_name.as_bytes()),
                AccountName::new_unchecked(current_account_name),
                false,
            ))
            .add::<KamuAuthOso>()
            .add::<OsoDatasetAuthorizer>()
            .add_value(TenancyConfig::MultiTenant)
            .add_builder(DatasetRepositoryLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<CreateDatasetUseCaseImpl>()
            .build();

        let dataset_authorizer = catalog.get_one::<dyn DatasetActionAuthorizer>().unwrap();

        Self {
            tempdir,
            catalog,
            dataset_authorizer,
        }
    }

    pub async fn create_dataset(&self, alias: &DatasetAlias) -> DatasetHandle {
        let create_dataset = self.catalog.get_one::<dyn CreateDatasetUseCase>().unwrap();

        create_dataset
            .execute(
                alias,
                MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                    .build_typed(),
                Default::default(),
            )
            .await
            .unwrap()
            .dataset_handle
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
