// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::testing::{BaseRepoHarness, BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::*;
use kamu_core::*;
use kamu_datasets::{DatasetPermission, MockDatasetPermissionChecker};
use pretty_assertions::assert_matches;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_into_existing_dataset_unauthorized() {
    let harness = SyncFromJobHarness::denying_write_on_foo();
    let foo = harness.create_foo().await;

    assert_matches!(
        harness.pull_into(PullLocalTarget::existing(foo)).await,
        Err(SyncFromJobError::Sync(SyncError::Access(_)))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A pull planned to create the dataset finds it created by the time the job
/// runs: moving its `HEAD` needs the same right as for any existing dataset
#[tokio::test]
async fn test_sync_into_dataset_created_since_planning_unauthorized() {
    let harness = SyncFromJobHarness::denying_write_on_foo();
    let foo = harness.create_foo().await;

    assert_matches!(
        harness
            .pull_into(PullLocalTarget::to_create(foo.alias))
            .await,
        Err(SyncFromJobError::Sync(SyncError::Access(_)))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The sync service has no expectations, so a transfer fails the test
#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct SyncFromJobHarness {
    base_use_case_harness: BaseUseCaseHarness,
    job: Arc<dyn SyncFromJob>,
    alias_foo: odf::DatasetAlias,
}

impl SyncFromJobHarness {
    fn denying_write_on_foo() -> Self {
        let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_permission_checker(Some(
                    MockDatasetPermissionChecker::new().expect_denied_but_readable(
                        DatasetPermission::Write,
                        &dataset_id_foo,
                        1,
                    ),
                ))
                .with_maybe_mock_did_generator(Some(MockDidGenerator::predefined_dataset_ids(
                    vec![dataset_id_foo],
                ))),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<SyncFromJobImpl>()
            .add_value(MockSyncService::new())
            .bind::<dyn SyncService, MockSyncService>();
        let catalog = b.build();

        Self {
            base_use_case_harness,
            job: catalog.get_one().unwrap(),
            alias_foo: BaseRepoHarness::alias("foo"),
        }
    }

    async fn create_foo(&self) -> odf::DatasetHandle {
        self.create_root_dataset(&self.alias_foo)
            .await
            .dataset_handle
    }

    async fn pull_into(&self, target: PullLocalTarget) -> Result<SyncResult, SyncFromJobError> {
        let remote_ref = odf::DatasetRefRemote::Alias(odf::DatasetAliasRemote {
            repo_name: odf::RepoName::new_unchecked("remote"),
            account_name: None,
            dataset_name: self.alias_foo.dataset_name.clone(),
        });
        self.job
            .run(&target, &remote_ref, SyncOptions::default(), None)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
