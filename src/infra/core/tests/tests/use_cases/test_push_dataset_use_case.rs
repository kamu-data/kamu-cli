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

use kamu::testing::{
    BaseUseCaseHarness,
    BaseUseCaseHarnessOptions,
    DummySmartTransferProtocolClient,
    MockDatasetActionAuthorizer,
};
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocol;
use kamu::*;
use kamu_core::auth::DatasetAction;
use kamu_core::*;
use kamu_datasets::CreateDatasetResult;
use kamu_datasets_services::{AppendDatasetMetadataBatchUseCaseImpl, DependencyGraphServiceImpl};
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
use tempfile::TempDir;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let mock_authorizer = MockDatasetActionAuthorizer::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetAction::Read,
            2,
            HashSet::from_iter([alias_foo.clone()]),
        );

    let harness = PushUseCaseHarness::new(mock_authorizer);
    let foo = harness.create_root_dataset(&alias_foo).await;

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Push));

    let push_options = PushMultiOptions {
        remote_target: Some(odf::DatasetPushTarget::Repository(
            harness.remote_repo_name.clone(),
        )),
        ..Default::default()
    };

    let mut responses = harness
        .use_case
        .execute_multi(vec![foo.dataset_handle.clone()], push_options.clone(), None)
        .await
        .unwrap();

    assert_eq!(responses.len(), 1);
    assert_matches!(
        responses.remove(0),
        PushResponse {
            local_handle: Some(local_handle),
            target: Some(odf::DatasetPushTarget::Repository(repo_name)),
            result: Ok(SyncResult::Updated { old_head, new_head: _, num_blocks }),
        } if local_handle == foo.dataset_handle &&
            repo_name == harness.remote_repo_name &&
            old_head.is_none() && num_blocks == 2
    );

    let aliases = harness.get_remote_aliases(&foo).await;
    let push_aliases: Vec<_> = aliases.get_by_kind(RemoteAliasKind::Push).collect();
    assert_eq!(
        push_aliases,
        vec![&odf::DatasetRefRemote::Url(Arc::new(
            harness.remote_repo_url.join("foo").unwrap()
        ))]
    );

    let mut responses = harness
        .use_case
        .execute_multi(vec![foo.dataset_handle.clone()], push_options, None)
        .await
        .unwrap();

    assert_eq!(responses.len(), 1);
    assert_matches!(
        responses.remove(0),
         PushResponse {
            local_handle: Some(local_handle),
            target: Some(odf::DatasetPushTarget::Repository(repo_name)),
            result: Ok(SyncResult::UpToDate),
        } if local_handle == foo.dataset_handle &&
            repo_name == harness.remote_repo_name
    );

    let aliases = harness.get_remote_aliases(&foo).await;
    let push_aliases: Vec<_> = aliases.get_by_kind(RemoteAliasKind::Push).collect();
    assert_eq!(
        push_aliases,
        vec![&odf::DatasetRefRemote::Url(Arc::new(
            harness.remote_repo_url.join("foo").unwrap()
        ))]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_success_without_saving_alias() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let mock_authorizer = MockDatasetActionAuthorizer::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetAction::Read,
            1,
            HashSet::from_iter([alias_foo.clone()]),
        );

    let harness = PushUseCaseHarness::new(mock_authorizer);
    let foo = harness.create_root_dataset(&alias_foo).await;

    let push_options = PushMultiOptions {
        remote_target: Some(odf::DatasetPushTarget::Repository(
            harness.remote_repo_name.clone(),
        )),
        add_aliases: false,
        ..Default::default()
    };

    let mut responses = harness
        .use_case
        .execute_multi(vec![foo.dataset_handle.clone()], push_options.clone(), None)
        .await
        .unwrap();

    assert_eq!(responses.len(), 1);
    assert_matches!(
        responses.remove(0),
         PushResponse {
            local_handle: Some(local_handle),
            target: Some(odf::DatasetPushTarget::Repository(repo_name)),
            result: Ok(SyncResult::Updated { old_head, new_head: _, num_blocks }),
        } if local_handle == foo.dataset_handle &&
            repo_name == harness.remote_repo_name &&
            old_head.is_none() && num_blocks == 2
    );

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Push));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_unauthorized() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let mock_authorizer = MockDatasetActionAuthorizer::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetAction::Read,
            1,
            HashSet::new(), // not authorized
        );

    let harness = PushUseCaseHarness::new(mock_authorizer);
    let foo = harness.create_root_dataset(&alias_foo).await;

    let push_options = PushMultiOptions {
        remote_target: Some(odf::DatasetPushTarget::Repository(
            harness.remote_repo_name.clone(),
        )),
        ..Default::default()
    };

    let mut responses = harness
        .use_case
        .execute_multi(vec![foo.dataset_handle.clone()], push_options.clone(), None)
        .await
        .unwrap();

    assert_eq!(responses.len(), 1);
    assert_matches!(
        responses.remove(0),
         PushResponse {
            local_handle: Some(local_handle),
            target: Some(odf::DatasetPushTarget::Repository(repo_name)),
            result: Err(PushError::SyncError(SyncError::Access(_))),
        } if local_handle == foo.dataset_handle &&
            repo_name == harness.remote_repo_name
    );

    // Aliases should not be touched in case of failure
    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Push));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_multiple_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let mock_authorizer = MockDatasetActionAuthorizer::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetAction::Read,
            1,
            HashSet::from_iter([alias_foo.clone(), alias_bar.clone()]),
        );

    let harness = PushUseCaseHarness::new(mock_authorizer);
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;

    let push_options = PushMultiOptions {
        remote_target: Some(odf::DatasetPushTarget::Repository(
            harness.remote_repo_name.clone(),
        )),
        ..Default::default()
    };

    let mut responses = harness
        .use_case
        .execute_multi(
            vec![foo.dataset_handle.clone(), bar.dataset_handle.clone()],
            push_options.clone(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(responses.len(), 2);
    assert_matches!(
        responses.remove(1),
         PushResponse {
            local_handle: Some(local_handle),
            target: Some(odf::DatasetPushTarget::Repository(repo_name)),
            result: Ok(SyncResult::Updated { .. }),
        } if local_handle == bar.dataset_handle &&
            repo_name == harness.remote_repo_name
    );
    assert_matches!(
        responses.remove(0),
         PushResponse {
            local_handle: Some(local_handle),
            target: Some(odf::DatasetPushTarget::Repository(repo_name)),
            result: Ok(SyncResult::Updated { .. }),
        } if local_handle == foo.dataset_handle &&
            repo_name == harness.remote_repo_name
    );

    let foo_aliases = harness.get_remote_aliases(&foo).await;
    let push_aliases: Vec<_> = foo_aliases.get_by_kind(RemoteAliasKind::Push).collect();
    assert_eq!(
        push_aliases,
        vec![&odf::DatasetRefRemote::Url(Arc::new(
            harness.remote_repo_url.join("foo").unwrap()
        ))]
    );

    let bar_aliases = harness.get_remote_aliases(&bar).await;
    let push_aliases: Vec<_> = bar_aliases.get_by_kind(RemoteAliasKind::Push).collect();
    assert_eq!(
        push_aliases,
        vec![&odf::DatasetRefRemote::Url(Arc::new(
            harness.remote_repo_url.join("bar").unwrap()
        ))]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_multiple_mixed_authorization_issues() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let mock_authorizer = MockDatasetActionAuthorizer::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetAction::Read,
            1,
            HashSet::from_iter([alias_bar.clone()]), // 1 of 3 is authorized
        );

    let harness = PushUseCaseHarness::new(mock_authorizer);
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness.create_root_dataset(&alias_baz).await;

    let push_options = PushMultiOptions {
        remote_target: Some(odf::DatasetPushTarget::Repository(
            harness.remote_repo_name.clone(),
        )),
        ..Default::default()
    };

    let mut responses = harness
        .use_case
        .execute_multi(
            vec![
                foo.dataset_handle.clone(),
                bar.dataset_handle.clone(),
                baz.dataset_handle.clone(),
            ],
            push_options.clone(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(responses.len(), 2);
    assert_matches!(
        responses.remove(1),
         PushResponse {
            local_handle: Some(local_handle),
            target: Some(odf::DatasetPushTarget::Repository(repo_name)),
            result: Err(PushError::SyncError(SyncError::Access(_))),
        } if local_handle == baz.dataset_handle &&
            repo_name == harness.remote_repo_name
    );
    assert_matches!(
        responses.remove(0),
         PushResponse {
            local_handle: Some(local_handle),
            target: Some(odf::DatasetPushTarget::Repository(repo_name)),
            result: Err(PushError::SyncError(SyncError::Access(_))),
        } if local_handle == foo.dataset_handle &&
            repo_name == harness.remote_repo_name
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct PushUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn PushDatasetUseCase>,
    remote_aliases_registry: Arc<dyn RemoteAliasesRegistry>,
    remote_repo_name: odf::RepoName,
    remote_repo_url: Url,
    _remote_tmp_dir: TempDir,
}

impl PushUseCaseHarness {
    fn new(mock_dataset_action_authorizer: MockDatasetActionAuthorizer) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_authorizer(Some(mock_dataset_action_authorizer)),
        );

        let repos_dir = base_use_case_harness.temp_dir_path().join("repos");
        std::fs::create_dir(&repos_dir).unwrap();

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());

        b.add::<PushDatasetUseCaseImpl>()
            .add::<PushRequestPlannerImpl>()
            .add::<SyncServiceImpl>()
            .add::<SyncRequestBuilder>()
            .add::<DatasetFactoryImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add::<RemoteRepositoryRegistryImpl>()
            .add_value(RemoteReposDir::new(repos_dir))
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add::<DummySmartTransferProtocolClient>()
            .add::<SimpleTransferProtocol>()
            .add_value(IpfsClient::default())
            .add_value(IpfsGateway::default())
            .add::<AppendDatasetMetadataBatchUseCaseImpl>()
            .add::<DependencyGraphServiceImpl>();

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        let use_case = catalog.get_one().unwrap();
        let remote_aliases_registry = catalog.get_one().unwrap();

        let remote_tmp_dir = tempfile::tempdir().unwrap();
        let remote_repo_url = Url::from_directory_path(remote_tmp_dir.path()).unwrap();

        let remote_repo_name = odf::RepoName::new_unchecked("remote");
        let remote_repo_registry = catalog.get_one::<dyn RemoteRepositoryRegistry>().unwrap();
        remote_repo_registry
            .add_repository(&remote_repo_name, remote_repo_url.clone())
            .unwrap();

        Self {
            base_use_case_harness,
            use_case,
            remote_aliases_registry,
            remote_repo_name,
            remote_repo_url,
            _remote_tmp_dir: remote_tmp_dir,
        }
    }

    async fn get_remote_aliases(&self, created: &CreateDatasetResult) -> Box<dyn RemoteAliases> {
        self.remote_aliases_registry
            .get_remote_aliases(&created.dataset_handle)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
