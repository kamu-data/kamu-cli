// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::HashSet;
use std::sync::Arc;

use internal_error::InternalError;
use kamu::testing::{
    BaseUseCaseHarness,
    BaseUseCaseHarnessOptions,
    DummySmartTransferProtocolClient,
};
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocol;
use kamu::*;
use kamu_core::*;
use kamu_datasets::{CreateDatasetResult, DatasetPermission, MockDatasetPermissionChecker};
use kamu_datasets_services::{
    AppendDatasetMetadataBatchUseCaseImpl,
    DatasetHeadCommitterImpl,
    DependencyGraphServiceImpl,
};
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
use tempfile::TempDir;
use url::Url;

use crate::utils::RecordingListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let mock_permission_checker = MockDatasetPermissionChecker::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetPermission::Read,
            2,
            HashSet::from_iter([alias_foo.clone()]),
        );

    let harness = PushUseCaseHarness::new(mock_permission_checker);
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
            result: Ok(SyncResult::Updated { old_head, new_head: _, num_blocks, increment: _ }),
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

    let mock_permission_checker = MockDatasetPermissionChecker::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetPermission::Read,
            1,
            HashSet::from_iter([alias_foo.clone()]),
        );

    let harness = PushUseCaseHarness::new(mock_permission_checker);
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
            result: Ok(SyncResult::Updated { old_head, new_head: _, num_blocks, increment: _ }),
        } if local_handle == foo.dataset_handle &&
            repo_name == harness.remote_repo_name &&
            old_head.is_none() && num_blocks == 2
    );

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Push));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_unreadable_is_not_found() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let mock_permission_checker = MockDatasetPermissionChecker::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetPermission::Read,
            1,
            HashSet::new(), // not authorized
        );

    let harness = PushUseCaseHarness::new(mock_permission_checker);
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
            result: Err(PushError::SyncError(SyncError::DatasetNotFound(_))),
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

    let mock_permission_checker = MockDatasetPermissionChecker::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetPermission::Read,
            1,
            HashSet::from_iter([alias_foo.clone(), alias_bar.clone()]),
        );

    let harness = PushUseCaseHarness::new(mock_permission_checker);
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
async fn test_push_multiple_mixed_authorization_pushes_readable_datasets() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let mock_permission_checker = MockDatasetPermissionChecker::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetPermission::Read,
            1,
            HashSet::from_iter([alias_bar.clone()]), // 1 of 3 is authorized
        );

    let harness = PushUseCaseHarness::new(mock_permission_checker);
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

    assert_eq!(responses.len(), 3);
    assert_matches!(
        responses.remove(2),
         PushResponse {
            local_handle: Some(local_handle),
            target: Some(odf::DatasetPushTarget::Repository(repo_name)),
            result: Ok(SyncResult::Updated { .. }),
        } if local_handle == bar.dataset_handle &&
            repo_name == harness.remote_repo_name
    );
    assert_matches!(
        responses.remove(1),
         PushResponse {
            local_handle: Some(local_handle),
            target: Some(odf::DatasetPushTarget::Repository(repo_name)),
            result: Err(PushError::SyncError(SyncError::DatasetNotFound(_))),
        } if local_handle == baz.dataset_handle &&
            repo_name == harness.remote_repo_name
    );
    assert_matches!(
        responses.remove(0),
         PushResponse {
            local_handle: Some(local_handle),
            target: Some(odf::DatasetPushTarget::Repository(repo_name)),
            result: Err(PushError::SyncError(SyncError::DatasetNotFound(_))),
        } if local_handle == foo.dataset_handle &&
            repo_name == harness.remote_repo_name
    );

    // The pushed dataset gets its alias, the unreadable ones do not
    let bar_aliases = harness.get_remote_aliases(&bar).await;
    assert!(!bar_aliases.is_empty(RemoteAliasKind::Push));
    let foo_aliases = harness.get_remote_aliases(&foo).await;
    assert!(foo_aliases.is_empty(RemoteAliasKind::Push));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_multiple_mixed_target_failures_pushes_resolved_datasets() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let mock_permission_checker = MockDatasetPermissionChecker::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetPermission::Read,
            1,
            HashSet::from_iter([alias_foo.clone(), alias_bar.clone(), alias_baz.clone()]),
        );

    let harness = PushUseCaseHarness::new(mock_permission_checker);
    harness.add_repository("other", "file:///other/");
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness.create_root_dataset(&alias_baz).await;
    let foo_url = harness.remote_repo_url.join("foo").unwrap();
    harness.add_push_alias(&foo, foo_url.clone()).await;
    // `bar` has no alias, and the plan cannot choose between two repositories.
    // `baz` has an alias the plan resolves, but no sync can reach
    harness
        .add_push_alias(&baz, Url::parse("ftp://example.com/baz").unwrap())
        .await;

    let responses = harness
        .use_case
        .execute_multi(
            vec![
                foo.dataset_handle.clone(),
                bar.dataset_handle.clone(),
                baz.dataset_handle.clone(),
            ],
            PushMultiOptions::default(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(responses.len(), 3);
    assert_matches!(
        PushUseCaseHarness::response_for(&responses, &foo).result,
        Ok(SyncResult::Updated { .. })
    );
    assert_matches!(
        PushUseCaseHarness::response_for(&responses, &bar).result,
        Err(PushError::AmbiguousRepository)
    );
    assert_matches!(
        PushUseCaseHarness::response_for(&responses, &baz).result,
        Err(PushError::SyncError(SyncError::UnsupportedProtocol(_)))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_reports_unsaved_alias_as_the_dataset_error() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let mock_permission_checker = MockDatasetPermissionChecker::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetPermission::Read,
            1,
            HashSet::from_iter([alias_foo.clone()]),
        );

    let harness = PushUseCaseHarness::with_failing_alias_registry(mock_permission_checker);
    let foo = harness.create_root_dataset(&alias_foo).await;

    let responses = harness
        .use_case
        .execute_multi(
            vec![foo.dataset_handle.clone()],
            PushMultiOptions {
                remote_target: Some(odf::DatasetPushTarget::Repository(
                    harness.remote_repo_name.clone(),
                )),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    assert_eq!(responses.len(), 1);
    assert_matches!(
        &responses[0].result,
        Err(PushError::SyncError(SyncError::Internal(e)))
            if e.reason().contains("Dataset was pushed, but its push alias was not saved")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_reports_success_to_the_listener_once() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let mock_permission_checker = MockDatasetPermissionChecker::new()
        .make_expect_classify_datasets_by_allowance(
            DatasetPermission::Read,
            1,
            HashSet::from_iter([alias_foo.clone()]),
        );

    let harness = PushUseCaseHarness::new(mock_permission_checker);
    let foo = harness.create_root_dataset(&alias_foo).await;

    let (responses, events) = harness.push_to_remote_repo_recording(&foo).await;

    assert_matches!(
        responses.as_slice(),
        [PushResponse {
            result: Ok(SyncResult::Updated { .. }),
            ..
        }]
    );
    assert_eq!(events, vec!["begin", "success"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct PushUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn PushDatasetUseCase>,
    remote_aliases_registry: Arc<dyn RemoteAliasesRegistry>,
    remote_repo_registry: Arc<dyn RemoteRepositoryRegistry>,
    remote_repo_name: odf::RepoName,
    remote_repo_url: Url,
    _remote_tmp_dir: TempDir,
}

impl PushUseCaseHarness {
    fn new(mock_dataset_permission_checker: MockDatasetPermissionChecker) -> Self {
        Self::build(mock_dataset_permission_checker, None)
    }

    /// Every attempt to read or store the aliases of a dataset fails
    fn with_failing_alias_registry(
        mock_dataset_permission_checker: MockDatasetPermissionChecker,
    ) -> Self {
        let mut alias_registry = MockRemoteAliasesRegistry::new();
        alias_registry
            .expect_get_remote_aliases()
            .returning(|_| Err(InternalError::new("alias storage is unavailable")));
        Self::build(mock_dataset_permission_checker, Some(alias_registry))
    }

    fn build(
        mock_dataset_permission_checker: MockDatasetPermissionChecker,
        maybe_alias_registry: Option<MockRemoteAliasesRegistry>,
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_permission_checker(Some(mock_dataset_permission_checker)),
        );

        let repos_dir = base_use_case_harness.temp_dir_path().join("repos");
        std::fs::create_dir(&repos_dir).unwrap();

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());

        b.add::<PushDatasetUseCaseImpl>()
            .add::<PushRequestPlannerImpl>()
            .add::<SyncServiceImpl>()
            .add::<IpfsServiceImpl>()
            .add::<SyncRequestBuilder>()
            .add::<DatasetFactoryImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add::<RemoteDatasetUrlResolverImpl>()
            .add::<RemoteRepositoryRegistryImpl>()
            .add_value(RemoteReposDir::new(repos_dir))
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add::<DummySmartTransferProtocolClient>()
            .add::<SimpleTransferProtocol>()
            .add_value(IpfsClient::default())
            .add_value(IpfsGateway::default())
            .add::<AppendDatasetMetadataBatchUseCaseImpl>()
            .add::<DatasetHeadCommitterImpl>()
            .add::<DependencyGraphServiceImpl>();

        match maybe_alias_registry {
            Some(alias_registry) => {
                b.add_value(alias_registry)
                    .bind::<dyn RemoteAliasesRegistry, MockRemoteAliasesRegistry>();
            }
            None => {
                b.add::<RemoteAliasesRegistryImpl>();
            }
        }

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
            remote_repo_registry,
            remote_repo_name,
            remote_repo_url,
            _remote_tmp_dir: remote_tmp_dir,
        }
    }

    async fn push_to_remote_repo_recording(
        &self,
        created: &CreateDatasetResult,
    ) -> (Vec<PushResponse>, Vec<&'static str>) {
        let listener = Arc::new(RecordingListener::default());
        let responses = self
            .use_case
            .execute_multi(
                vec![created.dataset_handle.clone()],
                PushMultiOptions {
                    remote_target: Some(odf::DatasetPushTarget::Repository(
                        self.remote_repo_name.clone(),
                    )),
                    ..Default::default()
                },
                Some(Arc::new(RecordingSyncMultiListener(listener.clone()))),
            )
            .await
            .unwrap();
        (responses, listener.events())
    }

    fn add_repository(&self, repo_name: &str, url: &str) {
        self.remote_repo_registry
            .add_repository(
                &odf::RepoName::new_unchecked(repo_name),
                Url::parse(url).unwrap(),
            )
            .unwrap();
    }

    async fn add_push_alias(&self, created: &CreateDatasetResult, url: Url) {
        self.get_remote_aliases(created)
            .await
            .add(
                &odf::DatasetRefRemote::Url(Arc::new(url)),
                RemoteAliasKind::Push,
            )
            .await
            .unwrap();
    }

    fn response_for<'a>(
        responses: &'a [PushResponse],
        created: &CreateDatasetResult,
    ) -> &'a PushResponse {
        responses
            .iter()
            .find(|response| response.local_handle.as_ref() == Some(&created.dataset_handle))
            .unwrap()
    }

    async fn get_remote_aliases(&self, created: &CreateDatasetResult) -> Box<dyn RemoteAliases> {
        self.remote_aliases_registry
            .get_remote_aliases(&created.dataset_handle)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct RecordingSyncMultiListener(Arc<RecordingListener>);

impl SyncMultiListener for RecordingSyncMultiListener {
    fn begin_sync(
        &self,
        _src: &odf::DatasetRefAny,
        _dst: &odf::DatasetRefAny,
    ) -> Option<Arc<dyn SyncListener>> {
        Some(self.0.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
