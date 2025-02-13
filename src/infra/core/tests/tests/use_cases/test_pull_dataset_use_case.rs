// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions, *};
use kamu::*;
use kamu_core::auth::DatasetAction;
use kamu_core::*;
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
use tempfile::TempDir;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let mocks = PullUseCaseHarnessMocks::default()
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
                DatasetAction::Write,
                1,
                HashSet::from_iter([alias_foo.clone()]),
            ),
        )
        .with_polling_ingest_mock(
            MockPollingIngestService::new().make_expect_ingest(alias_foo.clone()),
        );

    let harness = PullUseCaseHarness::new(mocks);
    let foo = harness.create_root_dataset(&alias_foo).await;

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));

    let pull_request = PullRequest::Local(alias_foo.as_local_ref());

    let pull_response = harness
        .use_case
        .execute(pull_request.clone(), PullOptions::default(), None)
        .await
        .unwrap();

    assert_matches!(
        pull_response,
        PullResponse {
            maybe_original_request: Some(a_pull_request),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(_)),
        } if a_pull_request == pull_request &&
        a_local_ref == foo.dataset_handle.as_local_ref()
    );

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let mocks = PullUseCaseHarnessMocks::default()
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Read,
                    1,
                    HashSet::from_iter([alias_foo.clone()]),
                ),
        )
        .with_transform_elaboration_mock(
            MockTransformElaborationService::new()
                .make_expect_elaborate_transform(alias_bar.clone()),
        )
        .with_transform_execution_mock(
            MockTransformExecutionService::new().make_expect_transform(alias_bar.clone()),
        );

    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let aliases = harness.get_remote_aliases(&bar).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));

    let pull_request = PullRequest::Local(alias_bar.as_local_ref());

    let pull_response = harness
        .use_case
        .execute(pull_request.clone(), PullOptions::default(), None)
        .await
        .unwrap();

    assert_matches!(
        pull_response,
        PullResponse {
            maybe_original_request: Some(a_pull_request),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(_)),
        } if a_pull_request == pull_request &&
        a_local_ref == bar.dataset_handle.as_local_ref()
    );

    let aliases = harness.get_remote_aliases(&bar).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_sync_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let remote_ref = odf::DatasetRefRemote::Alias(odf::DatasetAliasRemote {
        repo_name: odf::RepoName::new_unchecked(REMOTE_REPO_NAME_STR),
        account_name: None,
        dataset_name: alias_foo.dataset_name.clone(),
    });

    let mocks = PullUseCaseHarnessMocks::default()
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
                DatasetAction::Write,
                1,
                HashSet::from_iter([alias_foo.clone()]),
            ),
        )
        .with_sync_mock(
            MockSyncService::new().make_expect_sync_pull_from_remote_to_existing_local(
                alias_foo.clone(),
                remote_ref.clone(),
                SyncResult::Updated {
                    old_head: None,
                    new_head: odf::Multihash::from_multibase(
                        "f16205603b882241c71351baf996d6dba7e3ddbd571457e93c1cd282bdc61f9fed5f2",
                    )
                    .unwrap(),
                    num_blocks: 0,
                },
            ),
        );

    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;

    harness.copy_dataset_to_remote_repo(&alias_foo).await;

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));

    let pull_request = PullRequest::Remote(PullRequestRemote {
        remote_ref: remote_ref.clone(),
        maybe_local_alias: Some(alias_foo.clone()),
    });

    let pull_response = harness
        .use_case
        .execute(pull_request.clone(), PullOptions::default(), None)
        .await
        .unwrap();

    assert_matches!(
        pull_response,
        PullResponse {
            maybe_original_request: Some(a_pull_request),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: Some(a_remote_ref),
            result: Ok(PullResult::Updated { .. }),
        } if a_pull_request == pull_request &&
        a_local_ref == foo.dataset_handle.as_local_ref() &&
        a_remote_ref == remote_ref,
    );

    let aliases = harness.get_remote_aliases(&foo).await;
    let pull_aliases: Vec<_> = aliases.get_by_kind(RemoteAliasKind::Pull).collect();
    assert_eq!(
        pull_aliases,
        vec![&odf::DatasetRefRemote::Alias(odf::DatasetAliasRemote {
            repo_name: harness.remote_repo_name,
            account_name: None,
            dataset_name: alias_foo.dataset_name.clone()
        })]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_sync_success_without_saving_alias() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let remote_ref = odf::DatasetRefRemote::Alias(odf::DatasetAliasRemote {
        repo_name: odf::RepoName::new_unchecked(REMOTE_REPO_NAME_STR),
        account_name: None,
        dataset_name: alias_foo.dataset_name.clone(),
    });

    let mocks = PullUseCaseHarnessMocks::default()
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
                DatasetAction::Write,
                1,
                HashSet::from_iter([alias_foo.clone()]),
            ),
        )
        .with_sync_mock(
            MockSyncService::new().make_expect_sync_pull_from_remote_to_existing_local(
                alias_foo.clone(),
                remote_ref.clone(),
                SyncResult::Updated {
                    old_head: None,
                    new_head: odf::Multihash::from_multibase(
                        "f16205603b882241c71351baf996d6dba7e3ddbd571457e93c1cd282bdc61f9fed5f2",
                    )
                    .unwrap(),
                    num_blocks: 0,
                },
            ),
        );

    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;

    harness.copy_dataset_to_remote_repo(&alias_foo).await;

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));

    let pull_request = PullRequest::Remote(PullRequestRemote {
        remote_ref: remote_ref.clone(),
        maybe_local_alias: Some(alias_foo.clone()),
    });

    let pull_response = harness
        .use_case
        .execute(
            pull_request.clone(),
            PullOptions {
                add_aliases: false,
                ..PullOptions::default()
            },
            None,
        )
        .await
        .unwrap();

    assert_matches!(
        pull_response,
        PullResponse {
            maybe_original_request: Some(a_pull_request),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: Some(a_remote_ref),
            result: Ok(PullResult::Updated { .. }),
        } if a_pull_request == pull_request &&
        a_local_ref == foo.dataset_handle.as_local_ref() &&
        a_remote_ref == remote_ref,
    );

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_multi_recursive() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let alias_foo_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo-bar"));

    let mocks = PullUseCaseHarnessMocks::default()
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_foo.clone(), alias_bar.clone(), alias_baz.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_foo_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Read,
                    1,
                    HashSet::from_iter([alias_foo.clone(), alias_bar.clone()]),
                ),
        )
        .with_polling_ingest_mock(
            MockPollingIngestService::new()
                .make_expect_ingest(alias_foo.clone())
                .make_expect_ingest(alias_bar.clone())
                .make_expect_ingest(alias_baz.clone()),
        )
        .with_transform_elaboration_mock(
            MockTransformElaborationService::new()
                .make_expect_elaborate_transform(alias_foo_bar.clone()),
        )
        .with_transform_execution_mock(
            MockTransformExecutionService::new().make_expect_transform(alias_foo_bar.clone()),
        );
    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness.create_root_dataset(&alias_baz).await;

    let foo_bar = harness
        .create_derived_dataset(
            &alias_foo_bar,
            vec![
                foo.dataset_handle.as_local_ref(),
                bar.dataset_handle.as_local_ref(),
            ],
        )
        .await;

    let pull_responses = harness
        .use_case
        .execute_multi(
            vec![
                PullRequest::Local(baz.dataset_handle.as_local_ref()),
                PullRequest::Local(foo_bar.dataset_handle.as_local_ref()),
            ],
            PullOptions {
                recursive: true,
                ..PullOptions::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(4, pull_responses.len());

    let responses_by_name: HashMap<_, _> = pull_responses
        .into_iter()
        .map(|response| {
            assert!(response.maybe_local_ref.is_some());
            (
                response
                    .maybe_local_ref
                    .as_ref()
                    .unwrap()
                    .dataset_name()
                    .unwrap()
                    .clone(),
                response,
            )
        })
        .collect();

    assert_matches!(
        responses_by_name.get(&alias_foo.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: None,  // triggered via recursion
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(
                PollingIngestResultUpToDate { uncacheable: false }
            )))
        } if *a_local_ref == foo.dataset_handle.as_local_ref()
    );

    assert_matches!(
        responses_by_name.get(&alias_bar.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: None, // triggered via recursion
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(
                PollingIngestResultUpToDate { uncacheable: false }
            )))
        } if *a_local_ref == bar.dataset_handle.as_local_ref()
    );

    assert_matches!(
        responses_by_name.get(&alias_baz.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(
                PollingIngestResultUpToDate { uncacheable: false }
            )))
        } if *a_local_ref == baz.dataset_handle.as_local_ref()
    );

    assert_matches!(
        responses_by_name.get(&alias_foo_bar.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::Transform))
        } if *a_local_ref == foo_bar.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_all_owned() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let alias_foo_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo-bar"));
    let alias_foo_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo-baz"));

    let baz_ref_remote = odf::DatasetRefRemote::Alias(odf::DatasetAliasRemote {
        repo_name: odf::RepoName::new_unchecked(REMOTE_REPO_NAME_STR),
        account_name: None,
        dataset_name: alias_baz.dataset_name.clone(),
    });

    let mocks = PullUseCaseHarnessMocks::default()
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_foo.clone(), alias_bar.clone(), alias_baz.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_foo_bar.clone(), alias_foo_baz.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Read,
                    1,
                    HashSet::from_iter([alias_foo.clone(), alias_bar.clone(), alias_baz.clone()]),
                ),
        )
        .with_polling_ingest_mock(
            MockPollingIngestService::new()
                .make_expect_ingest(alias_foo.clone())
                .make_expect_ingest(alias_bar.clone()),
        )
        .with_sync_mock(
            MockSyncService::new().make_expect_sync_pull_from_remote_to_existing_local(
                alias_baz.clone(),
                baz_ref_remote.clone(),
                SyncResult::UpToDate,
            ),
        )
        .with_transform_elaboration_mock(
            MockTransformElaborationService::new()
                .make_expect_elaborate_transform(alias_foo_bar.clone())
                .make_expect_elaborate_transform(alias_foo_baz.clone()),
        )
        .with_transform_execution_mock(
            MockTransformExecutionService::new()
                .make_expect_transform(alias_foo_bar.clone())
                .make_expect_transform(alias_foo_baz.clone()),
        );
    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness.create_root_dataset(&alias_baz).await;

    harness.copy_dataset_to_remote_repo(&alias_baz).await;
    harness
        .get_remote_aliases(&baz)
        .await
        .add(&baz_ref_remote, RemoteAliasKind::Pull)
        .await
        .unwrap();

    let foo_bar = harness
        .create_derived_dataset(
            &alias_foo_bar,
            vec![
                foo.dataset_handle.as_local_ref(),
                bar.dataset_handle.as_local_ref(),
            ],
        )
        .await;
    let foo_baz = harness
        .create_derived_dataset(
            &alias_foo_baz,
            vec![
                foo.dataset_handle.as_local_ref(),
                baz.dataset_handle.as_local_ref(),
            ],
        )
        .await;

    let pull_responses = harness
        .use_case
        .execute_all_owned(PullOptions::default(), None)
        .await
        .unwrap();
    assert_eq!(5, pull_responses.len());

    let responses_by_name: HashMap<_, _> = pull_responses
        .into_iter()
        .map(|response| {
            assert!(response.maybe_local_ref.is_some());
            (
                response
                    .maybe_local_ref
                    .as_ref()
                    .unwrap()
                    .dataset_name()
                    .unwrap()
                    .clone(),
                response,
            )
        })
        .collect();

    assert_matches!(
        responses_by_name.get(&alias_foo.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(
                PollingIngestResultUpToDate { uncacheable: false }
            )))
        } if *a_local_ref == foo.dataset_handle.as_local_ref()
    );

    assert_matches!(
        responses_by_name.get(&alias_bar.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(
                PollingIngestResultUpToDate { uncacheable: false }
            )))
        } if *a_local_ref == bar.dataset_handle.as_local_ref()
    );

    assert_matches!(
        responses_by_name.get(&alias_baz.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: Some(a_remote_ref),
            result: Ok(PullResult::UpToDate(PullResultUpToDate::Sync))
        } if *a_local_ref == baz.dataset_handle.as_local_ref() && *a_remote_ref == baz_ref_remote
    );

    assert_matches!(
        responses_by_name.get(&alias_foo_bar.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::Transform))
        } if *a_local_ref == foo_bar.dataset_handle.as_local_ref()
    );

    assert_matches!(
        responses_by_name.get(&alias_foo_baz.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::Transform))
        } if *a_local_ref == foo_baz.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_authorization_issue() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let mocks = PullUseCaseHarnessMocks::default().with_authorizer_mock(
        MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
            DatasetAction::Write,
            1,
            HashSet::from_iter([alias_foo.clone(), alias_baz.clone()]),
        ),
    );
    let harness = PullUseCaseHarness::new(mocks);

    let _foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let _baz = harness.create_root_dataset(&alias_baz).await;

    let mut pull_responses = harness
        .use_case
        .execute_multi(
            vec![
                PullRequest::local(alias_foo.as_local_ref()),
                PullRequest::local(alias_bar.as_local_ref()),
                PullRequest::local(alias_baz.as_local_ref()),
            ],
            PullOptions::default(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(1, pull_responses.len());

    assert_matches!(
        pull_responses.remove(0),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Err(PullError::Access(_))
        } if a_local_ref == bar.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static REMOTE_REPO_NAME_STR: &str = "remote";

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct PullUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn PullDatasetUseCase>,
    remote_aliases_registry: Arc<dyn RemoteAliasesRegistry>,
    remote_repo_name: odf::RepoName,
    remote_tmp_dir: TempDir,
}

impl PullUseCaseHarness {
    fn new(mocks: PullUseCaseHarnessMocks) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_authorizer(Some(mocks.mock_dataset_action_authorizer)),
        );

        let repos_dir = base_use_case_harness.temp_dir_path().join("repos");
        std::fs::create_dir(&repos_dir).unwrap();

        let catalog = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog())
            .add::<PullDatasetUseCaseImpl>()
            .add::<PullRequestPlannerImpl>()
            .add::<TransformRequestPlannerImpl>()
            .add_value(mocks.mock_polling_ingest_service)
            .bind::<dyn PollingIngestService, MockPollingIngestService>()
            .add_value(mocks.mock_transform_elaboration_service)
            .bind::<dyn TransformElaborationService, MockTransformElaborationService>()
            .add_value(mocks.mock_transform_execution_service)
            .bind::<dyn TransformExecutor, MockTransformExecutionService>()
            .add_value(mocks.mock_sync_service)
            .bind::<dyn SyncService, MockSyncService>()
            .add::<SyncRequestBuilder>()
            .add::<DatasetFactoryImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add_value(RemoteRepositoryRegistryImpl::create(repos_dir).unwrap())
            .bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryImpl>()
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add_value(IpfsGateway::default())
            .build();

        let use_case = catalog.get_one().unwrap();
        let remote_aliases_registry = catalog.get_one().unwrap();

        let remote_tmp_dir = tempfile::tempdir().unwrap();
        let remote_repo_url = Url::from_directory_path(remote_tmp_dir.path()).unwrap();

        let remote_repo_name = odf::RepoName::new_unchecked(REMOTE_REPO_NAME_STR);
        let remote_repo_registry = catalog.get_one::<dyn RemoteRepositoryRegistry>().unwrap();
        remote_repo_registry
            .add_repository(&remote_repo_name, remote_repo_url)
            .unwrap();

        Self {
            base_use_case_harness,
            use_case,
            remote_aliases_registry,
            remote_repo_name,
            remote_tmp_dir,
        }
    }

    async fn get_remote_aliases(
        &self,
        created: &odf::CreateDatasetResult,
    ) -> Box<dyn RemoteAliases> {
        self.remote_aliases_registry
            .get_remote_aliases(&created.dataset_handle)
            .await
            .unwrap()
    }

    async fn copy_dataset_to_remote_repo(&self, dataset_alias: &odf::DatasetAlias) {
        let src_path = self
            .base_use_case_harness
            .temp_dir_path()
            .join("datasets")
            .join(&dataset_alias.dataset_name);

        let dst_path = self.remote_tmp_dir.path().join(&dataset_alias.dataset_name);

        tokio::fs::create_dir_all(&dst_path).await.unwrap();
        let copy_options = fs_extra::dir::CopyOptions::new().content_only(true);
        fs_extra::dir::copy(src_path, dst_path, &copy_options).unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PullUseCaseHarnessMocks {
    mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
    mock_polling_ingest_service: MockPollingIngestService,
    mock_transform_elaboration_service: MockTransformElaborationService,
    mock_transform_execution_service: MockTransformExecutionService,
    mock_sync_service: MockSyncService,
}

impl PullUseCaseHarnessMocks {
    fn with_authorizer_mock(
        self,
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
    ) -> Self {
        Self {
            mock_dataset_action_authorizer,
            ..self
        }
    }

    fn with_polling_ingest_mock(
        self,
        mock_polling_ingest_service: MockPollingIngestService,
    ) -> Self {
        Self {
            mock_polling_ingest_service,
            ..self
        }
    }

    fn with_transform_elaboration_mock(
        self,
        mock_transform_elaboration_service: MockTransformElaborationService,
    ) -> Self {
        Self {
            mock_transform_elaboration_service,
            ..self
        }
    }

    fn with_transform_execution_mock(
        self,
        mock_transform_execution_service: MockTransformExecutionService,
    ) -> Self {
        Self {
            mock_transform_execution_service,
            ..self
        }
    }

    fn with_sync_mock(self, mock_sync_service: MockSyncService) -> Self {
        Self {
            mock_sync_service,
            ..self
        }
    }
}

impl Default for PullUseCaseHarnessMocks {
    fn default() -> Self {
        Self {
            mock_dataset_action_authorizer: MockDatasetActionAuthorizer::new(),
            mock_polling_ingest_service: MockPollingIngestService::new(),
            mock_transform_elaboration_service: MockTransformElaborationService::new(),
            mock_transform_execution_service: MockTransformExecutionService::new(),
            mock_sync_service: MockSyncService::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
