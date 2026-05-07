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

use chrono::{TimeZone, Utc};
use kamu_core::MockDidGenerator;
use kamu_datasets::{
    DatasetAction,
    DeleteDatasetError,
    DeleteDatasetPlan,
    DeleteDatasetPlanEvaluationError,
    DeleteDatasetPlanTarget,
    DeleteDatasetPlanningError,
    DeleteDatasetUseCase,
};
use kamu_datasets_services::DeleteDatasetUseCaseImpl;
use kamu_datasets_services::testing::MockDatasetActionAuthorizer;
use time_source::{SystemTimeSourceProvider, SystemTimeSourceStub};

use super::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_success_via_plan() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, foo_id) = odf::DatasetID::new_generated_ed25519();

    let harness = DeleteUseCaseHarness::new(
        MockDatasetActionAuthorizer::allowing(),
        Some(MockDidGenerator::predefined_dataset_ids(vec![
            foo_id.clone(),
        ])),
    )
    .await;

    let foo = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness.reset_collected_outbox_messages();

    harness
        .execute_plan(vec![foo.dataset_handle], false)
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );

    // Note: the stability of these identifiers is ensured via
    //  predefined dataset ID and stubbed system time
    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Lifecycle Messages: 1
              Deleted {
                Dataset ID: {foo_id}
              }
            "#
        )
        .replace("{foo_id}", foo_id.to_string().as_str()),
        harness.collected_outbox_messages()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_success_via_resolved_handle_plan() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, foo_id) = odf::DatasetID::new_generated_ed25519();

    let harness = DeleteUseCaseHarness::new(
        MockDatasetActionAuthorizer::allowing(),
        Some(MockDidGenerator::predefined_dataset_ids(vec![
            foo_id.clone(),
        ])),
    )
    .await;

    let foo = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness.reset_collected_outbox_messages();

    harness
        .execute_plan(vec![foo.dataset_handle.clone()], false)
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );

    // Note: the stability of these identifiers is ensured via
    //  predefined dataset ID and stubbed system time
    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Lifecycle Messages: 1
              Deleted {
                Dataset ID: {foo_id}
              }
            "#
        )
        .replace("{foo_id}", foo_id.to_string().as_str()),
        harness.collected_outbox_messages()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_execute_plan_is_idempotent_for_stale_plan() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = DeleteUseCaseHarness::new(MockDatasetActionAuthorizer::allowing(), None).await;

    let foo = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness.reset_collected_outbox_messages();

    let plan = harness
        .use_case
        .plan_delete(vec![foo.dataset_handle.clone()], false)
        .await
        .unwrap()
        .into_executable_plan(false)
        .unwrap();

    let first_summary = harness.use_case.execute_plan(plan).await.unwrap();
    let second_plan = DeleteDatasetPlan {
        authorized_targets: first_summary
            .deleted_dataset_handles
            .iter()
            .cloned()
            .map(|dataset_handle| DeleteDatasetPlanTarget { dataset_handle })
            .collect(),
    };
    let second_summary = harness.use_case.execute_plan(second_plan).await.unwrap();

    assert_eq!(
        vec![foo.dataset_handle.clone()],
        first_summary.deleted_dataset_handles
    );
    assert_eq!(
        vec![foo.dataset_handle],
        second_summary.deleted_dataset_handles
    );
    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_plan_recursive_not_found() {
    let harness = DeleteUseCaseHarness::new(MockDatasetActionAuthorizer::new(), None).await;

    let missing_dataset_handle = odf::DatasetHandle::new(
        odf::DatasetID::new_seeded_ed25519(b"missing"),
        odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("missing")),
        odf::DatasetKind::Root,
    );

    assert_matches!(
        harness
            .use_case
            .plan_delete(vec![missing_dataset_handle], true)
            .await,
        Err(DeleteDatasetPlanningError::Internal(_))
    );

    pretty_assertions::assert_eq!("", harness.collected_outbox_messages());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_unauthorized() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = DeleteUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
            DatasetAction::Own,
            1,
            HashSet::new(),
        ),
        Some(MockDidGenerator::predefined_dataset_ids(vec![
            dataset_id_foo,
        ])),
    )
    .await;

    let foo = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness.reset_collected_outbox_messages();

    assert_matches!(
        harness.execute_plan(vec![foo.dataset_handle], false,).await,
        Err(DeleteDatasetPlanEvaluationError::Access(_))
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));

    pretty_assertions::assert_eq!("", harness.collected_outbox_messages());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_respects_dangling_refs() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let harness = DeleteUseCaseHarness::new(MockDatasetActionAuthorizer::allowing(), None).await;

    let root = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    let derived = harness
        .create_derived_dataset(&harness.catalog, &alias_bar, vec![alias_foo.as_local_ref()])
        .await;
    harness.reset_collected_outbox_messages();

    assert_matches!(
        harness
            .execute_plan(
                vec![root.dataset_handle.clone()],
                false,
            )
            .await,
        Err(DeleteDatasetPlanEvaluationError::DanglingReference(e)) if e.children == vec![derived.dataset_handle.clone()]
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));

    harness
        .execute_plan(vec![derived.dataset_handle.clone()], false)
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(
        harness.check_dataset_exists(&alias_bar).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );

    harness
        .execute_plan(vec![root.dataset_handle.clone()], false)
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );
    assert_matches!(
        harness.check_dataset_exists(&alias_bar).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Lifecycle Messages: 2
              Deleted {
                Dataset ID: {bar_id}
              }
              Deleted {
                Dataset ID: {foo_id}
              }
            "#
        )
        .replace("{foo_id}", root.dataset_handle.id.to_string().as_str())
        .replace("{bar_id}", derived.dataset_handle.id.to_string().as_str()),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_plan_delete_selected_unauthorized() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = DeleteUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
            DatasetAction::Own,
            1,
            HashSet::new(),
        ),
        None,
    )
    .await;

    let foo = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness.reset_collected_outbox_messages();

    let plan = harness
        .use_case
        .plan_delete(vec![foo.dataset_handle.clone()], false)
        .await
        .unwrap();

    assert!(plan.plan.authorized_targets.is_empty());
    assert_eq!(plan.issues.unauthorized_selected_handles.len(), 1);
    assert!(plan.issues.unauthorized_recursive_handles.is_empty());
    assert!(plan.issues.dangling_references.is_empty());

    assert_matches!(
        plan.into_executable_plan(false),
        Err(DeleteDatasetPlanEvaluationError::Access(_))
    );
    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    pretty_assertions::assert_eq!("", harness.collected_outbox_messages());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_plan_delete_recursive_orders_authorized_targets() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let harness = DeleteUseCaseHarness::new(MockDatasetActionAuthorizer::allowing(), None).await;

    let root = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness
        .create_derived_dataset(&harness.catalog, &alias_bar, vec![alias_foo.as_local_ref()])
        .await;

    let plan = harness
        .use_case
        .plan_delete(vec![root.dataset_handle], true)
        .await
        .unwrap();

    let planned_aliases = plan
        .plan
        .authorized_targets
        .iter()
        .map(|target| target.dataset_handle.alias.to_string())
        .collect::<Vec<_>>();

    pretty_assertions::assert_eq!(vec!["bar", "foo"], planned_aliases);
    assert!(plan.issues.unauthorized_selected_handles.is_empty());
    assert!(plan.issues.unauthorized_recursive_handles.is_empty());
    assert!(plan.issues.dangling_references.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_plan_delete_recursive_foreign_downstream_blocks_without_force() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let harness = DeleteUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
            DatasetAction::Own,
            1,
            HashSet::from([alias_foo.clone()]),
        ),
        None,
    )
    .await;

    let root = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness
        .create_derived_dataset(&harness.catalog, &alias_bar, vec![alias_foo.as_local_ref()])
        .await;
    harness.reset_collected_outbox_messages();

    let plan = harness
        .use_case
        .plan_delete(vec![root.dataset_handle], true)
        .await
        .unwrap();

    assert_eq!(plan.plan.authorized_targets.len(), 1);
    assert_eq!(plan.issues.unauthorized_recursive_handles.len(), 1);
    assert!(plan.issues.unauthorized_selected_handles.is_empty());
    assert!(plan.issues.dangling_references.is_empty());

    assert_matches!(
        plan.into_executable_plan(false),
        Err(DeleteDatasetPlanEvaluationError::Access(_))
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));
    pretty_assertions::assert_eq!("", harness.collected_outbox_messages());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_plan_delete_recursive_force_orphans_foreign_downstream() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let harness = DeleteUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
            DatasetAction::Own,
            1,
            HashSet::from([alias_foo.clone()]),
        ),
        None,
    )
    .await;

    let root = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness
        .create_derived_dataset(&harness.catalog, &alias_bar, vec![alias_foo.as_local_ref()])
        .await;
    harness.reset_collected_outbox_messages();

    let plan = harness
        .use_case
        .plan_delete(vec![root.dataset_handle], true)
        .await
        .unwrap();

    let plan = plan.into_executable_plan(true).unwrap();

    harness.use_case.execute_plan(plan).await.unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_plan_delete_non_recursive_dangling_refs_allowed_with_force() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let harness = DeleteUseCaseHarness::new(MockDatasetActionAuthorizer::allowing(), None).await;

    let root = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness
        .create_derived_dataset(&harness.catalog, &alias_bar, vec![alias_foo.as_local_ref()])
        .await;

    let plan = harness
        .use_case
        .plan_delete(vec![root.dataset_handle], false)
        .await
        .unwrap();

    assert_eq!(plan.plan.authorized_targets.len(), 1);
    assert_eq!(plan.issues.dangling_references.len(), 1);

    let plan = plan.into_executable_plan(true).unwrap();

    harness.use_case.execute_plan(plan).await.unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct DeleteUseCaseHarness {
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    use_case: Arc<dyn DeleteDatasetUseCase>,
    catalog: dill::Catalog,
}

impl DeleteUseCaseHarness {
    async fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        maybe_mock_did_generator: Option<MockDidGenerator>,
    ) -> Self {
        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                system_time_source_provider: SystemTimeSourceProvider::Stub(
                    SystemTimeSourceStub::new_set(
                        Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                    ),
                ),
                maybe_mock_did_generator,
                maybe_mock_dataset_action_authorizer: Some(mock_dataset_action_authorizer),
                ..DatasetBaseUseCaseHarnessOpts::default()
            })
            .await;

        let catalog =
            dill::CatalogBuilder::new_chained(dataset_base_use_case_harness.intermediate_catalog())
                .add::<DeleteDatasetUseCaseImpl>()
                .build();

        Self {
            dataset_base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
            catalog,
        }
    }

    async fn execute_plan(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        allow_orphan: bool,
    ) -> Result<(), DeleteDatasetPlanEvaluationError> {
        let plan = self
            .use_case
            .plan_delete(dataset_handles, false)
            .await
            .map_err(|e| match e {
                DeleteDatasetPlanningError::Internal(e) => {
                    DeleteDatasetPlanEvaluationError::Internal(e)
                }
            })?
            .into_executable_plan(allow_orphan)?;

        self.use_case
            .execute_plan(plan)
            .await
            .map_err(|e| match e {
                DeleteDatasetError::Internal(e) => DeleteDatasetPlanEvaluationError::Internal(e),
            })?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
