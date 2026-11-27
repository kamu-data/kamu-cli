// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_resources::{
    ApplyManifestApplicationDecision,
    ApplyManifestPlanningDecision,
    ApplyManifestRejection,
    ApplyResourceOutcome,
    ApplyResourceRejectionCategory,
    ResourceID,
    ResourcePhase,
    ResourceRef,
    ResourceSelector,
    ResourceVisibility,
    TypeName,
    WARNING_CODE_REPLACES_RESOURCE_BEING_DELETED,
};
use kamu_resources_facade::{
    ApplyManifestRequest,
    ResourceDeletion,
    ResourceFacade,
    ResourceLookupProblem,
    ResourceManifestFormat,
    SearchResourcesRequest,
    SpecViewOpts,
};
use pretty_assertions::{assert_eq, assert_matches};
use uuid::Uuid;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_CANONICAL_SELECTOR,
    assert_applied_outcome,
    assert_single_batch_problem,
    assert_single_batch_success,
    create_variable_set,
    create_variable_set_with_labels,
    label_filter,
    variable_set_manifest_json,
    variable_set_manifest_json_with_id,
    variable_set_ref_by_id,
    variable_set_ref_by_name,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-130
contract_test!(delete_by_name, super::test_delete_by_name);

pub async fn test_delete_by_name(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "del-name-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let deleted_uid = assert_single_batch_success(
        facade
            .delete(vec![variable_set_ref_by_name("del-name-test")])
            .await
            .unwrap(),
    );

    // The controllers clean up after the request
    assert_eq!(
        deleted_uid,
        ResourceDeletion {
            id,
            phase: ResourcePhase::Deleting
        }
    );
    h.settle().await;

    // Resource must not be found by name
    let get_by_name = facade
        .get(
            vec![variable_set_ref_by_name("del-name-test")],
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await
        .unwrap();
    let get_err = assert_single_batch_problem(get_by_name);
    assert_matches!(
        get_err,
        ResourceLookupProblem::NameNotFound(_),
        "deleted resource must not be found by name"
    );

    // Its id finds the resource in any state
    let resource = assert_single_batch_success(
        facade
            .get(
                vec![variable_set_ref_by_id(id)],
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .unwrap(),
    );
    assert_eq!(resource.status.phase, ResourcePhase::Deleted);
    assert!(resource.headers.deleted_at.is_some());

    // Resource must not appear in list
    let list = facade
        .search(SearchResourcesRequest::new(
            vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            PaginationOpts {
                limit: 1000,
                offset: 0,
            },
        ))
        .await
        .unwrap();
    assert!(
        !list.items.iter().any(|s| s.id == id),
        "deleted resource must not appear in list"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-131
contract_test!(delete_by_uid, super::test_delete_by_uid);

pub async fn test_delete_by_uid(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "del-id-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let deleted_uid = assert_single_batch_success(
        facade
            .delete(vec![variable_set_ref_by_id(id)])
            .await
            .unwrap(),
    );

    assert_eq!(deleted_uid.id, id, "deleted id must match created id");

    let get_by_name = facade
        .get(
            vec![variable_set_ref_by_name("del-id-test")],
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await
        .unwrap();
    let get_err = assert_single_batch_problem(get_by_name);
    assert_matches!(
        get_err,
        ResourceLookupProblem::NameNotFound(_),
        "resource must not be found by name after delete by id"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-132
contract_test!(
    delete_missing_name_returns_not_found,
    super::test_delete_missing_name_returns_not_found
);

pub async fn test_delete_missing_name_returns_not_found(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .delete(vec![variable_set_ref_by_name("no-such-delete")])
        .await
        .unwrap();
    let err = assert_single_batch_problem(result);
    assert_matches!(
        err,
        ResourceLookupProblem::NameNotFound(_),
        "expected NameNotFound"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-133
contract_test!(
    delete_missing_uid_returns_not_found,
    super::test_delete_missing_uid_returns_not_found
);

pub async fn test_delete_missing_uid_returns_not_found(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let absent_uid = ResourceID::new(Uuid::new_v4());

    let result = facade
        .delete(vec![variable_set_ref_by_id(absent_uid)])
        .await
        .unwrap();
    let err = assert_single_batch_problem(result);
    assert_matches!(
        err,
        ResourceLookupProblem::IDNotFound(_),
        "expected IDNotFound"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-134
contract_test!(
    delete_wrong_schema_returns_mismatch,
    super::test_delete_wrong_schema_returns_mismatch
);

pub async fn test_delete_wrong_schema_returns_mismatch(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "del-api-ver").await;
    let facade = h.facade_for(TestAccount::Alice);

    let wrong_schema_selector = ResourceRef {
        account: None,
        r#type: Some(
            SECRET_SET_CANONICAL_SELECTOR
                .parse::<TypeName>()
                .unwrap()
                .into(),
        ),
        id: Some(id),
        did: None,
        name: None,
    };
    let result = facade
        .delete(vec![wrong_schema_selector.clone()])
        .await
        .unwrap();
    let err = assert_single_batch_problem(result);
    assert_matches!(
        err,
        ResourceLookupProblem::SchemaMismatch(_),
        "expected SchemaMismatch"
    );

    let result = facade.delete(vec![wrong_schema_selector]).await.unwrap();
    let err = assert_single_batch_problem(result);
    assert_matches!(
        err,
        ResourceLookupProblem::SchemaMismatch(_),
        "expected SchemaMismatch"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-135
// Deleting a resource in one account must not affect a resource with the
// same name in another account.
contract_test!(
    delete_is_account_scoped,
    super::test_delete_is_account_scoped
);

pub async fn test_delete_is_account_scoped(h: &impl FacadeContractHarness) {
    let alice_uid = create_variable_set(h, TestAccount::Alice, "scoped-del").await;

    // Create same-named resource for Bob.
    let bob_uid = {
        let facade = h.facade_for(TestAccount::Bob);
        let manifest = variable_set_manifest_json("scoped-del", None, &[("K", "v")]);
        let decision = facade
            .apply_manifest(ApplyManifestRequest {
                format: ResourceManifestFormat::Json,
                manifest,
            })
            .await
            .unwrap();
        assert_applied_outcome(&decision, ApplyResourceOutcome::Created)
            .headers
            .id
    };

    // Delete Alice's resource.
    let alice_facade = h.facade_for(TestAccount::Alice);
    let deleted_uid = assert_single_batch_success(
        alice_facade
            .delete(vec![variable_set_ref_by_name("scoped-del")])
            .await
            .unwrap(),
    );
    assert_eq!(deleted_uid.id, alice_uid);

    // Alice's resource must be gone.
    let alice_get = alice_facade
        .get(
            vec![variable_set_ref_by_name("scoped-del")],
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await
        .unwrap();
    let alice_err = assert_single_batch_problem(alice_get);
    assert_matches!(
        alice_err,
        ResourceLookupProblem::NameNotFound(_),
        "Alice's resource must be gone after delete"
    );

    // Bob's resource must still exist.
    let bob_facade = h.facade_for(TestAccount::Bob);
    let bob_view = assert_single_batch_success(
        bob_facade
            .get(
                vec![variable_set_ref_by_name("scoped-del")],
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .unwrap(),
    );
    assert_eq!(bob_view.headers.id, bob_uid);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-136
// Deleting a deleted resource again must return a not-found error.
contract_test!(
    repeated_delete_is_deterministic,
    super::test_repeated_delete_is_deterministic
);

pub async fn test_repeated_delete_is_deterministic(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "repeat-del").await;
    let facade = h.facade_for(TestAccount::Alice);

    // First delete succeeds.
    let deleted = assert_single_batch_success(
        facade
            .delete(vec![variable_set_ref_by_id(id)])
            .await
            .unwrap(),
    );
    assert_eq!(deleted.id, id);
    h.settle().await;

    // Second delete must return not-found.
    let result = facade
        .delete(vec![variable_set_ref_by_id(id)])
        .await
        .unwrap();
    let err = assert_single_batch_problem(result);
    assert_matches!(
        err,
        ResourceLookupProblem::IDNotFound(_),
        "second delete must return IDNotFound"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-137
contract_test!(
    delete_waits_for_cleanup,
    super::test_delete_reports_deleting_until_cleanup,
    messaging_outbox::OutboxProvider::Dispatching
);

pub async fn test_delete_reports_deleting_until_cleanup(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "del-phases").await;
    let facade = h.facade_for(TestAccount::Alice);

    let deletion = assert_single_batch_success(
        facade
            .delete(vec![variable_set_ref_by_name("del-phases")])
            .await
            .unwrap(),
    );
    assert_eq!(
        deletion,
        ResourceDeletion {
            id,
            phase: ResourcePhase::Deleting
        }
    );

    // Found by id while controllers clean up
    let resource = assert_single_batch_success(
        facade
            .get(
                vec![variable_set_ref_by_id(id)],
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .unwrap(),
    );
    assert_eq!(resource.headers.name.as_str(), "del-phases");
    assert_eq!(resource.status.phase, ResourcePhase::Deleting);
    assert!(resource.headers.deletion_requested_at.is_some());
    assert!(resource.headers.deleted_at.is_none());

    // Hidden from names and searches unless asked for
    assert_matches!(
        assert_single_batch_problem(
            facade
                .get_handles(
                    vec![variable_set_ref_by_name("del-phases")],
                    ResourceVisibility::Active
                )
                .await
                .unwrap()
        ),
        ResourceLookupProblem::NameNotFound(_)
    );
    assert_eq!(
        found_by_name(
            facade.as_ref(),
            "del-phases",
            ResourceVisibility::IncludeDeleting
        )
        .await,
        vec![(0, id)]
    );
    assert_eq!(
        searched_ids(facade.as_ref(), ResourceVisibility::Active).await,
        Vec::<ResourceID>::new()
    );
    assert_eq!(
        searched_ids(facade.as_ref(), ResourceVisibility::IncludeDeleting).await,
        vec![id]
    );
    assert_eq!(
        searched_summaries(facade.as_ref(), ResourceVisibility::Active).await,
        Vec::<(ResourceID, Option<ResourcePhase>)>::new()
    );
    assert_eq!(
        searched_summaries(facade.as_ref(), ResourceVisibility::IncludeDeleting).await,
        vec![(id, Some(ResourcePhase::Deleting))]
    );

    h.settle().await;

    let resource = assert_single_batch_success(
        facade
            .get(
                vec![variable_set_ref_by_id(id)],
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .unwrap(),
    );
    assert_eq!(resource.headers.name.as_str(), "del-phases");
    assert_eq!(resource.status.phase, ResourcePhase::Deleted);
    assert!(resource.headers.deleted_at.is_some());

    for visibility in [
        ResourceVisibility::Active,
        ResourceVisibility::IncludeDeleting,
    ] {
        assert_eq!(
            found_by_name(facade.as_ref(), "del-phases", visibility).await,
            Vec::<(usize, ResourceID)>::new(),
            "{visibility:?}"
        );
        assert_eq!(
            searched_ids(facade.as_ref(), visibility).await,
            Vec::<ResourceID>::new(),
            "{visibility:?}"
        );
        assert_eq!(
            searched_summaries(facade.as_ref(), visibility).await,
            Vec::<(ResourceID, Option<ResourcePhase>)>::new(),
            "{visibility:?}"
        );
    }
    for visibility in [
        ResourceVisibility::IncludeDeleted,
        ResourceVisibility::DeletedOnly,
    ] {
        assert_eq!(
            found_by_name(facade.as_ref(), "del-phases", visibility).await,
            vec![(0, id)],
            "{visibility:?}"
        );
        assert_eq!(
            searched_ids(facade.as_ref(), visibility).await,
            vec![id],
            "{visibility:?}"
        );
        assert_eq!(
            searched_summaries(facade.as_ref(), visibility).await,
            vec![(id, Some(ResourcePhase::Deleted))],
            "{visibility:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-138
contract_test!(
    name_of_resource_being_deleted_can_be_reused,
    super::test_name_of_resource_being_deleted_can_be_reused,
    messaging_outbox::OutboxProvider::Dispatching
);

pub async fn test_name_of_resource_being_deleted_can_be_reused(h: &impl FacadeContractHarness) {
    let old_id = create_variable_set(h, TestAccount::Alice, "reused").await;
    let facade = h.facade_for(TestAccount::Alice);
    facade
        .delete(vec![variable_set_ref_by_id(old_id)])
        .await
        .unwrap();

    let (new_id, replacement_warnings) =
        create_variable_set_with_replacement_warnings(facade.as_ref(), "reused", "v2").await;
    assert_ne!(new_id, old_id);
    assert_eq!(
        replacement_warnings,
        vec![format!(
            "Resource replaces a resource that is still being deleted ({old_id})"
        )]
    );

    // The live resource comes first
    assert_eq!(
        found_by_name(
            facade.as_ref(),
            "reused",
            ResourceVisibility::IncludeDeleting
        )
        .await,
        vec![(0, new_id), (0, old_id)]
    );
    assert_eq!(
        got_by_name(facade.as_ref(), "reused").await,
        vec![
            (0, new_id, ResourcePhase::Pending),
            (0, old_id, ResourcePhase::Deleting)
        ]
    );
    // Rendered manifests carry no id: the variable value tells them apart
    assert_eq!(
        rendered_variable_by_name(facade.as_ref(), "reused", "K").await,
        vec![(0, "v2".to_string()), (0, "v".to_string())]
    );

    h.settle().await;

    assert_eq!(
        found_by_name(
            facade.as_ref(),
            "reused",
            ResourceVisibility::IncludeDeleting
        )
        .await,
        vec![(0, new_id)]
    );

    // Deleted resources keep the name too, but are not replaced
    facade
        .delete(vec![variable_set_ref_by_id(new_id)])
        .await
        .unwrap();
    h.settle().await;

    let (newest_id, replacement_warnings) =
        create_variable_set_with_replacement_warnings(facade.as_ref(), "reused", "v3").await;
    assert_eq!(replacement_warnings, Vec::<String>::new());
    assert_eq!(
        found_by_name(
            facade.as_ref(),
            "reused",
            ResourceVisibility::IncludeDeleting
        )
        .await,
        vec![(0, newest_id)]
    );
    // After the live resource, the newest deletion comes first
    assert_eq!(
        found_by_name(
            facade.as_ref(),
            "reused",
            ResourceVisibility::IncludeDeleted
        )
        .await,
        vec![(0, newest_id), (0, new_id), (0, old_id)]
    );
    assert_eq!(
        found_by_name(facade.as_ref(), "reused", ResourceVisibility::DeletedOnly).await,
        vec![(0, new_id), (0, old_id)]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-139
contract_test!(
    repeated_delete_of_resource_being_deleted_reports_deleting,
    super::test_repeated_delete_of_resource_being_deleted_reports_deleting,
    messaging_outbox::OutboxProvider::Dispatching
);

pub async fn test_repeated_delete_of_resource_being_deleted_reports_deleting(
    h: &impl FacadeContractHarness,
) {
    let id = create_variable_set(h, TestAccount::Alice, "retry-del").await;
    let facade = h.facade_for(TestAccount::Alice);
    let deleting = ResourceDeletion {
        id,
        phase: ResourcePhase::Deleting,
    };

    facade
        .delete(vec![variable_set_ref_by_id(id)])
        .await
        .unwrap();

    assert_eq!(
        assert_single_batch_success(
            facade
                .delete(vec![variable_set_ref_by_id(id)])
                .await
                .unwrap()
        ),
        deleting
    );
    // Its name no longer finds it
    assert_matches!(
        assert_single_batch_problem(
            facade
                .delete(vec![variable_set_ref_by_name("retry-del")])
                .await
                .unwrap()
        ),
        ResourceLookupProblem::NameNotFound(_)
    );

    h.settle().await;

    assert_matches!(
        assert_single_batch_problem(
            facade
                .delete(vec![variable_set_ref_by_id(id)])
                .await
                .unwrap()
        ),
        ResourceLookupProblem::IDNotFound(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-182
contract_test!(
    apply_by_id_of_resource_being_deleted_is_rejected,
    super::test_apply_by_id_of_resource_being_deleted_is_rejected,
    messaging_outbox::OutboxProvider::Dispatching
);

pub async fn test_apply_by_id_of_resource_being_deleted_is_rejected(
    h: &impl FacadeContractHarness,
) {
    let id = create_variable_set(h, TestAccount::Alice, "apply-deleting").await;
    let facade = h.facade_for(TestAccount::Alice);
    facade
        .delete(vec![variable_set_ref_by_id(id)])
        .await
        .unwrap();

    let request = ApplyManifestRequest {
        format: ResourceManifestFormat::Json,
        manifest: variable_set_manifest_json_with_id(id, "apply-deleting", "v2"),
    };
    assert_matches!(
        facade.plan_apply_manifest(request.clone()).await,
        Ok(ApplyManifestPlanningDecision::Rejected(
            ApplyManifestRejection {
                category: ApplyResourceRejectionCategory::LifecycleRuleConflict,
                ..
            }
        ))
    );
    assert_matches!(
        facade.apply_manifest(request).await,
        Ok(ApplyManifestApplicationDecision::Rejected(
            ApplyManifestRejection {
                category: ApplyResourceRejectionCategory::LifecycleRuleConflict,
                ..
            }
        ))
    );

    // The rejected apply leaves the resource being deleted, spec untouched
    assert_eq!(
        rendered_variable_by_name(facade.as_ref(), "apply-deleting", "K").await,
        vec![(0, "v".to_string())]
    );
    assert_eq!(
        got_by_name(facade.as_ref(), "apply-deleting").await,
        vec![(0, id, ResourcePhase::Deleting)]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-183
contract_test!(
    apply_by_id_of_deleted_resource_is_not_found,
    super::test_apply_by_id_of_deleted_resource_is_not_found,
    messaging_outbox::OutboxProvider::Dispatching
);

pub async fn test_apply_by_id_of_deleted_resource_is_not_found(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "apply-deleted").await;
    let facade = h.facade_for(TestAccount::Alice);
    facade
        .delete(vec![variable_set_ref_by_id(id)])
        .await
        .unwrap();
    h.settle().await;

    let request = ApplyManifestRequest {
        format: ResourceManifestFormat::Json,
        manifest: variable_set_manifest_json_with_id(id, "apply-deleted", "v2"),
    };
    // A remote facade reports a single apply as an untyped request error
    let not_found = format!("Resource with id {id} was not found");
    let error = facade
        .plan_apply_manifest(request.clone())
        .await
        .unwrap_err();
    assert!(error.to_string().contains(&not_found), "{error}");
    let error = facade.apply_manifest(request).await.unwrap_err();
    assert!(error.to_string().contains(&not_found), "{error}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-184
contract_test!(
    deleted_only_hides_live_resources,
    super::test_deleted_only_hides_live_resources,
    messaging_outbox::OutboxProvider::Dispatching
);

pub async fn test_deleted_only_hides_live_resources(h: &impl FacadeContractHarness) {
    let live_id = create_variable_set(h, TestAccount::Alice, "live").await;
    let deleted_id = create_variable_set(h, TestAccount::Alice, "deleted").await;
    let facade = h.facade_for(TestAccount::Alice);
    facade
        .delete(vec![variable_set_ref_by_id(deleted_id)])
        .await
        .unwrap();
    h.settle().await;
    let deleting_id = create_variable_set(h, TestAccount::Alice, "deleting").await;
    facade
        .delete(vec![variable_set_ref_by_id(deleting_id)])
        .await
        .unwrap();

    let visibility = ResourceVisibility::DeletedOnly;
    assert_eq!(
        searched_summaries(facade.as_ref(), visibility).await,
        vec![
            (deleting_id, Some(ResourcePhase::Deleting)),
            (deleted_id, Some(ResourcePhase::Deleted))
        ]
    );
    assert_eq!(
        searched_summaries(facade.as_ref(), ResourceVisibility::IncludeDeleted)
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        vec![live_id, deleting_id, deleted_id]
    );

    // Not even its id finds a live resource
    for live_ref in [
        variable_set_ref_by_id(live_id),
        variable_set_ref_by_name("live"),
    ] {
        let handles = facade
            .get_handles(vec![live_ref.clone()], visibility)
            .await
            .unwrap();
        assert_not_found(&assert_single_batch_problem(handles));

        let resources = facade
            .get(vec![live_ref.clone()], SpecViewOpts::ENCRYPTED, visibility)
            .await
            .unwrap();
        assert_not_found(&assert_single_batch_problem(resources));

        let manifests = facade
            .render_manifests(
                vec![live_ref],
                ResourceManifestFormat::Json,
                SpecViewOpts::ENCRYPTED,
                visibility,
            )
            .await
            .unwrap();
        assert_not_found(&assert_single_batch_problem(manifests));
    }

    for id in [deleting_id, deleted_id] {
        let handle = assert_single_batch_success(
            facade
                .get_handles(vec![variable_set_ref_by_id(id)], visibility)
                .await
                .unwrap(),
        );
        assert_eq!(handle.id, id);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[track_caller]
fn assert_not_found(problem: &ResourceLookupProblem) {
    assert_matches!(
        problem,
        ResourceLookupProblem::IDNotFound(_) | ResourceLookupProblem::NameNotFound(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-185
contract_test!(
    label_selectors_find_deleted_resources,
    super::test_label_selectors_find_deleted_resources,
    messaging_outbox::OutboxProvider::Dispatching
);

pub async fn test_label_selectors_find_deleted_resources(h: &impl FacadeContractHarness) {
    let labelled_id = create_variable_set_with_labels(
        h,
        TestAccount::Alice,
        "labelled",
        &[("environment", "prod".into())],
    )
    .await;
    let other_id = create_variable_set(h, TestAccount::Alice, "other").await;
    let facade = h.facade_for(TestAccount::Alice);
    facade
        .delete(vec![
            variable_set_ref_by_id(labelled_id),
            variable_set_ref_by_id(other_id),
        ])
        .await
        .unwrap();
    h.settle().await;

    let found = facade
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(label_filter(&[("environment", "prod")])),
                ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
            }],
            ..search_request(ResourceVisibility::DeletedOnly)
        })
        .await
        .unwrap();
    assert_eq!(
        found
            .items
            .into_iter()
            .map(|handle| handle.id)
            .collect::<Vec<_>>(),
        vec![labelled_id]
    );
    assert_eq!(found.total_count, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Creates a variable set with `K` set to `value`, returning its id and the
/// messages of the warnings about resources being deleted it replaces
async fn create_variable_set_with_replacement_warnings(
    facade: &dyn ResourceFacade,
    name: &str,
    value: &str,
) -> (ResourceID, Vec<String>) {
    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json(name, None, &[("K", value)]),
        })
        .await
        .unwrap();
    let id = assert_applied_outcome(&decision, ApplyResourceOutcome::Created)
        .headers
        .id;
    let replacement_warnings = decision
        .expect_applied()
        .warnings
        .into_iter()
        .filter(|warning| warning.code == WARNING_CODE_REPLACES_RESOURCE_BEING_DELETED)
        .map(|warning| warning.message)
        .collect();

    (id, replacement_warnings)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `(request_index, id)` of the resources a name resolves to
async fn found_by_name(
    facade: &dyn ResourceFacade,
    name: &str,
    visibility: ResourceVisibility,
) -> Vec<(usize, ResourceID)> {
    facade
        .get_handles(vec![variable_set_ref_by_name(name)], visibility)
        .await
        .unwrap()
        .successes
        .into_iter()
        .map(|success| (success.request_index, success.item.id))
        .collect()
}

/// `(request_index, id, phase)` of the resources a name resolves to, resources
/// being deleted included
async fn got_by_name(
    facade: &dyn ResourceFacade,
    name: &str,
) -> Vec<(usize, ResourceID, ResourcePhase)> {
    facade
        .get(
            vec![variable_set_ref_by_name(name)],
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::IncludeDeleting,
        )
        .await
        .unwrap()
        .successes
        .into_iter()
        .map(|success| {
            (
                success.request_index,
                success.item.headers.id,
                success.item.status.phase,
            )
        })
        .collect()
}

/// `(request_index, value of the variable)` of the manifests rendered for a
/// name, resources being deleted included
async fn rendered_variable_by_name(
    facade: &dyn ResourceFacade,
    name: &str,
    variable: &str,
) -> Vec<(usize, String)> {
    facade
        .render_manifests(
            vec![variable_set_ref_by_name(name)],
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::IncludeDeleting,
        )
        .await
        .unwrap()
        .successes
        .into_iter()
        .map(|success| {
            let manifest: serde_json::Value = serde_json::from_str(&success.item.manifest).unwrap();
            let value = manifest["spec"]["variables"][variable]["value"]
                .as_str()
                .unwrap()
                .to_string();
            (success.request_index, value)
        })
        .collect()
}

fn search_request(visibility: ResourceVisibility) -> SearchResourcesRequest {
    SearchResourcesRequest {
        visibility,
        ..SearchResourcesRequest::new(
            vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            PaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
    }
}

async fn searched_ids(
    facade: &dyn ResourceFacade,
    visibility: ResourceVisibility,
) -> Vec<ResourceID> {
    facade
        .search_handles(search_request(visibility))
        .await
        .unwrap()
        .items
        .into_iter()
        .map(|handle| handle.id)
        .collect()
}

async fn searched_summaries(
    facade: &dyn ResourceFacade,
    visibility: ResourceVisibility,
) -> Vec<(ResourceID, Option<ResourcePhase>)> {
    facade
        .search(search_request(visibility))
        .await
        .unwrap()
        .items
        .into_iter()
        .map(|summary| (summary.id, summary.status.and_then(|status| status.phase)))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
