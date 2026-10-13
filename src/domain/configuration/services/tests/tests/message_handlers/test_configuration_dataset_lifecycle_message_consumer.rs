// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use internal_error::InternalError;
use kamu_configuration::RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI;
use kamu_configuration_services::DatasetEnvVarMutationAdapterImpl;
use kamu_configuration_services::testing::BaseConfigurationServiceHarness;
use kamu_datasets::DatasetLifecycleMessage;
use kamu_resources::{ResourceID, ResourceSnapshot};
use messaging_outbox::MessageConsumerT;
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_dataset_deletes_auto_managed_sets_of_both_kinds() {
    let harness = ConsumerHarness::new().await;
    let dataset_id = make_dataset_id(1);

    let vars_id = harness
        .apply_variable_set_targeting(
            &harness.owner,
            &dataset_id,
            &legacy_vars_name(&dataset_id),
            [("DB_HOST", "localhost")],
            [],
        )
        .await;
    let secrets_id = harness
        .apply_secret_set_targeting(
            &harness.owner,
            &dataset_id,
            &legacy_secrets_name(&dataset_id),
            [("TOKEN", "s3cret")],
            [],
        )
        .await;

    harness.consume_deleted(&dataset_id).await.unwrap();

    // Actually tombstoned, not merely de-indexed
    assert!(
        harness.get_snapshot_by_id(&vars_id).await.is_none(),
        "auto-managed VariableSet must be deleted"
    );
    assert!(
        harness.get_snapshot_by_id(&secrets_id).await.is_none(),
        "auto-managed SecretSet must be deleted"
    );

    assert_eq!(
        harness
            .variable_sets_targeting(&harness.owner, &dataset_id)
            .await,
        Vec::<ResourceID>::new()
    );
    assert_eq!(
        harness
            .secret_sets_targeting(&harness.owner, &dataset_id)
            .await,
        Vec::<ResourceID>::new()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_dataset_strips_label_from_user_authored_variable_set() {
    let harness = ConsumerHarness::new().await;
    let dataset_id = make_dataset_id(2);

    let id = harness
        .apply_variable_set_targeting(
            &harness.owner,
            &dataset_id,
            "my-vars",
            [("DB_HOST", "localhost"), ("DB_PORT", "5432")],
            [(
                "https://kamu.dev/schemas/resource/v1alpha1/labels/Environment",
                "prod",
            )],
        )
        .await;

    let before = harness.get_snapshot_by_id(&id).await.unwrap();

    harness.consume_deleted(&dataset_id).await.unwrap();

    let after = harness
        .get_snapshot_by_id(&id)
        .await
        .expect("user-authored VariableSet must survive");

    assert!(
        !has_target_label(&after),
        "the dangling target-dataset label must be stripped"
    );
    assert_eq!(
        after.spec, before.spec,
        "the spec must be preserved byte-for-byte"
    );
    assert_eq!(
        after.headers.generation, before.headers.generation,
        "a headers-only change must not bump the generation, i.e. must not schedule reconciliation"
    );
    assert!(
        after
            .headers
            .labels
            .entries
            .keys()
            .any(|k| k.as_str().ends_with("/Environment")),
        "unrelated labels must survive"
    );

    // No longer resolvable through the label index either
    assert_eq!(
        harness
            .variable_sets_targeting(&harness.owner, &dataset_id)
            .await,
        Vec::<ResourceID>::new()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_dataset_strips_label_from_user_authored_secret_set() {
    let harness = ConsumerHarness::new().await;
    let dataset_id = make_dataset_id(3);

    let id = harness
        .apply_secret_set_targeting(
            &harness.owner,
            &dataset_id,
            "my-secrets",
            [("TOKEN", "s3cret")],
            [],
        )
        .await;

    let before = harness.get_snapshot_by_id(&id).await.unwrap();

    harness.consume_deleted(&dataset_id).await.unwrap();

    let after = harness
        .get_snapshot_by_id(&id)
        .await
        .expect("user-authored SecretSet must survive");

    assert!(!has_target_label(&after), "label must be stripped");
    assert_eq!(
        after.spec, before.spec,
        "an already-`jwe` secret must not be re-encrypted, so the ciphertext is unchanged"
    );
    assert_eq!(
        after.headers.generation, before.headers.generation,
        "no generation bump for an already-sanitized secret set"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_auto_managed_and_user_authored_are_treated_differently() {
    let harness = ConsumerHarness::new().await;
    let dataset_id = make_dataset_id(4);

    let legacy_id = harness
        .apply_variable_set_targeting(
            &harness.owner,
            &dataset_id,
            &legacy_vars_name(&dataset_id),
            [("A", "1")],
            [],
        )
        .await;
    let user_id = harness
        .apply_variable_set_targeting(&harness.owner, &dataset_id, "my-vars", [("B", "2")], [])
        .await;

    harness.consume_deleted(&dataset_id).await.unwrap();

    assert!(
        harness.get_snapshot_by_id(&legacy_id).await.is_none(),
        "the auto-managed set is deleted"
    );
    let survivor = harness
        .get_snapshot_by_id(&user_id)
        .await
        .expect("the user-authored set survives");
    assert!(!has_target_label(&survivor), "but loses the dangling label");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_legacy_name_of_another_dataset_is_stripped_not_deleted() {
    // Pins that the partition compares the full derived name, not a
    // `legacy-vars-` prefix: this resource is named after a *different*
    // dataset, so it is not ours to delete even though it carries our label.
    let harness = ConsumerHarness::new().await;
    let dataset_id = make_dataset_id(5);
    let other_dataset_id = make_dataset_id(6);

    let id = harness
        .apply_variable_set_targeting(
            &harness.owner,
            &dataset_id,
            &legacy_vars_name(&other_dataset_id),
            [("A", "1")],
            [],
        )
        .await;

    harness.consume_deleted(&dataset_id).await.unwrap();

    let survivor = harness
        .get_snapshot_by_id(&id)
        .await
        .expect("a legacy name belonging to another dataset must not be deleted");
    assert!(!has_target_label(&survivor), "only the label is retracted");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_resources_of_other_accounts_are_swept_too() {
    // The account-free property end to end. A stranger's resource pointing at
    // this dataset is still a dead link, and there is no `dataset_entries` row
    // to resolve an owner from in any case.
    let harness = ConsumerHarness::new().await;
    let dataset_id = make_dataset_id(7);

    let stranger = harness.ensure_test_account("stranger").await;

    let stranger_legacy_id = harness
        .apply_variable_set_targeting(
            &stranger,
            &dataset_id,
            &legacy_vars_name(&dataset_id),
            [("A", "1")],
            [],
        )
        .await;
    let stranger_user_id = harness
        .apply_variable_set_targeting(&stranger, &dataset_id, "stranger-vars", [("B", "2")], [])
        .await;

    harness.consume_deleted(&dataset_id).await.unwrap();

    assert!(
        harness
            .get_snapshot_by_id(&stranger_legacy_id)
            .await
            .is_none(),
        "another account's auto-managed set is deleted too"
    );
    let survivor = harness
        .get_snapshot_by_id(&stranger_user_id)
        .await
        .expect("another account's user-authored set survives");
    assert!(
        !has_target_label(&survivor),
        "but its dangling label is retracted"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_other_datasets_resources_are_untouched() {
    let harness = ConsumerHarness::new().await;
    let deleted_dataset_id = make_dataset_id(8);
    let kept_dataset_id = make_dataset_id(9);

    let kept_legacy_id = harness
        .apply_variable_set_targeting(
            &harness.owner,
            &kept_dataset_id,
            &legacy_vars_name(&kept_dataset_id),
            [("A", "1")],
            [],
        )
        .await;
    let kept_user_id = harness
        .apply_variable_set_targeting(
            &harness.owner,
            &kept_dataset_id,
            "kept-vars",
            [("B", "2")],
            [],
        )
        .await;

    harness.consume_deleted(&deleted_dataset_id).await.unwrap();

    assert!(
        harness.get_snapshot_by_id(&kept_legacy_id).await.is_some(),
        "another dataset's auto-managed set must survive"
    );
    let kept_user = harness.get_snapshot_by_id(&kept_user_id).await.unwrap();
    assert!(
        has_target_label(&kept_user),
        "another dataset's label must not be stripped"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_deleted_messages_are_no_ops() {
    let harness = ConsumerHarness::new().await;
    let dataset_id = make_dataset_id(10);

    let id = harness
        .apply_variable_set_targeting(
            &harness.owner,
            &dataset_id,
            &legacy_vars_name(&dataset_id),
            [("A", "1")],
            [],
        )
        .await;

    harness
        .consume(&DatasetLifecycleMessage::created(
            Utc::now(),
            dataset_id.clone(),
            harness.owner.did.clone(),
            odf::DatasetVisibility::Private,
            odf::DatasetName::new_unchecked("foo"),
        ))
        .await
        .unwrap();

    harness
        .consume(&DatasetLifecycleMessage::renamed(
            Utc::now(),
            dataset_id.clone(),
            odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar")),
        ))
        .await
        .unwrap();

    assert!(
        harness.get_snapshot_by_id(&id).await.is_some(),
        "only Deleted triggers cleanup"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_without_labelled_resources_is_a_no_op() {
    let harness = ConsumerHarness::new().await;

    harness
        .consume_deleted(&make_dataset_id(11))
        .await
        .expect("a dataset with no labelled resources must not error");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_consuming_the_same_deleted_message_twice_is_idempotent() {
    // Outbox redelivery is real, so the second pass must be a clean no-op.
    let harness = ConsumerHarness::new().await;
    let dataset_id = make_dataset_id(12);

    let legacy_id = harness
        .apply_variable_set_targeting(
            &harness.owner,
            &dataset_id,
            &legacy_vars_name(&dataset_id),
            [("A", "1")],
            [],
        )
        .await;
    let user_id = harness
        .apply_variable_set_targeting(&harness.owner, &dataset_id, "my-vars", [("B", "2")], [])
        .await;

    harness.consume_deleted(&dataset_id).await.unwrap();
    harness
        .consume_deleted(&dataset_id)
        .await
        .expect("redelivery must not fail");

    assert!(harness.get_snapshot_by_id(&legacy_id).await.is_none());
    let survivor = harness.get_snapshot_by_id(&user_id).await.unwrap();
    assert!(!has_target_label(&survivor));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_dataset_id(seed: u64) -> odf::DatasetID {
    odf::DatasetID::new_seeded_ed25519(&seed.to_le_bytes())
}

fn legacy_vars_name(dataset_id: &odf::DatasetID) -> String {
    DatasetEnvVarMutationAdapterImpl::legacy_variable_set_resource_name(dataset_id)
        .as_str()
        .to_string()
}

fn legacy_secrets_name(dataset_id: &odf::DatasetID) -> String {
    DatasetEnvVarMutationAdapterImpl::legacy_secret_set_resource_name(dataset_id)
        .as_str()
        .to_string()
}

fn has_target_label(snapshot: &ResourceSnapshot) -> bool {
    snapshot
        .headers
        .labels
        .entries
        .keys()
        .any(|key| key.as_str() == RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseConfigurationServiceHarness, base)]
struct ConsumerHarness {
    base: BaseConfigurationServiceHarness,
    owner: odf::AccountHandle,
}

impl ConsumerHarness {
    async fn new() -> Self {
        let base = BaseConfigurationServiceHarness::new();
        let owner = base.ensure_test_account("test-owner").await;
        Self { base, owner }
    }

    async fn consume_deleted(&self, dataset_id: &odf::DatasetID) -> Result<(), InternalError> {
        self.consume(&DatasetLifecycleMessage::deleted(
            Utc::now(),
            dataset_id.clone(),
        ))
        .await
    }

    async fn consume(&self, message: &DatasetLifecycleMessage) -> Result<(), InternalError> {
        let catalog = self.base.catalog();
        catalog
            .get_one::<dyn MessageConsumerT<DatasetLifecycleMessage>>()
            .unwrap()
            .consume_message(catalog, message)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
