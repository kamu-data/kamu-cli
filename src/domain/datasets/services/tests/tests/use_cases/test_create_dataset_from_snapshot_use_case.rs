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

use chrono::{TimeZone, Utc};
use kamu_accounts::{DidEntity, DidSecretEncryptionConfig, DidSecretKeyRepository};
use kamu_core::MockDidGenerator;
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, DatasetReferenceRepository};
use kamu_datasets_services::CreateDatasetFromSnapshotUseCaseImpl;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::assert_eq;
use time_source::SystemTimeSourceStub;

use super::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_root_dataset_from_snapshot() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let predefined_foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mock_did_generator =
        MockDidGenerator::predefined_dataset_ids(vec![predefined_foo_id.clone()]);
    let harness = CreateFromSnapshotUseCaseHarness::new(Some(mock_did_generator)).await;

    let snapshot = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let foo_created = harness
        .use_case
        .execute(snapshot, Default::default())
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_eq!(
        harness
            .get_dataset_reference(&foo_created.dataset_handle.id, &odf::BlockRef::Head)
            .await,
        foo_created.head,
    );

    // Note: the stability of these identifiers is ensured via
    //  predefined dataset ID and stubbed system time
    assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Lifecycle Messages: 1
              Created {
                Dataset ID: <foo_id>
                Dataset Name: foo
                Owner: did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f
                Visibility: private
              }
            Dataset Reference Messages: 2
              Ref Updating {
                Dataset ID: <foo_id>
                Ref: head
                Prev Head: None
                New Head: Multihash<Sha3_256>(<foo_head>)
              }
              Ref Updated {
                Dataset ID: <foo_id>
                Ref: head
                Prev Head: None
                New Head: Multihash<Sha3_256>(<foo_head>)
              }
            "#
        )
        .replace("<foo_id>", predefined_foo_id.to_string().as_str())
        .replace("<foo_head>", foo_created.head.to_string().as_str()),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_derived_dataset_from_snapshot() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let predefined_foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let predefined_bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");

    let mock_did_generator = MockDidGenerator::predefined_dataset_ids(vec![
        predefined_foo_id.clone(),
        predefined_bar_id.clone(),
    ]);
    let harness = CreateFromSnapshotUseCaseHarness::new(Some(mock_did_generator)).await;

    let snapshot_root = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let snapshot_derived = MetadataFactory::dataset_snapshot()
        .name(alias_bar.clone())
        .kind(odf::DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs(vec![alias_foo.as_local_ref()])
                .build(),
        )
        .build();

    let options = Default::default();

    let foo_created = harness
        .use_case
        .execute(snapshot_root, options)
        .await
        .unwrap();
    let bar_created = harness
        .use_case
        .execute(snapshot_derived, options)
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));

    assert_eq!(
        harness
            .get_dataset_reference(&foo_created.dataset_handle.id, &odf::BlockRef::Head)
            .await,
        foo_created.head,
    );
    assert_eq!(
        harness
            .get_dataset_reference(&bar_created.dataset_handle.id, &odf::BlockRef::Head)
            .await,
        bar_created.head,
    );

    // Note: the stability of these identifiers is ensured via
    //  predefined dataset ID and stubbed system time
    assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Lifecycle Messages: 2
              Created {
                Dataset ID: <foo_id>
                Dataset Name: foo
                Owner: did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f
                Visibility: private
              }
              Created {
                Dataset ID: <bar_id>
                Dataset Name: bar
                Owner: did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f
                Visibility: private
              }
            Dataset Reference Messages: 4
              Ref Updating {
                Dataset ID: <foo_id>
                Ref: head
                Prev Head: None
                New Head: Multihash<Sha3_256>(<foo_head>)
              }
              Ref Updated {
                Dataset ID: <foo_id>
                Ref: head
                Prev Head: None
                New Head: Multihash<Sha3_256>(<foo_head>)
              }
              Ref Updating {
                Dataset ID: <bar_id>
                Ref: head
                Prev Head: None
                New Head: Multihash<Sha3_256>(<bar_head>)
              }
              Ref Updated {
                Dataset ID: <bar_id>
                Ref: head
                Prev Head: None
                New Head: Multihash<Sha3_256>(<bar_head>)
              }
            Dataset Dependency Messages: 1
              Deps Updated {
                Dataset ID: <bar_id>
                Added: [<foo_id>]
                Removed: []
              }
            "#
        )
        .replace("<foo_id>", predefined_foo_id.to_string().as_str())
        .replace("<bar_id>", predefined_bar_id.to_string().as_str())
        .replace("<foo_head>", foo_created.head.to_string().as_str())
        .replace("<bar_head>", bar_created.head.to_string().as_str()),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_from_snapshot_creates_did_secret_key() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = CreateFromSnapshotUseCaseHarness::new(None).await;

    let snapshot = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let foo_created = harness
        .use_case
        .execute(snapshot, Default::default())
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_eq!(
        harness
            .get_dataset_reference(&foo_created.dataset_handle.id, &odf::BlockRef::Head)
            .await,
        foo_created.head,
    );

    let did_secret_key = harness
        .did_secret_key_repo
        .get_did_secret_key(&DidEntity::new_dataset(
            foo_created.dataset_handle.id.to_string(),
        ))
        .await
        .unwrap();

    let did_private_key = did_secret_key
        .get_decrypted_private_key(&DidSecretEncryptionConfig::sample().encryption_key.unwrap())
        .unwrap();

    let public_key = did_private_key.verifying_key().to_bytes();
    let did_odf = odf::metadata::DidOdf::from(
        odf::metadata::DidKey::new(odf::metadata::Multicodec::Ed25519Pub, &public_key).unwrap(),
    );

    // Compare original account_id from db and id generated from a stored private
    // key
    assert_eq!(foo_created.dataset_handle.id.as_did(), &did_odf);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct CreateFromSnapshotUseCaseHarness {
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    dataset_reference_repo: Arc<dyn DatasetReferenceRepository>,
    did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,
}

impl CreateFromSnapshotUseCaseHarness {
    async fn new(maybe_mock_did_generator: Option<MockDidGenerator>) -> Self {
        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                maybe_system_time_source_stub: Some(SystemTimeSourceStub::new_set(
                    Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                )),
                maybe_mock_did_generator,
                ..DatasetBaseUseCaseHarnessOpts::default()
            })
            .await;

        let mut b = dill::CatalogBuilder::new_chained(dataset_base_use_case_harness.catalog());
        b.add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<CreateDatasetUseCaseHelper>();

        let catalog = b.build();

        Self {
            dataset_base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
            dataset_reference_repo: catalog.get_one().unwrap(),
            did_secret_key_repo: catalog.get_one().unwrap(),
        }
    }

    async fn get_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> odf::Multihash {
        self.dataset_reference_repo
            .get_dataset_reference(dataset_id, block_ref)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
