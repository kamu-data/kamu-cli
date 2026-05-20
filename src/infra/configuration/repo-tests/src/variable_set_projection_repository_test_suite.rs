// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{SubsecRound, Utc};
use dill::Catalog;
use kamu_configuration::{
    ReplaceProjectionEntriesError,
    VariableSetEntry,
    VariableSetProjectionRepository,
    VariableSetResource,
    VariableSetSpec,
    VariableSpec,
    VariableValueSpec,
};
use kamu_resources::{ResourceMetadata, ResourceRepository, ResourceSnapshot, ResourceUID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_entry(key: &str, value: &str) -> VariableSetEntry {
    let now = Utc::now();

    VariableSetEntry {
        entry_id: uuid::Uuid::new_v4(),
        account_id: odf::AccountID::new_seeded_ed25519(b"test-account"),
        key: key.to_string(),
        value: value.to_string(),
        created_at: now,
        updated_at: now,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn make_variable_set_resource(catalog: &Catalog) -> ResourceUID {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let variable_set_id = ResourceUID::new(uuid::Uuid::new_v4());

    repo.create_resource(&ResourceSnapshot {
        uid: variable_set_id,
        kind: VariableSetResource::RESOURCE_TYPE.to_string(),
        api_version: VariableSetResource::API_VERSION.to_string(),
        metadata: ResourceMetadata::new_minimal(
            Utc::now(),
            odf::AccountID::new_seeded_ed25519(b"test-account"),
            variable_set_id.to_string(),
        ),
        spec: serde_json::to_value(VariableSetSpec {
            variables: [(
                "PLACEHOLDER".to_string(),
                VariableSpec::Value(VariableValueSpec {
                    value: "placeholder".to_string(),
                }),
            )]
            .into(),
        })
        .unwrap(),
        status: None,
        last_reconciled_at: None,
        last_event_id: None,
    })
    .await
    .unwrap();

    variable_set_id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_entries_empty_initially(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn VariableSetProjectionRepository>()
        .unwrap();
    let resource_uid = make_variable_set_resource(catalog).await;

    let entries = repo.get_entries(&resource_uid, 0).await.unwrap();
    assert!(entries.is_empty());

    let entry = repo.find_entry(&resource_uid, 0, "key").await.unwrap();
    assert!(entry.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_and_get_entries(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn VariableSetProjectionRepository>()
        .unwrap();
    let resource_uid = make_variable_set_resource(catalog).await;

    let entries = vec![
        make_entry("alpha", "1"),
        make_entry("beta", "2"),
        make_entry("gamma", "3"),
    ];

    repo.replace_entries(&resource_uid, 1, &entries)
        .await
        .unwrap();

    let stored = repo.get_entries(&resource_uid, 1).await.unwrap();
    assert_eq!(3, stored.len());

    let keys: Vec<&str> = stored.iter().map(|e| e.key.as_str()).collect();
    assert!(keys.contains(&"alpha"));
    assert!(keys.contains(&"beta"));
    assert!(keys.contains(&"gamma"));

    assert_eq!(stored.iter().find(|e| e.key == "alpha").unwrap().value, "1");
    assert_eq!(stored.iter().find(|e| e.key == "beta").unwrap().value, "2");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_entry_by_key(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn VariableSetProjectionRepository>()
        .unwrap();
    let resource_uid = make_variable_set_resource(catalog).await;

    repo.replace_entries(
        &resource_uid,
        1,
        &[
            make_entry("db_host", "localhost"),
            make_entry("db_port", "5432"),
        ],
    )
    .await
    .unwrap();

    let found = repo.find_entry(&resource_uid, 1, "db_host").await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().value, "localhost");

    let missing = repo
        .find_entry(&resource_uid, 1, "nonexistent")
        .await
        .unwrap();
    assert!(missing.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_entries_isolated_by_resource(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn VariableSetProjectionRepository>()
        .unwrap();
    let resource_a = make_variable_set_resource(catalog).await;
    let resource_b = make_variable_set_resource(catalog).await;

    repo.replace_entries(&resource_a, 1, &[make_entry("key", "from-a")])
        .await
        .unwrap();
    repo.replace_entries(&resource_b, 1, &[make_entry("key", "from-b")])
        .await
        .unwrap();

    let entries_a = repo.get_entries(&resource_a, 1).await.unwrap();
    assert_eq!(1, entries_a.len());
    assert_eq!(entries_a[0].value, "from-a");

    let entries_b = repo.get_entries(&resource_b, 1).await.unwrap();
    assert_eq!(1, entries_b.len());
    assert_eq!(entries_b[0].value, "from-b");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_entries_isolated_by_generation(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn VariableSetProjectionRepository>()
        .unwrap();
    let resource_uid = make_variable_set_resource(catalog).await;

    repo.replace_entries(&resource_uid, 1, &[make_entry("key", "gen1")])
        .await
        .unwrap();
    repo.replace_entries(&resource_uid, 2, &[make_entry("key", "gen2")])
        .await
        .unwrap();

    let gen1 = repo.get_entries(&resource_uid, 1).await.unwrap();
    assert_eq!(1, gen1.len());
    assert_eq!(gen1[0].value, "gen1");

    let gen2 = repo.get_entries(&resource_uid, 2).await.unwrap();
    assert_eq!(1, gen2.len());
    assert_eq!(gen2[0].value, "gen2");

    // Generation 3 has no entries
    let gen3 = repo.get_entries(&resource_uid, 3).await.unwrap();
    assert!(gen3.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_latest_entries_before_generation(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn VariableSetProjectionRepository>()
        .unwrap();
    let resource_uid = make_variable_set_resource(catalog).await;

    repo.replace_entries(&resource_uid, 1, &[make_entry("key", "gen1")])
        .await
        .unwrap();
    repo.replace_entries(&resource_uid, 3, &[make_entry("key", "gen3")])
        .await
        .unwrap();

    let latest_before_3 = repo
        .get_latest_entries_before_generation(&resource_uid, 3)
        .await
        .unwrap();
    assert_eq!(1, latest_before_3.len());
    assert_eq!(latest_before_3[0].value, "gen1");

    let latest_before_4 = repo
        .get_latest_entries_before_generation(&resource_uid, 4)
        .await
        .unwrap();
    assert_eq!(1, latest_before_4.len());
    assert_eq!(latest_before_4[0].value, "gen3");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_entries_concurrent_modification(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn VariableSetProjectionRepository>()
        .unwrap();
    let resource_uid = make_variable_set_resource(catalog).await;

    repo.replace_entries(&resource_uid, 1, &[make_entry("key", "value")])
        .await
        .unwrap();

    // Replacing the same (resource_uid, generation) again must fail
    let result = repo
        .replace_entries(&resource_uid, 1, &[make_entry("key", "other")])
        .await;
    assert!(
        matches!(
            result,
            Err(ReplaceProjectionEntriesError::ConcurrentModification(_))
        ),
        "expected ConcurrentModification, got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_cleanup_entries_before_generation(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn VariableSetProjectionRepository>()
        .unwrap();
    let resource_uid = make_variable_set_resource(catalog).await;

    repo.replace_entries(&resource_uid, 1, &[make_entry("k", "gen1")])
        .await
        .unwrap();
    repo.replace_entries(&resource_uid, 2, &[make_entry("k", "gen2")])
        .await
        .unwrap();
    repo.replace_entries(&resource_uid, 3, &[make_entry("k", "gen3")])
        .await
        .unwrap();

    // Cleanup entries with generation < 3 (i.e. gens 1 and 2 are removed)
    repo.cleanup_entries_before_generation(&resource_uid, 3)
        .await
        .unwrap();

    assert!(repo.get_entries(&resource_uid, 1).await.unwrap().is_empty());
    assert!(repo.get_entries(&resource_uid, 2).await.unwrap().is_empty());

    let surviving = repo.get_entries(&resource_uid, 3).await.unwrap();
    assert_eq!(1, surviving.len());
    assert_eq!(surviving[0].value, "gen3");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_cleanup_does_not_affect_other_resources(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn VariableSetProjectionRepository>()
        .unwrap();
    let resource_a = make_variable_set_resource(catalog).await;
    let resource_b = make_variable_set_resource(catalog).await;

    repo.replace_entries(&resource_a, 1, &[make_entry("k", "a-gen1")])
        .await
        .unwrap();
    repo.replace_entries(&resource_b, 1, &[make_entry("k", "b-gen1")])
        .await
        .unwrap();

    // Cleanup resource_a's old generations – must not touch resource_b
    repo.cleanup_entries_before_generation(&resource_a, 2)
        .await
        .unwrap();

    assert!(repo.get_entries(&resource_a, 1).await.unwrap().is_empty());

    let b_entries = repo.get_entries(&resource_b, 1).await.unwrap();
    assert_eq!(1, b_entries.len());
    assert_eq!(b_entries[0].value, "b-gen1");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_preserves_stable_identity_and_creation_time(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn VariableSetProjectionRepository>()
        .unwrap();
    let resource_uid = make_variable_set_resource(catalog).await;

    let original = VariableSetEntry {
        entry_id: uuid::Uuid::new_v4(),
        account_id: odf::AccountID::new_seeded_ed25519(b"test-account"),
        key: "shared".to_string(),
        value: "v1".to_string(),
        created_at: Utc::now().round_subsecs(6),
        updated_at: Utc::now().round_subsecs(6),
    };

    repo.replace_entries(&resource_uid, 1, std::slice::from_ref(&original))
        .await
        .unwrap();

    let replacement = VariableSetEntry {
        value: "v2".to_string(),
        updated_at: Utc::now().round_subsecs(6),
        ..original.clone()
    };

    repo.replace_entries(&resource_uid, 2, std::slice::from_ref(&replacement))
        .await
        .unwrap();

    let stored = repo
        .find_entry(&resource_uid, 2, "shared")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.entry_id, original.entry_id);
    assert_eq!(stored.created_at, original.created_at);
    assert_eq!(stored.updated_at, replacement.updated_at);
    assert_eq!(stored.value, "v2");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_latest_entries(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn VariableSetProjectionRepository>()
        .unwrap();
    let resource_uid = make_variable_set_resource(catalog).await;

    repo.replace_entries(&resource_uid, 1, &[make_entry("key", "gen1")])
        .await
        .unwrap();
    repo.replace_entries(&resource_uid, 3, &[make_entry("key", "gen3")])
        .await
        .unwrap();

    // Empty resource returns empty vec
    let empty_resource = make_variable_set_resource(catalog).await;
    let empty_latest = repo.get_latest_entries(&empty_resource).await.unwrap();
    assert!(empty_latest.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_all_entries(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn VariableSetProjectionRepository>()
        .unwrap();
    let resource_a = make_variable_set_resource(catalog).await;
    let resource_b = make_variable_set_resource(catalog).await;
    let resource_c = make_variable_set_resource(catalog).await;

    repo.replace_entries(&resource_a, 1, &[make_entry("k1", "a-gen1")])
        .await
        .unwrap();
    repo.replace_entries(&resource_a, 2, &[make_entry("k1", "a-gen2")])
        .await
        .unwrap();
    repo.replace_entries(&resource_a, 3, &[make_entry("k1", "a-gen3")])
        .await
        .unwrap();
    repo.replace_entries(&resource_b, 1, &[make_entry("k1", "b-gen1")])
        .await
        .unwrap();
    repo.replace_entries(&resource_c, 1, &[make_entry("k1", "c-gen1")])
        .await
        .unwrap();

    // Delete both resource_a and resource_b in a single call
    repo.delete_all_entries(&[resource_a, resource_b])
        .await
        .unwrap();

    assert!(repo.get_entries(&resource_a, 1).await.unwrap().is_empty());
    assert!(repo.get_entries(&resource_a, 2).await.unwrap().is_empty());
    assert!(repo.get_entries(&resource_a, 3).await.unwrap().is_empty());
    assert!(repo.get_entries(&resource_b, 1).await.unwrap().is_empty());

    let c_entries = repo.get_entries(&resource_c, 1).await.unwrap();
    assert_eq!(1, c_entries.len());
    assert_eq!(c_entries[0].value, "c-gen1");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
