// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::vec;

use dill::Catalog;
use kamu_auth_rebac::*;
use pretty_assertions::{assert_eq, assert_matches};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_get_properties_from_nonexistent_entity(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let nonexistent_entity = Entity::new_dataset("foo");

    assert_matches!(rebac_repo.properties_count().await, Ok(0));

    assert_matches!(
        rebac_repo.get_entity_properties(&nonexistent_entity).await,
        Ok(actual_properties)
            if actual_properties.is_empty()
    );
    assert_matches!(
        rebac_repo.get_entities_properties(&[nonexistent_entity]).await,
        Ok(actual_properties)
            if actual_properties.is_empty()
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(0));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_set_property(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let entity = Entity::new_dataset("bar");
    let anon_read_property = PropertyName::dataset_allows_anonymous_read(true);

    assert_matches!(rebac_repo.properties_count().await, Ok(0));
    assert_matches!(
        rebac_repo
            .set_entity_property(&entity, anon_read_property.0, &anon_read_property.1)
            .await,
        Ok(_)
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(1));

    assert_matches!(
        rebac_repo.get_entity_properties(&entity).await,
        Ok(actual_properties)
            if [anon_read_property.clone()] == *actual_properties
    );
    assert_matches!(
        rebac_repo.get_entities_properties(std::slice::from_ref(&entity)).await,
        Ok(actual_properties)
            if [(entity, anon_read_property.0, anon_read_property.1)] == *actual_properties
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_delete_property_from_nonexistent_entity(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let nonexistent_entity = Entity::new_dataset("foo");
    let property = DatasetPropertyName::AllowsAnonymousRead.into();

    assert_matches!(rebac_repo.properties_count().await, Ok(0));

    assert_matches!(
        rebac_repo.delete_entity_property(&nonexistent_entity, property).await,
        Err(DeleteEntityPropertyError::NotFound(e))
            if e.entity == nonexistent_entity
                && e.property_name == property
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(0));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_delete_nonexistent_property_from_entity(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let entity = Entity::new_dataset("bar");
    let anon_read_property = PropertyName::dataset_allows_anonymous_read(true);

    assert_matches!(rebac_repo.properties_count().await, Ok(0));
    assert_matches!(
        rebac_repo
            .set_entity_property(&entity, anon_read_property.0, &anon_read_property.1)
            .await,
        Ok(_)
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(1));

    let nonexistent_property = DatasetPropertyName::AllowsPublicRead.into();

    assert_matches!(
        rebac_repo.delete_entity_property(&entity, nonexistent_property).await,
        Err(DeleteEntityPropertyError::NotFound(e))
            if e.entity == entity
                && e.property_name == nonexistent_property
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(1));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_property_from_entity(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let entity = Entity::new_dataset("bar");
    let anon_read_property = PropertyName::dataset_allows_anonymous_read(true);

    assert_matches!(rebac_repo.properties_count().await, Ok(0));

    assert_matches!(
        rebac_repo
            .set_entity_property(&entity, anon_read_property.0, &anon_read_property.1)
            .await,
        Ok(_)
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(1));

    let public_read_property = PropertyName::dataset_allows_public_read(true);

    assert_matches!(
        rebac_repo
            .set_entity_property(&entity, public_read_property.0, &public_read_property.1)
            .await,
        Ok(_)
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(2));

    {
        match rebac_repo.get_entity_properties(&entity).await {
            Ok(mut actual_properties) => {
                let mut expected_properties =
                    vec![anon_read_property.clone(), public_read_property.clone()];

                expected_properties.sort();
                actual_properties.sort();

                assert_eq!(expected_properties, actual_properties);
            }
            Err(e) => {
                panic!("A successful result was expected, but an error was received: {e}");
            }
        }
        match rebac_repo
            .get_entities_properties(std::slice::from_ref(&entity))
            .await
        {
            Ok(mut actual_properties) => {
                let mut expected_properties = vec![
                    (
                        entity.clone(),
                        anon_read_property.0,
                        anon_read_property.1.clone(),
                    ),
                    (
                        entity.clone(),
                        public_read_property.0,
                        public_read_property.1.clone(),
                    ),
                ];

                expected_properties.sort();
                actual_properties.sort();

                assert_eq!(expected_properties, actual_properties);
            }
            Err(e) => {
                panic!("A successful result was expected, but an error was received: {e}");
            }
        }
    }

    assert_matches!(
        rebac_repo
            .delete_entity_property(&entity, anon_read_property.0)
            .await,
        Ok(_)
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(1));

    assert_matches!(
        rebac_repo.delete_entity_property(&entity, anon_read_property.0).await,
        Err(DeleteEntityPropertyError::NotFound(e))
            if e.entity == entity
                && e.property_name == anon_read_property.0
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(1));

    assert_matches!(
        rebac_repo.get_entity_properties(&entity).await,
        Ok(actual_properties)
            if [public_read_property.clone()] == *actual_properties
    );
    assert_matches!(
        rebac_repo.get_entities_properties(std::slice::from_ref(&entity)).await,
        Ok(actual_properties)
            if [(entity, public_read_property.0, public_read_property.1)] == *actual_properties
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_entity_properties(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let entity = Entity::new_dataset("bar");
    let anon_read_property = PropertyName::dataset_allows_anonymous_read(true);

    assert_matches!(rebac_repo.properties_count().await, Ok(0));

    assert_matches!(
        rebac_repo
            .set_entity_property(&entity, anon_read_property.0, &anon_read_property.1)
            .await,
        Ok(_)
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(1));

    let public_read_property = PropertyName::dataset_allows_public_read(true);

    assert_matches!(
        rebac_repo
            .set_entity_property(&entity, public_read_property.0, &public_read_property.1)
            .await,
        Ok(_)
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(2));

    match rebac_repo.get_entity_properties(&entity).await {
        Ok(mut actual_properties) => {
            let mut expected_properties =
                vec![anon_read_property.clone(), public_read_property.clone()];

            expected_properties.sort();
            actual_properties.sort();

            assert_eq!(expected_properties, actual_properties);
        }
        Err(e) => {
            panic!("A successful result was expected, but an error was received: {e}");
        }
    }
    match rebac_repo
        .get_entities_properties(std::slice::from_ref(&entity))
        .await
    {
        Ok(mut actual_properties) => {
            let mut expected_properties = vec![
                (entity.clone(), anon_read_property.0, anon_read_property.1),
                (
                    entity.clone(),
                    public_read_property.0,
                    public_read_property.1,
                ),
            ];

            expected_properties.sort();
            actual_properties.sort();

            assert_eq!(expected_properties, actual_properties);
        }
        Err(e) => {
            panic!("A successful result was expected, but an error was received: {e}");
        }
    }
    assert_matches!(rebac_repo.properties_count().await, Ok(2));

    assert_matches!(rebac_repo.delete_entity_properties(&entity).await, Ok(_));
    assert_matches!(rebac_repo.properties_count().await, Ok(0));

    assert_matches!(
        rebac_repo.get_entity_properties(&entity).await,
        Ok(actual_properties)
            if actual_properties.is_empty()
    );
    assert_matches!(
        rebac_repo.get_entities_properties(std::slice::from_ref(&entity)).await,
        Ok(actual_properties)
            if actual_properties.is_empty()
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(0));

    assert_matches!(
        rebac_repo.delete_entity_properties(&entity).await,
        Err(DeleteEntityPropertiesError::NotFound(e))
            if e.entity == entity
    );
    assert_matches!(rebac_repo.properties_count().await, Ok(0));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_upsert_entities_relations(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let account_1 = Entity::new_account("account1");
    let account_2 = Entity::new_account("account2");

    let dataset_1 = Entity::new_dataset("dataset1");
    let dataset_2 = Entity::new_dataset("dataset2");
    let dataset_3 = Entity::new_dataset("dataset3");

    let reader = Relation::account_is_a_dataset_reader();
    let editor = Relation::account_is_a_dataset_editor();

    // Initial
    let account_1_initial_state = CrossoverTestState {
        account_id: "account1",
        dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
        relation_map: HashMap::from([(
            Relation::account_is_a_dataset_reader(),
            ["dataset1", "dataset2"].into(),
        )]),
    };
    let account_2_initial_state = CrossoverTestState {
        account_id: "account2",
        dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
        relation_map: HashMap::from([
            (Relation::account_is_a_dataset_reader(), ["dataset1"].into()),
            (Relation::account_is_a_dataset_editor(), ["dataset3"].into()),
        ]),
    };

    {
        assert_matches!(
            rebac_repo
                .upsert_entities_relations(&[
                    // Account1
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        relationship: reader,
                        object_entity: Cow::Borrowed(&dataset_1),
                    },
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        relationship: reader,
                        object_entity: Cow::Borrowed(&dataset_2),
                    },
                    // Account2
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_2),
                        relationship: reader,
                        object_entity: Cow::Borrowed(&dataset_1),
                    },
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_2),
                        relationship: editor,
                        object_entity: Cow::Borrowed(&dataset_3),
                    },
                ])
                .await,
            Ok(_)
        );

        assert_get_relations("1.", &rebac_repo, &account_1_initial_state).await;
        assert_get_relations("2.", &rebac_repo, &account_2_initial_state).await;
    }

    // Same operations (idempotence)
    {
        assert_matches!(
            rebac_repo
                .upsert_entities_relations(&[
                    // Account1
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        relationship: reader,
                        object_entity: Cow::Borrowed(&dataset_1),
                    },
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        relationship: reader,
                        object_entity: Cow::Borrowed(&dataset_2),
                    },
                    // Account2
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_2),
                        relationship: reader,
                        object_entity: Cow::Borrowed(&dataset_1),
                    },
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_2),
                        relationship: editor,
                        object_entity: Cow::Borrowed(&dataset_3),
                    },
                ])
                .await,
            Ok(_)
        );

        assert_get_relations("3.", &rebac_repo, &account_1_initial_state).await;
        assert_get_relations("4.", &rebac_repo, &account_2_initial_state).await;
    }

    // Mix of adding and updating
    {
        assert_matches!(
            rebac_repo
                .upsert_entities_relations(&[
                    // Account1
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        relationship: editor, // updating (from reader)
                        object_entity: Cow::Borrowed(&dataset_1),
                    },
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        relationship: editor,
                        object_entity: Cow::Borrowed(&dataset_3), // adding
                    },
                    // Account2
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_2),
                        relationship: reader,
                        object_entity: Cow::Borrowed(&dataset_2), // adding
                    },
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_2),
                        relationship: reader, // updating (from editor)
                        object_entity: Cow::Borrowed(&dataset_3),
                    },
                ])
                .await,
            Ok(_)
        );

        assert_get_relations(
            "5.",
            &rebac_repo,
            &CrossoverTestState {
                account_id: "account1",
                dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
                relation_map: HashMap::from([
                    (Relation::account_is_a_dataset_reader(), ["dataset2"].into()),
                    (
                        Relation::account_is_a_dataset_editor(),
                        ["dataset1", "dataset3"].into(),
                    ),
                ]),
            },
        )
        .await;
        assert_get_relations(
            "6.",
            &rebac_repo,
            &CrossoverTestState {
                account_id: "account2",
                dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
                relation_map: HashMap::from([(
                    Relation::account_is_a_dataset_reader(),
                    ["dataset1", "dataset2", "dataset3"].into(),
                )]),
            },
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_entities_relations(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let account_1 = Entity::new_account("account1");
    let account_2 = Entity::new_account("account2");

    let dataset_1 = Entity::new_dataset("dataset1");
    let dataset_2 = Entity::new_dataset("dataset2");
    let dataset_3 = Entity::new_dataset("dataset3");

    let reader = Relation::account_is_a_dataset_reader();
    let editor = Relation::account_is_a_dataset_editor();

    // Initial
    {
        assert_matches!(
            rebac_repo
                .upsert_entities_relations(&[
                    // Account1
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        relationship: reader,
                        object_entity: Cow::Borrowed(&dataset_1),
                    },
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        relationship: reader,
                        object_entity: Cow::Borrowed(&dataset_2),
                    },
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        relationship: editor,
                        object_entity: Cow::Borrowed(&dataset_3),
                    },
                    // Account2
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_2),
                        relationship: reader,
                        object_entity: Cow::Borrowed(&dataset_1),
                    },
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_2),
                        relationship: editor,
                        object_entity: Cow::Borrowed(&dataset_3),
                    },
                ])
                .await,
            Ok(_)
        );

        assert_get_relations(
            "1.",
            &rebac_repo,
            &CrossoverTestState {
                account_id: "account1",
                dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
                relation_map: HashMap::from([
                    (
                        Relation::account_is_a_dataset_reader(),
                        ["dataset1", "dataset2"].into(),
                    ),
                    (Relation::account_is_a_dataset_editor(), ["dataset3"].into()),
                ]),
            },
        )
        .await;
        assert_get_relations(
            "2.",
            &rebac_repo,
            &CrossoverTestState {
                account_id: "account2",
                dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
                relation_map: HashMap::from([
                    (Relation::account_is_a_dataset_reader(), ["dataset1"].into()),
                    (Relation::account_is_a_dataset_editor(), ["dataset3"].into()),
                ]),
            },
        )
        .await;
    }

    // Partial deletion
    let account_1_after_deletion_state = CrossoverTestState {
        account_id: "account1",
        dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
        relation_map: HashMap::from([(
            Relation::account_is_a_dataset_editor(),
            ["dataset3"].into(),
        )]),
    };
    let account_2_after_deletion_state = CrossoverTestState {
        account_id: "account2",
        dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
        relation_map: HashMap::from([(
            Relation::account_is_a_dataset_editor(),
            ["dataset3"].into(),
        )]),
    };

    {
        assert_matches!(
            rebac_repo
                .delete_entities_relations(&[
                    // Account1
                    DeleteEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        object_entity: Cow::Borrowed(&dataset_1),
                    },
                    DeleteEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        object_entity: Cow::Borrowed(&dataset_2),
                    },
                    // Account2
                    DeleteEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_2),
                        object_entity: Cow::Borrowed(&dataset_1),
                    },
                ])
                .await,
            Ok(_)
        );

        assert_get_relations("3.", &rebac_repo, &account_1_after_deletion_state).await;
        assert_get_relations("4.", &rebac_repo, &account_2_after_deletion_state).await;
    }

    // Same operations (idempotence)
    {
        assert_matches!(
            rebac_repo
                .delete_entities_relations(&[
                    // Account1
                    DeleteEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        object_entity: Cow::Borrowed(&dataset_1),
                    },
                    DeleteEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_1),
                        object_entity: Cow::Borrowed(&dataset_2),
                    },
                    // Account2
                    DeleteEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account_2),
                        object_entity: Cow::Borrowed(&dataset_1),
                    },
                ])
                .await,
            Ok(_)
        );

        assert_get_relations("5.", &rebac_repo, &account_1_after_deletion_state).await;
        assert_get_relations("6.", &rebac_repo, &account_2_after_deletion_state).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_relations_crossover_test(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    // 1. Prepare

    let state = CrossoverTestState {
        account_id: "account",
        dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
        relation_map: HashMap::from([
            (
                Relation::account_is_a_dataset_reader(),
                ["dataset1", "dataset2"].into(),
            ),
            (Relation::account_is_a_dataset_editor(), ["dataset3"].into()),
        ]),
    };

    {
        let account = Entity::new_account(state.account_id);
        let operations = state
            .relation_map
            .iter()
            .flat_map(|(relation, dataset_ids)| {
                dataset_ids.iter().map(|dataset_id| {
                    let dataset = Entity::new_dataset(*dataset_id);
                    UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(&account),
                        relationship: *relation,
                        object_entity: Cow::Owned(dataset),
                    }
                })
            })
            .collect::<Vec<_>>();

        assert_matches!(
            rebac_repo.upsert_entities_relations(&operations).await,
            Ok(_)
        );
    }

    // 2. Validate inserted

    assert_get_relations("2.", &rebac_repo, &state).await;

    // 3. Deletions

    {
        let account = Entity::new_account("account");
        let dataset1 = Entity::new_dataset("dataset1");

        assert_matches!(
            rebac_repo
                .delete_entities_relations(&[DeleteEntitiesRelationOperation {
                    subject_entity: Cow::Owned(account),
                    object_entity: Cow::Owned(dataset1)
                }])
                .await,
            Ok(())
        );

        let state = CrossoverTestState {
            account_id: "account",
            dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
            relation_map: HashMap::from([
                (Relation::account_is_a_dataset_reader(), ["dataset2"].into()),
                (Relation::account_is_a_dataset_editor(), ["dataset3"].into()),
            ]),
        };

        assert_get_relations("3.1.", &rebac_repo, &state).await;
    }
    {
        let account = Entity::new_account("account");
        let dataset2 = Entity::new_dataset("dataset2");

        assert_matches!(
            rebac_repo
                .delete_entities_relations(&[DeleteEntitiesRelationOperation {
                    subject_entity: Cow::Owned(account),
                    object_entity: Cow::Owned(dataset2)
                }])
                .await,
            Ok(())
        );

        let state = CrossoverTestState {
            account_id: "account",
            dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
            relation_map: HashMap::from([
                (Relation::account_is_a_dataset_reader(), [].into()),
                (Relation::account_is_a_dataset_editor(), ["dataset3"].into()),
            ]),
        };

        assert_get_relations("3.2.", &rebac_repo, &state).await;
    }
    {
        let account = Entity::new_account("account");
        let dataset3 = Entity::new_dataset("dataset3");

        assert_matches!(
            rebac_repo
                .delete_entities_relations(&[DeleteEntitiesRelationOperation {
                    subject_entity: Cow::Owned(account),
                    object_entity: Cow::Owned(dataset3)
                }])
                .await,
            Ok(())
        );

        let state = CrossoverTestState {
            account_id: "account",
            dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
            relation_map: HashMap::from([
                (Relation::account_is_a_dataset_reader(), [].into()),
                (Relation::account_is_a_dataset_editor(), [].into()),
            ]),
        };

        assert_get_relations("3.3.", &rebac_repo, &state).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_subject_entities_object_entity_relations(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let account_1 = Entity::new_account("account1");
    let account_2 = Entity::new_account("account2");
    let account_3 = Entity::new_account("account3");

    let dataset_1 = Entity::new_dataset("dataset1");
    let dataset_2 = Entity::new_dataset("dataset2");

    let reader_relationship = Relation::account_is_a_dataset_reader();
    let editor_relationship = Relation::account_is_a_dataset_editor();

    for account in [&account_1, &account_2, &account_3] {
        for (dataset, relationship) in [
            (&dataset_1, reader_relationship),
            (&dataset_2, editor_relationship),
        ] {
            assert_matches!(
                rebac_repo
                    .upsert_entities_relations(&[UpsertEntitiesRelationOperation {
                        subject_entity: Cow::Borrowed(account),
                        relationship,
                        object_entity: Cow::Borrowed(dataset),
                    }])
                    .await,
                Ok(())
            );
        }
    }

    // NOTE: Without account_3
    let de_relationship_accounts = vec![account_1.clone(), account_2.clone()];

    assert_matches!(
        rebac_repo
            .delete_subject_entities_object_entity_relations(
                de_relationship_accounts.clone(),
                &dataset_1
            )
            .await,
        Ok(())
    );

    // Verification, after deletion
    let expected_states = [
        CrossoverTestState {
            account_id: "account1",
            dataset_ids_for_check: vec!["dataset1", "dataset2"],
            relation_map: HashMap::from([(
                Relation::account_is_a_dataset_editor(),
                ["dataset2"].into(),
            )]),
        },
        CrossoverTestState {
            account_id: "account2",
            dataset_ids_for_check: vec!["dataset1", "dataset2"],
            relation_map: HashMap::from([(
                Relation::account_is_a_dataset_editor(),
                ["dataset2"].into(),
            )]),
        },
        CrossoverTestState {
            account_id: "account3",
            dataset_ids_for_check: vec!["dataset1", "dataset2"],
            relation_map: HashMap::from([
                (Relation::account_is_a_dataset_reader(), ["dataset1"].into()),
                (Relation::account_is_a_dataset_editor(), ["dataset2"].into()),
            ]),
        },
    ];

    for expected_state in &expected_states {
        assert_get_relations("before", &rebac_repo, expected_state).await;
    }

    assert_matches!(
        rebac_repo
            .delete_subject_entities_object_entity_relations(
                de_relationship_accounts.clone(),
                &dataset_1
            )
            .await,
        Err(DeleteSubjectEntitiesObjectEntityRelationsError::NotFound(e))
            if e.subject_entities == de_relationship_accounts
                && e.object_entity == dataset_1
    );

    for expected_state in &expected_states {
        assert_get_relations("after", &rebac_repo, expected_state).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_object_entity_relations_matrix(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let account_1 = Entity::new_account("account1");
    let account_2 = Entity::new_account("account2");
    let account_3 = Entity::new_account("account3");
    let account_4 = Entity::new_account("account4");

    let dataset_1 = Entity::new_dataset("dataset1");
    let dataset_2 = Entity::new_dataset("dataset2");
    let dataset_3 = Entity::new_dataset("dataset3");

    let reader = Relation::account_is_a_dataset_reader();
    let editor = Relation::account_is_a_dataset_editor();
    let maintainer = Relation::account_is_a_dataset_maintainer();

    for (dataset, account, relationship) in [
        // dataset_1
        (&dataset_1, &account_1, reader),
        (&dataset_1, &account_2, editor),
        (&dataset_1, &account_3, reader),
        (&dataset_1, &account_4, maintainer),
        // dataset2
        (&dataset_2, &account_2, maintainer),
        (&dataset_2, &account_3, maintainer),
        // dataset3
        //   no relations
    ] {
        assert_matches!(
            rebac_repo
                .upsert_entities_relations(&[UpsertEntitiesRelationOperation {
                    subject_entity: Cow::Borrowed(account),
                    relationship,
                    object_entity: Cow::Borrowed(dataset),
                }])
                .await,
            Ok(())
        );
    }

    // get_object_entity_relations()

    {
        macro_rules! get_object_entity_relations_sorted {
            ($repo: expr, $object: expr) => {{
                let r = $repo.get_object_entity_relations($object).await;
                assert_matches!(r, Ok(_));
                let mut r = r.unwrap();
                r.sort();
                r
            }};
        }

        use EntityWithRelation as E;

        // dataset_1
        assert_eq!(
            [
                E::new_account("account1", reader),
                E::new_account("account2", editor),
                E::new_account("account3", reader),
                E::new_account("account4", maintainer),
            ],
            *get_object_entity_relations_sorted!(&rebac_repo, &dataset_1)
        );
        // dataset_2
        assert_eq!(
            [
                E::new_account("account2", maintainer),
                E::new_account("account3", maintainer),
            ],
            *get_object_entity_relations_sorted!(&rebac_repo, &dataset_2)
        );
        // dataset_3
        assert_eq!(
            *Vec::<E>::new(),
            *get_object_entity_relations_sorted!(&rebac_repo, &dataset_3)
        );
    }

    // get_object_entities_relations()

    {
        macro_rules! get_object_entities_relations_sorted {
            ($repo: expr, $objects: expr) => {{
                let r = $repo.get_object_entities_relations($objects).await;
                assert_matches!(r, Ok(_));
                let mut r = r.unwrap();
                r.sort();
                r
            }};
        }

        use EntitiesWithRelation as E;

        // [dataset1]
        assert_eq!(
            [
                E::new_account_dataset_relation("account1", reader, "dataset1"),
                E::new_account_dataset_relation("account2", editor, "dataset1"),
                E::new_account_dataset_relation("account3", reader, "dataset1"),
                E::new_account_dataset_relation("account4", maintainer, "dataset1"),
            ],
            *get_object_entities_relations_sorted!(&rebac_repo, std::slice::from_ref(&dataset_1)),
        );
        // [dataset2]
        assert_eq!(
            [
                E::new_account_dataset_relation("account2", maintainer, "dataset2"),
                E::new_account_dataset_relation("account3", maintainer, "dataset2"),
            ],
            *get_object_entities_relations_sorted!(&rebac_repo, std::slice::from_ref(&dataset_2)),
        );
        // [dataset3]
        assert_eq!(
            *Vec::<E>::new(),
            *get_object_entities_relations_sorted!(&rebac_repo, std::slice::from_ref(&dataset_3)),
        );
        // [dataset1, dataset3]
        assert_eq!(
            [
                E::new_account_dataset_relation("account1", reader, "dataset1"),
                E::new_account_dataset_relation("account2", editor, "dataset1"),
                E::new_account_dataset_relation("account3", reader, "dataset1"),
                E::new_account_dataset_relation("account4", maintainer, "dataset1"),
            ],
            *get_object_entities_relations_sorted!(&rebac_repo, &[dataset_1.clone(), dataset_3]),
        );
        // [dataset1, dataset2]
        assert_eq!(
            [
                E::new_account_dataset_relation("account1", reader, "dataset1"),
                E::new_account_dataset_relation("account2", editor, "dataset1"),
                E::new_account_dataset_relation("account2", maintainer, "dataset2"),
                E::new_account_dataset_relation("account3", reader, "dataset1"),
                E::new_account_dataset_relation("account3", maintainer, "dataset2"),
                E::new_account_dataset_relation("account4", maintainer, "dataset1"),
            ],
            *get_object_entities_relations_sorted!(&rebac_repo, &[dataset_1, dataset_2]),
        );
    }

    // get_subject_entities_relations

    {
        macro_rules! get_subject_entities_relations_sorted {
            ($repo: expr, $objects: expr) => {{
                let r = $repo.get_subject_entities_relations($objects).await;
                assert_matches!(r, Ok(_));
                let mut r = r.unwrap();
                r.sort();
                r
            }};
        }

        use EntitiesWithRelation as E;

        // [account1]
        assert_eq!(
            [E::new_account_dataset_relation(
                "account1", reader, "dataset1"
            )],
            *get_subject_entities_relations_sorted!(&rebac_repo, std::slice::from_ref(&account_1)),
        );
        // [account2]
        assert_eq!(
            [
                E::new_account_dataset_relation("account2", editor, "dataset1"),
                E::new_account_dataset_relation("account2", maintainer, "dataset2"),
            ],
            *get_subject_entities_relations_sorted!(&rebac_repo, std::slice::from_ref(&account_2)),
        );
        // [account3]
        assert_eq!(
            [
                E::new_account_dataset_relation("account3", reader, "dataset1"),
                E::new_account_dataset_relation("account3", maintainer, "dataset2"),
            ],
            *get_subject_entities_relations_sorted!(&rebac_repo, std::slice::from_ref(&account_3)),
        );
        // [account1, account2]
        assert_eq!(
            [
                E::new_account_dataset_relation("account1", reader, "dataset1"),
                E::new_account_dataset_relation("account2", editor, "dataset1"),
                E::new_account_dataset_relation("account2", maintainer, "dataset2"),
            ],
            *get_subject_entities_relations_sorted!(&rebac_repo, &[account_1.clone(), account_2]),
        );
        // [account1, account3]
        assert_eq!(
            [
                E::new_account_dataset_relation("account1", reader, "dataset1"),
                E::new_account_dataset_relation("account3", reader, "dataset1"),
                E::new_account_dataset_relation("account3", maintainer, "dataset2"),
            ],
            *get_subject_entities_relations_sorted!(&rebac_repo, &[account_1, account_3]),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn assert_get_relations(
    tag: &'static str,
    rebac_repo: &Arc<dyn RebacRepository>,
    state: &CrossoverTestState,
) {
    let account = Entity::new_account(state.account_id);
    let mut object_entities = state.get_object_entities_with_relation();

    object_entities.sort();

    {
        match rebac_repo.get_subject_entity_relations(&account).await {
            Ok(mut actual_res) => {
                actual_res.sort();

                assert_eq!(
                    object_entities, actual_res,
                    "[{}] AccountID: {}",
                    tag, state.account_id
                );
            }
            unexpected_res => {
                panic!("Unexpected result: {unexpected_res:?}");
            }
        }
    }

    // Check RebacRepository::get_subject_entity_relations_by_object_type()
    {
        {
            match rebac_repo
                .get_subject_entity_relations_by_object_type(&account, EntityType::Dataset)
                .await
            {
                Ok(mut actual_res) => {
                    actual_res.sort();

                    assert_eq!(
                        object_entities, actual_res,
                        "[{}] AccountID: {}",
                        tag, state.account_id
                    );
                }
                unexpected_res => {
                    panic!("Unexpected result: {unexpected_res:?}");
                }
            }
        }
        {
            // NOTE: We never have any EntityType::Account objects
            assert_matches!(
                rebac_repo
                    .get_subject_entity_relations_by_object_type(&account, EntityType::Account)
                    .await,
                Ok(actual_object_relations)
                    if actual_object_relations.is_empty()
            );
        }
    }

    // Check RebacRepository::try_get_relation_between_entities()
    {
        let relation_map = state.get_object_entity_relation_map();

        for dataset_id in &state.dataset_ids_for_check {
            let dataset = Entity::new_dataset(*dataset_id);

            match rebac_repo
                .try_get_relation_between_entities(&account, &dataset)
                .await
            {
                Ok(actual_relation) => {
                    let expected_relation = relation_map.get(dataset_id).unwrap();

                    assert_eq!(
                        expected_relation, &actual_relation,
                        "[{}] AccountID: {}",
                        tag, state.account_id
                    );
                }
                unexpected_res => {
                    panic!("Unexpected result: {unexpected_res:?}");
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type AccountId = &'static str;
type DatasetId = &'static str;

struct CrossoverTestState {
    pub account_id: AccountId,
    pub dataset_ids_for_check: Vec<DatasetId>,
    pub relation_map: HashMap<Relation, HashSet<DatasetId>>,
}

impl CrossoverTestState {
    pub fn get_object_entities_with_relation(&self) -> Vec<EntityWithRelation<'_>> {
        self.relation_map
            .iter()
            .fold(Vec::new(), |mut acc, (relation, dataset_ids)| {
                let object_entity_iter = dataset_ids
                    .iter()
                    .map(|dataset_id| EntityWithRelation::new_dataset(*dataset_id, *relation));

                acc.extend(object_entity_iter);

                acc
            })
    }

    pub fn get_object_entity_relation_map(&self) -> HashMap<DatasetId, Option<Relation>> {
        self.dataset_ids_for_check
            .iter()
            .fold(HashMap::new(), |mut acc, dataset_id| {
                let mut relations = [
                    Relation::account_is_a_dataset_reader(),
                    Relation::account_is_a_dataset_editor(),
                ]
                .into_iter()
                .fold(Vec::new(), |mut acc, relation| {
                    if let Some(dataset_ids) = self.relation_map.get(&relation)
                        && dataset_ids.contains(dataset_id)
                    {
                        acc.push(relation);
                    }

                    acc
                });

                let maybe_relation = match relations.len() {
                    0 => None,
                    1 => Some(relations.remove(0)),
                    _ => {
                        panic!(
                            "[CrossoverTestState::relation_map]: More than one relation found for \
                             dataset: {dataset_id}",
                        );
                    }
                };

                acc.insert(*dataset_id, maybe_relation);

                acc
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
