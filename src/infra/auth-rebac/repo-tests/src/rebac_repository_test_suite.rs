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
use std::vec;

use dill::Catalog;
use kamu_auth_rebac::{
    DatasetPropertyName,
    DeleteEntitiesRelationError,
    DeleteEntityPropertiesError,
    DeleteEntityPropertyError,
    Entity,
    EntityType,
    EntityWithRelation,
    InsertEntitiesRelationError,
    PropertyName,
    RebacRepository,
    Relation,
};

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
        rebac_repo.get_entities_properties(&[entity.clone()]).await,
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

                pretty_assertions::assert_eq!(expected_properties, actual_properties);
            }
            Err(e) => {
                panic!("A successful result was expected, but an error was received: {e}");
            }
        }
        match rebac_repo.get_entities_properties(&[entity.clone()]).await {
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

                pretty_assertions::assert_eq!(expected_properties, actual_properties);
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
        rebac_repo.get_entities_properties(&[entity.clone()]).await,
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

            pretty_assertions::assert_eq!(expected_properties, actual_properties);
        }
        Err(e) => {
            panic!("A successful result was expected, but an error was received: {e}");
        }
    }
    match rebac_repo.get_entities_properties(&[entity.clone()]).await {
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

            pretty_assertions::assert_eq!(expected_properties, actual_properties);
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
        rebac_repo.get_entities_properties(&[entity.clone()]).await,
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

pub async fn test_try_insert_duplicate_entities_relation(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let account = Entity::new_account("kamu");
    let dataset = Entity::new_account("dataset");
    let relationship = Relation::account_is_a_dataset_reader();

    assert_matches!(
        rebac_repo
            .insert_entities_relation(&account, relationship, &dataset)
            .await,
        Ok(())
    );
    assert_matches!(
        rebac_repo
            .insert_entities_relation(&account, relationship, &dataset)
            .await,
        Err(InsertEntitiesRelationError::Duplicate(e))
            if e.subject_entity == account
                && e.relationship == relationship
                && e.object_entity == dataset
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_entities_relation(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let account = Entity::new_account("kamu");
    let dataset = Entity::new_dataset("dataset");
    let relationship = Relation::account_is_a_dataset_reader();

    assert_matches!(
        rebac_repo
            .insert_entities_relation(&account, relationship, &dataset)
            .await,
        Ok(())
    );

    assert_matches!(
        rebac_repo
            .delete_entities_relation(&account, relationship, &dataset)
            .await,
        Ok(())
    );
    assert_matches!(
        rebac_repo
            .delete_entities_relation(&account, relationship, &dataset)
            .await,
        Err(DeleteEntitiesRelationError::NotFound(e))
            if e.subject_entity == account
                && e.relationship == relationship
                && e.object_entity == dataset
    );
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
            (
                Relation::account_is_a_dataset_editor(),
                ["dataset1", "dataset3"].into(),
            ),
        ]),
    };

    {
        let account = Entity::new_account(state.account_id);

        for (relation, dataset_ids) in &state.relation_map {
            for dataset_id in dataset_ids {
                let dataset = Entity::new_dataset(*dataset_id);

                assert_matches!(
                    rebac_repo
                        .insert_entities_relation(&account, *relation, &dataset)
                        .await,
                    Ok(_)
                );
            }
        }
    }

    // 2. Validate inserted

    assert_get_relations(&rebac_repo, &state).await;

    // 3. Deletions

    {
        let account = Entity::new_account("account");
        let dataset1 = Entity::new_dataset("dataset1");

        assert_matches!(
            rebac_repo
                .delete_entities_relation(
                    &account,
                    Relation::account_is_a_dataset_reader(),
                    &dataset1
                )
                .await,
            Ok(())
        );

        let state = CrossoverTestState {
            account_id: "account",
            dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
            relation_map: HashMap::from([
                (Relation::account_is_a_dataset_reader(), ["dataset2"].into()),
                (
                    Relation::account_is_a_dataset_editor(),
                    ["dataset1", "dataset3"].into(),
                ),
            ]),
        };

        assert_get_relations(&rebac_repo, &state).await;
    }
    {
        let account = Entity::new_account("account");
        let dataset2 = Entity::new_dataset("dataset2");

        assert_matches!(
            rebac_repo
                .delete_entities_relation(
                    &account,
                    Relation::account_is_a_dataset_reader(),
                    &dataset2
                )
                .await,
            Ok(())
        );

        let state = CrossoverTestState {
            account_id: "account",
            dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
            relation_map: HashMap::from([
                (Relation::account_is_a_dataset_reader(), [].into()),
                (
                    Relation::account_is_a_dataset_editor(),
                    ["dataset1", "dataset3"].into(),
                ),
            ]),
        };

        assert_get_relations(&rebac_repo, &state).await;
    }
    {
        let account = Entity::new_account("account");
        let dataset3 = Entity::new_dataset("dataset3");

        assert_matches!(
            rebac_repo
                .delete_entities_relation(
                    &account,
                    Relation::account_is_a_dataset_editor(),
                    &dataset3
                )
                .await,
            Ok(())
        );

        let state = CrossoverTestState {
            account_id: "account",
            dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
            relation_map: HashMap::from([
                (Relation::account_is_a_dataset_reader(), [].into()),
                (Relation::account_is_a_dataset_editor(), ["dataset1"].into()),
            ]),
        };

        assert_get_relations(&rebac_repo, &state).await;
    }
    {
        let account = Entity::new_account("account");
        let dataset1 = Entity::new_dataset("dataset1");

        assert_matches!(
            rebac_repo
                .delete_entities_relation(
                    &account,
                    Relation::account_is_a_dataset_editor(),
                    &dataset1
                )
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

        assert_get_relations(&rebac_repo, &state).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn assert_get_relations(rebac_repo: &Arc<dyn RebacRepository>, state: &CrossoverTestState) {
    let account = Entity::new_account(state.account_id);
    let mut object_entities = state.get_object_entities_with_relation();

    object_entities.sort();

    {
        match rebac_repo.get_subject_entity_relations(&account).await {
            Ok(mut actual_res) => {
                actual_res.sort();

                pretty_assertions::assert_eq!(object_entities, actual_res);
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

                    pretty_assertions::assert_eq!(object_entities, actual_res);
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

    // Check RebacRepository::get_relations_between_entities()
    {
        let relation_map = state.get_object_entity_relation_map();

        for dataset_id in &state.dataset_ids_for_check {
            let dataset = Entity::new_dataset(*dataset_id);

            match rebac_repo
                .get_relations_between_entities(&account, &dataset)
                .await
            {
                Ok(mut actual_relations) => {
                    actual_relations.sort();

                    let expected_relations = relation_map.get(dataset_id).unwrap();

                    pretty_assertions::assert_eq!(expected_relations, &actual_relations);
                }
                unexpected_res => {
                    panic!("Unexpected result: {unexpected_res:?}");
                }
            }
        }
    }
}

type AccountId = &'static str;
type DatasetId = &'static str;

struct CrossoverTestState {
    pub account_id: AccountId,
    pub dataset_ids_for_check: Vec<DatasetId>,
    pub relation_map: HashMap<Relation, HashSet<DatasetId>>,
}

impl CrossoverTestState {
    pub fn get_object_entities_with_relation(&self) -> Vec<EntityWithRelation> {
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

    pub fn get_object_entity_relation_map(&self) -> HashMap<DatasetId, Vec<Relation>> {
        self.dataset_ids_for_check
            .iter()
            .fold(HashMap::new(), |mut acc, dataset_id| {
                let relations = [
                    Relation::account_is_a_dataset_reader(),
                    Relation::account_is_a_dataset_editor(),
                ]
                .into_iter()
                .fold(Vec::new(), |mut acc, relation| {
                    if let Some(dataset_ids) = self.relation_map.get(&relation) {
                        if dataset_ids.contains(dataset_id) {
                            acc.push(relation);
                        }
                    }

                    acc
                });

                acc.insert(*dataset_id, relations);

                acc
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
