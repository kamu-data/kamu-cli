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
    DeleteEntitiesRelationError,
    DeleteEntityPropertyError,
    Entity,
    EntityType,
    GetEntityPropertiesError,
    GetRelationsBetweenEntitiesError,
    InsertEntitiesRelationError,
    ObjectEntity,
    ObjectEntityWithRelation,
    Property,
    PropertyName,
    RebacRepository,
    Relation,
    SubjectEntityRelationsByObjectTypeError,
    SubjectEntityRelationsError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_get_properties_from_nonexistent_entity(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let nonexistent_entity = Entity::new_dataset("foo");

    let res = rebac_repo.get_entity_properties(&nonexistent_entity).await;

    assert_matches!(
        res,
        Err(GetEntityPropertiesError::NotFound(e))
            if e.entity_type == EntityType::Dataset
                && e.entity_id == *"foo"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_set_property(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let entity = Entity::new_dataset("bar");
    let anon_read_property = Property::new(PropertyName::DatasetAllowsAnonymousRead, "true");

    let set_res = rebac_repo
        .set_entity_property(&entity, &anon_read_property)
        .await;

    assert_matches!(set_res, Ok(_));

    let get_res = rebac_repo.get_entity_properties(&entity).await;
    let expected_properties = vec![anon_read_property];

    assert_matches!(
        get_res,
        Ok(actual_properties)
            if expected_properties == actual_properties
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_delete_property_from_nonexistent_entity(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let nonexistent_entity = Entity::new_dataset("foo");

    let delete_res = rebac_repo
        .delete_entity_property(
            &nonexistent_entity,
            PropertyName::DatasetAllowsAnonymousRead,
        )
        .await;

    assert_matches!(
        delete_res,
        Err(DeleteEntityPropertyError::EntityNotFound(e))
            if e.entity_type == EntityType::Dataset
                && e.entity_id == *"foo"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_delete_nonexistent_property_from_entity(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let entity = Entity::new_dataset("bar");
    let anon_read_property = Property::new(PropertyName::DatasetAllowsAnonymousRead, "true");

    let set_res = rebac_repo
        .set_entity_property(&entity, &anon_read_property)
        .await;

    assert_matches!(set_res, Ok(_));

    let delete_res = rebac_repo
        .delete_entity_property(&entity, PropertyName::DatasetAllowsPublicRead)
        .await;

    assert_matches!(
        delete_res,
        Err(DeleteEntityPropertyError::PropertyNotFound(e))
            if e.entity_type == EntityType::Dataset
                && e.entity_id == *"bar"
                && e.property_name == PropertyName::DatasetAllowsPublicRead
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_property_from_entity(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let entity = Entity::new_dataset("bar");
    let anon_read_property = Property::new(PropertyName::DatasetAllowsAnonymousRead, "true");

    {
        let set_res = rebac_repo
            .set_entity_property(&entity, &anon_read_property)
            .await;

        assert_matches!(set_res, Ok(_));
    }

    let public_read_property = Property::new(PropertyName::DatasetAllowsPublicRead, "true");

    {
        let set_res = rebac_repo
            .set_entity_property(&entity, &public_read_property)
            .await;

        assert_matches!(set_res, Ok(_));
    }

    {
        let get_res = rebac_repo.get_entity_properties(&entity).await;
        let expected_properties = vec![anon_read_property, public_read_property.clone()];

        assert_matches!(
            get_res,
            Ok(actual_properties)
                if expected_properties == actual_properties
        );
    }

    {
        let delete_res = rebac_repo
            .delete_entity_property(&entity, PropertyName::DatasetAllowsAnonymousRead)
            .await;

        assert_matches!(delete_res, Ok(_));
    }

    {
        let delete_res = rebac_repo
            .delete_entity_property(&entity, PropertyName::DatasetAllowsAnonymousRead)
            .await;

        assert_matches!(
            delete_res,
            Err(DeleteEntityPropertyError::PropertyNotFound(e))
                if e.entity_type == EntityType::Dataset
                    && e.entity_id == *"bar"
                    && e.property_name == PropertyName::DatasetAllowsAnonymousRead
        );
    }

    {
        let get_res = rebac_repo.get_entity_properties(&entity).await;
        let expected_properties = vec![public_read_property];

        assert_matches!(
            get_res,
            Ok(actual_properties)
                if actual_properties == expected_properties
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_insert_duplicate_entities_relation(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let account = Entity::new_account("kamu");
    let dataset = Entity::new_account("dataset");
    let relationship = Relation::AccountDatasetReader;

    {
        let insert_res = rebac_repo
            .insert_entities_relation(&account, relationship, &dataset)
            .await;

        assert_matches!(insert_res, Ok(()));
    }
    {
        let insert_res = rebac_repo
            .insert_entities_relation(&account, relationship, &dataset)
            .await;

        assert_matches!(
            insert_res,
            Err(InsertEntitiesRelationError::Duplicate(e))
                if e.subject_entity_type == account.entity_type
                    && e.subject_entity_id == account.entity_id
                    && e.relationship == relationship
                    && e.object_entity_type == dataset.entity_type
                    && e.object_entity_id == dataset.entity_id
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_entities_relation(catalog: &Catalog) {
    let rebac_repo = catalog.get_one::<dyn RebacRepository>().unwrap();

    let account = Entity::new_account("kamu");
    let dataset = Entity::new_account("dataset");
    let relationship = Relation::AccountDatasetReader;

    let insert_res = rebac_repo
        .insert_entities_relation(&account, relationship, &dataset)
        .await;

    assert_matches!(insert_res, Ok(()));

    {
        let delete_res = rebac_repo
            .delete_entities_relation(&account, relationship, &dataset)
            .await;

        assert_matches!(delete_res, Ok(()));
    }
    {
        let delete_res = rebac_repo
            .delete_entities_relation(&account, relationship, &dataset)
            .await;

        assert_matches!(
            delete_res,
            Err(DeleteEntitiesRelationError::NotFound(e))
                if e.subject_entity_type == account.entity_type
                    && e.subject_entity_id == account.entity_id
                    && e.relationship == relationship
                    && e.object_entity_type == dataset.entity_type
                    && e.object_entity_id == dataset.entity_id
        );
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
                Relation::AccountDatasetReader,
                ["dataset1", "dataset2"].into(),
            ),
            (
                Relation::AccountDatasetEditor,
                ["dataset1", "dataset3"].into(),
            ),
        ]),
    };
    // TODO: try Map<DatasetId, [Relation]>?

    {
        let account = Entity::new_account(state.account_id);

        for (relation, dataset_ids) in &state.relation_map {
            for dataset_id in dataset_ids {
                let dataset = Entity::new_dataset(*dataset_id);

                let insert_res = rebac_repo
                    .insert_entities_relation(&account, *relation, &dataset)
                    .await;

                assert_matches!(insert_res, Ok(_));
            }
        }
    }

    // 2. Validate inserted

    assert_get_relations(&rebac_repo, &state).await;

    // 3. Deletions

    {
        let account = ObjectEntity::new_account("account");
        let dataset1 = ObjectEntity::new_dataset("dataset1");

        let delete_res = rebac_repo
            .delete_entities_relation(&account, Relation::AccountDatasetReader, &dataset1)
            .await;

        assert_matches!(delete_res, Ok(()));

        let state = CrossoverTestState {
            account_id: "account",
            dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
            relation_map: HashMap::from([
                (Relation::AccountDatasetReader, ["dataset2"].into()),
                (
                    Relation::AccountDatasetEditor,
                    ["dataset1", "dataset3"].into(),
                ),
            ]),
        };

        assert_get_relations(&rebac_repo, &state).await;
    }
    {
        let account = ObjectEntity::new_account("account");
        let dataset2 = ObjectEntity::new_dataset("dataset2");

        let delete_res = rebac_repo
            .delete_entities_relation(&account, Relation::AccountDatasetReader, &dataset2)
            .await;

        assert_matches!(delete_res, Ok(()));

        let state = CrossoverTestState {
            account_id: "account",
            dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
            relation_map: HashMap::from([
                (Relation::AccountDatasetReader, [].into()),
                (
                    Relation::AccountDatasetEditor,
                    ["dataset1", "dataset3"].into(),
                ),
            ]),
        };

        assert_get_relations(&rebac_repo, &state).await;
    }
    {
        let account = ObjectEntity::new_account("account");
        let dataset3 = ObjectEntity::new_dataset("dataset3");

        let delete_res = rebac_repo
            .delete_entities_relation(&account, Relation::AccountDatasetEditor, &dataset3)
            .await;

        assert_matches!(delete_res, Ok(()));

        let state = CrossoverTestState {
            account_id: "account",
            dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
            relation_map: HashMap::from([
                (Relation::AccountDatasetReader, [].into()),
                (Relation::AccountDatasetEditor, ["dataset1"].into()),
            ]),
        };

        assert_get_relations(&rebac_repo, &state).await;
    }
    {
        let account = ObjectEntity::new_account("account");
        let dataset1 = ObjectEntity::new_dataset("dataset1");

        let delete_res = rebac_repo
            .delete_entities_relation(&account, Relation::AccountDatasetEditor, &dataset1)
            .await;

        assert_matches!(delete_res, Ok(()));

        let state = CrossoverTestState {
            account_id: "account",
            dataset_ids_for_check: vec!["dataset1", "dataset2", "dataset3"],
            relation_map: HashMap::from([
                (Relation::AccountDatasetReader, [].into()),
                (Relation::AccountDatasetEditor, [].into()),
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
        let get_res = rebac_repo.get_subject_entity_relations(&account).await;

        match get_res {
            Ok(mut actual_res) => {
                actual_res.sort();

                assert_eq!(object_entities, actual_res);
            }
            Err(actual_error) if object_entities.is_empty() => {
                assert_matches!(
                    actual_error,
                    SubjectEntityRelationsError::NotFound(e)
                        if e.subject_entity_type == account.entity_type
                            && e.subject_entity_id == account.entity_id
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
            let get_res = rebac_repo
                .get_subject_entity_relations_by_object_type(&account, EntityType::Dataset)
                .await;

            match get_res {
                Ok(mut actual_res) => {
                    actual_res.sort();

                    assert_eq!(actual_res, object_entities);
                }
                Err(actual_error) if object_entities.is_empty() => {
                    assert_matches!(
                        actual_error,
                        SubjectEntityRelationsByObjectTypeError::NotFound(e)
                            if e.subject_entity_type == account.entity_type
                                && e.subject_entity_id == account.entity_id
                                && e.object_entity_type == EntityType::Dataset
                    );
                }
                unexpected_res => {
                    panic!("Unexpected result: {unexpected_res:?}");
                }
            }
        }
        {
            let actual_res = rebac_repo
                .get_subject_entity_relations_by_object_type(&account, EntityType::Account)
                .await;

            assert_matches!(
                actual_res,
                Err(SubjectEntityRelationsByObjectTypeError::NotFound(e))
                    if e.subject_entity_type == account.entity_type
                        && e.subject_entity_id == account.entity_id
                        && e.object_entity_type == EntityType::Account
            );
        }
    }

    // Check RebacRepository::get_relations_between_entities()
    {
        let relation_map = state.get_object_entity_relation_map();

        for dataset_id in &state.dataset_ids_for_check {
            let dataset = ObjectEntity::new_dataset(*dataset_id);

            let get_res = rebac_repo
                .get_relations_between_entities(&account, &dataset)
                .await;

            let expected_relations = relation_map.get(dataset_id).unwrap();

            match get_res {
                Ok(mut actual_relations) => {
                    actual_relations.sort();

                    assert_eq!(&actual_relations, expected_relations);
                }
                Err(actual_error) if expected_relations.is_empty() => {
                    assert_matches!(
                        actual_error,
                        GetRelationsBetweenEntitiesError::NotFound(e)
                            if e.subject_entity_type == account.entity_type
                                && e.subject_entity_id == account.entity_id
                                && e.object_entity_type == EntityType::Dataset
                                && e.object_entity_id == *dataset_id
                    );
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
    pub fn get_object_entities_with_relation(&self) -> Vec<ObjectEntityWithRelation> {
        self.relation_map
            .iter()
            .fold(Vec::new(), |mut acc, (relation, dataset_ids)| {
                let object_entity_iter = dataset_ids.iter().map(|dataset_id| {
                    ObjectEntityWithRelation::new_dataset(*dataset_id, *relation)
                });

                acc.extend(object_entity_iter);

                acc
            })
    }

    pub fn get_object_entity_relation_map(&self) -> HashMap<DatasetId, Vec<Relation>> {
        self.dataset_ids_for_check
            .iter()
            .fold(HashMap::new(), |mut acc, dataset_id| {
                let relations = [
                    Relation::AccountDatasetReader,
                    Relation::AccountDatasetEditor,
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
