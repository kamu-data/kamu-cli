// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use dill::{component, interface};
use kamu_auth_rebac::{
    DeleteEntitiesRelationError,
    DeleteEntityPropertyError,
    Entity,
    GetEntityPropertiesError,
    InsertEntitiesRelationError,
    ObjectEntity,
    Property,
    PropertyName,
    PropertyValue,
    RebacRepository,
    RebacService,
    Relation,
    SubjectEntityRelationsError,
    UpsertEntityPropertyError,
};
use opendatafabric::{AccountID, DatasetID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RebacServiceImpl {
    rebac_repo: Arc<dyn RebacRepository>,
}

#[component(pub)]
#[interface(dyn RebacService)]
impl RebacServiceImpl {
    pub fn new(rebac_repo: Arc<dyn RebacRepository>) -> Self {
        Self { rebac_repo }
    }
}

#[async_trait::async_trait]
impl RebacService for RebacServiceImpl {
    async fn set_account_property(
        &self,
        account_id: &AccountID,
        property: &Property,
    ) -> Result<(), UpsertEntityPropertyError> {
        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        self.rebac_repo
            .set_entity_property(&account_entity, property)
            .await
    }

    async fn unset_account_property(
        &self,
        account_id: &AccountID,
        property_name: PropertyName,
    ) -> Result<(), DeleteEntityPropertyError> {
        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        self.rebac_repo
            .delete_entity_property(&account_entity, property_name)
            .await
    }

    async fn get_account_properties(
        &self,
        account_id: &AccountID,
    ) -> Result<HashMap<PropertyName, PropertyValue>, GetEntityPropertiesError> {
        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        let properties = self
            .rebac_repo
            .get_entity_properties(&account_entity)
            .await?;

        Ok(represent_properties_as_a_map(properties))
    }

    async fn set_dataset_property(
        &self,
        dataset_id: &DatasetID,
        property: &Property,
    ) -> Result<(), UpsertEntityPropertyError> {
        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .set_entity_property(&dataset_id_entity, property)
            .await
    }

    async fn unset_dataset_property(
        &self,
        dataset_id: &DatasetID,
        property_name: PropertyName,
    ) -> Result<(), DeleteEntityPropertyError> {
        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .delete_entity_property(&dataset_id_entity, property_name)
            .await
    }

    async fn get_dataset_properties(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<HashMap<PropertyName, PropertyValue>, GetEntityPropertiesError> {
        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_entity = Entity::new_dataset(dataset_id.as_str());

        let properties = self
            .rebac_repo
            .get_entity_properties(&dataset_id_entity)
            .await?;

        Ok(represent_properties_as_a_map(properties))
    }

    async fn insert_account_dataset_relation(
        &self,
        account_id: &AccountID,
        relationship: Relation,
        dataset_id: &DatasetID,
    ) -> Result<(), InsertEntitiesRelationError> {
        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .insert_entities_relation(&account_entity, relationship, &dataset_id_entity)
            .await?;

        Ok(())
    }

    async fn delete_account_dataset_relation(
        &self,
        account_id: &AccountID,
        relationship: Relation,
        dataset_id: &DatasetID,
    ) -> Result<(), DeleteEntitiesRelationError> {
        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .delete_entities_relation(&account_entity, relationship, &dataset_id_entity)
            .await?;

        Ok(())
    }

    async fn get_account_dataset_relations(
        &self,
        account_id: &AccountID,
    ) -> Result<HashMap<Relation, Vec<ObjectEntity>>, SubjectEntityRelationsError> {
        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        let object_entities = self
            .rebac_repo
            .get_subject_entity_relations(&account_entity)
            .await?;

        let mut res = HashMap::with_capacity(object_entities.len());

        for object_entity in object_entities {
            res.entry(object_entity.relation)
                .or_insert_with(Vec::new)
                .push(object_entity.into());
        }

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn represent_properties_as_a_map(
    properties: Vec<Property>,
) -> HashMap<PropertyName, PropertyValue> {
    let mut res = HashMap::with_capacity(properties.len());

    for property in properties {
        res.insert(property.name, property.value.into_owned());
    }

    res
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
