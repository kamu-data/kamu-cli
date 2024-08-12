// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use kamu_auth_rebac::{
    AccountPropertyName,
    AccountToDatasetRelation,
    DatasetPropertyName,
    DeleteEntitiesRelationError,
    DeleteEntityPropertyError,
    Entity,
    GetEntityPropertiesError,
    InsertEntitiesRelationError,
    InsertRelationError,
    ObjectEntityWithRelation,
    PropertyName,
    PropertyValue,
    RebacRepository,
    RebacService,
    Relation,
    SetEntityPropertyError,
    SubjectEntityRelationsError,
    UnsetEntityPropertyError,
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
        property_name: AccountPropertyName,
        property_value: &PropertyValue,
    ) -> Result<(), SetEntityPropertyError> {
        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        self.rebac_repo
            .set_entity_property(&account_entity, property_name.into(), property_value)
            .await
    }

    async fn unset_account_property(
        &self,
        account_id: &AccountID,
        property_name: AccountPropertyName,
    ) -> Result<(), UnsetEntityPropertyError> {
        use futures::FutureExt;

        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        self.rebac_repo
            .delete_entity_property(&account_entity, property_name.into())
            .map(map_delete_entity_property_result)
            .await
    }

    async fn get_account_properties(
        &self,
        account_id: &AccountID,
    ) -> Result<Vec<(PropertyName, PropertyValue)>, GetEntityPropertiesError> {
        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        let properties = self
            .rebac_repo
            .get_entity_properties(&account_entity)
            .await?;

        Ok(properties)
    }

    async fn set_dataset_property(
        &self,
        dataset_id: &DatasetID,
        property_name: DatasetPropertyName,
        property_value: &PropertyValue,
    ) -> Result<(), SetEntityPropertyError> {
        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .set_entity_property(&dataset_id_entity, property_name.into(), property_value)
            .await
    }

    async fn unset_dataset_property(
        &self,
        dataset_id: &DatasetID,
        property_name: DatasetPropertyName,
    ) -> Result<(), UnsetEntityPropertyError> {
        use futures::FutureExt;

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .delete_entity_property(&dataset_id_entity, property_name.into())
            .map(map_delete_entity_property_result)
            .await
    }

    async fn get_dataset_properties(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Vec<(PropertyName, PropertyValue)>, GetEntityPropertiesError> {
        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_entity = Entity::new_dataset(dataset_id.as_str());

        let properties = self
            .rebac_repo
            .get_entity_properties(&dataset_id_entity)
            .await?;

        Ok(properties)
    }

    async fn insert_account_dataset_relation(
        &self,
        account_id: &AccountID,
        relationship: AccountToDatasetRelation,
        dataset_id: &DatasetID,
    ) -> Result<(), InsertRelationError> {
        use futures::FutureExt;

        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .insert_entities_relation(
                &account_entity,
                Relation::AccountToDataset(relationship),
                &dataset_id_entity,
            )
            .map(|res| match res {
                Ok(_) => Ok(()),
                Err(err) => match err {
                    InsertEntitiesRelationError::Duplicate(_) => Ok(()),
                    InsertEntitiesRelationError::Internal(e) => {
                        Err(InsertRelationError::Internal(e))
                    }
                },
            })
            .await
    }

    async fn delete_account_dataset_relation(
        &self,
        account_id: &AccountID,
        relationship: AccountToDatasetRelation,
        dataset_id: &DatasetID,
    ) -> Result<(), DeleteEntitiesRelationError> {
        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .delete_entities_relation(
                &account_entity,
                Relation::AccountToDataset(relationship),
                &dataset_id_entity,
            )
            .await?;

        Ok(())
    }

    async fn get_account_dataset_relations(
        &self,
        account_id: &AccountID,
    ) -> Result<Vec<ObjectEntityWithRelation>, SubjectEntityRelationsError> {
        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        let object_entities = self
            .rebac_repo
            .get_subject_entity_relations(&account_entity)
            .await?;

        Ok(object_entities)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_delete_entity_property_result(
    res: Result<(), DeleteEntityPropertyError>,
) -> Result<(), UnsetEntityPropertyError> {
    match res {
        Ok(_) => Ok(()),
        Err(err) => match err {
            DeleteEntityPropertyError::EntityNotFound(e) => {
                Err(UnsetEntityPropertyError::NotFound(e))
            }
            DeleteEntityPropertyError::PropertyNotFound(_) => Ok(()),
            DeleteEntityPropertyError::Internal(e) => Err(UnsetEntityPropertyError::Internal(e)),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
