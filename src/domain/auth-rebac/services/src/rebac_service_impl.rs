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
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_auth_rebac::{
    AccountProperties,
    AccountPropertyName,
    AccountToDatasetRelation,
    DatasetProperties,
    DatasetPropertyName,
    DeleteEntitiesRelationError,
    DeleteEntityPropertiesError,
    DeleteEntityPropertyError,
    DeletePropertiesError,
    DeleteRelationError,
    Entity,
    EntityWithRelation,
    GetPropertiesError,
    InsertEntitiesRelationError,
    InsertRelationError,
    PropertiesCountError,
    PropertyName,
    PropertyValue,
    RebacRepository,
    RebacService,
    Relation,
    SetEntityPropertyError,
    SubjectEntityRelationsError,
    UnsetEntityPropertyError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type DefaultAccountProperties = AccountProperties;
pub type DefaultDatasetProperties = DatasetProperties;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RebacServiceImpl {
    rebac_repo: Arc<dyn RebacRepository>,
    default_account_properties: Arc<DefaultAccountProperties>,
    default_dataset_properties: Arc<DefaultDatasetProperties>,
}

#[component(pub)]
#[interface(dyn RebacService)]
impl RebacServiceImpl {
    pub fn new(
        rebac_repo: Arc<dyn RebacRepository>,
        default_account_properties: Arc<DefaultAccountProperties>,
        default_dataset_properties: Arc<DefaultDatasetProperties>,
    ) -> Self {
        Self {
            rebac_repo,
            default_account_properties,
            default_dataset_properties,
        }
    }
}

#[async_trait::async_trait]
impl RebacService for RebacServiceImpl {
    async fn properties_count(&self) -> Result<usize, PropertiesCountError> {
        self.rebac_repo.properties_count().await
    }

    async fn set_account_property(
        &self,
        account_id: &odf::AccountID,
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
        account_id: &odf::AccountID,
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
        account_id: &odf::AccountID,
    ) -> Result<AccountProperties, GetPropertiesError> {
        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        let entity_properties = self
            .rebac_repo
            .get_entity_properties(&account_entity)
            .await
            .int_err()?;

        let default_account_properties = (*self.default_account_properties).clone();
        let account_properties = entity_properties
            .into_iter()
            .map(|(name, value)| match name {
                PropertyName::Dataset(_) => unreachable!(),
                PropertyName::Account(account_property_name) => (account_property_name, value),
            })
            .fold(default_account_properties, |mut acc, (name, value)| {
                acc.apply(name, &value);
                acc
            });

        Ok(account_properties)
    }

    async fn set_dataset_property(
        &self,
        dataset_id: &odf::DatasetID,
        property_name: DatasetPropertyName,
        property_value: &PropertyValue,
    ) -> Result<(), SetEntityPropertyError> {
        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .set_entity_property(&dataset_entity, property_name.into(), property_value)
            .await
    }

    async fn unset_dataset_property(
        &self,
        dataset_id: &odf::DatasetID,
        property_name: DatasetPropertyName,
    ) -> Result<(), UnsetEntityPropertyError> {
        use futures::FutureExt;

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .delete_entity_property(&dataset_entity, property_name.into())
            .map(map_delete_entity_property_result)
            .await
    }

    async fn delete_dataset_properties(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), DeletePropertiesError> {
        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id.as_str());

        match self
            .rebac_repo
            .delete_entity_properties(&dataset_entity)
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => match err {
                DeleteEntityPropertiesError::NotFound(_) => Ok(()),
                DeleteEntityPropertiesError::Internal(e) => Err(DeletePropertiesError::Internal(e)),
            },
        }
    }

    async fn get_dataset_properties(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetProperties, GetPropertiesError> {
        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id.as_str());

        let entity_properties = self
            .rebac_repo
            .get_entity_properties(&dataset_entity)
            .await
            .int_err()?;

        let default_dataset_properties = (*self.default_dataset_properties).clone();
        let dataset_properties = entity_properties
            .into_iter()
            .map(|(name, value)| match name {
                PropertyName::Dataset(dataset_property_name) => (dataset_property_name, value),
                PropertyName::Account(_) => unreachable!(),
            })
            .fold(default_dataset_properties, |mut acc, (name, value)| {
                acc.apply(name, &value);
                acc
            });

        Ok(dataset_properties)
    }

    async fn get_dataset_properties_by_ids(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<HashMap<odf::DatasetID, DatasetProperties>, GetPropertiesError> {
        let dataset_entities = dataset_ids
            .iter()
            .map(|id| Entity::new_dataset(id.to_string()))
            .collect::<Vec<_>>();

        let entity_properties = self
            .rebac_repo
            .get_entities_properties(&dataset_entities)
            .await
            .int_err()?;

        let mut dataset_properties_map = HashMap::new();
        let default_dataset_properties = (*self.default_dataset_properties).clone();

        for dataset_id in dataset_ids {
            dataset_properties_map.insert(dataset_id.clone(), default_dataset_properties.clone());
        }

        let entity_properties_it =
            entity_properties
                .into_iter()
                .map(|(entity, name, value)| match name {
                    PropertyName::Dataset(dataset_property_name) => {
                        (entity.entity_id, dataset_property_name, value)
                    }
                    PropertyName::Account(_) => unreachable!(),
                });

        for (entity_id, name, value) in entity_properties_it {
            let dataset_id = odf::DatasetID::from_did_str(&entity_id).int_err()?;
            let dataset_properties = dataset_properties_map
                .get_mut(&dataset_id)
                .ok_or_else(|| format!("dataset_id not found: {dataset_id}").int_err())?;

            dataset_properties.apply(name, &value);
        }

        Ok(dataset_properties_map)
    }

    async fn insert_account_dataset_relation(
        &self,
        account_id: &odf::AccountID,
        relationship: AccountToDatasetRelation,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InsertRelationError> {
        use futures::FutureExt;

        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .insert_entities_relation(
                &account_entity,
                Relation::AccountToDataset(relationship),
                &dataset_entity,
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
        account_id: &odf::AccountID,
        relationship: AccountToDatasetRelation,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), DeleteRelationError> {
        let account_id = account_id.as_did_str().to_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id.as_str());

        match self
            .rebac_repo
            .delete_entities_relation(
                &account_entity,
                Relation::AccountToDataset(relationship),
                &dataset_entity,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => match err {
                DeleteEntitiesRelationError::NotFound(_) => Ok(()),
                DeleteEntitiesRelationError::Internal(e) => Err(DeleteRelationError::Internal(e)),
            },
        }
    }

    async fn get_account_dataset_relations(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsError> {
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
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_delete_entity_property_result(
    res: Result<(), DeleteEntityPropertyError>,
) -> Result<(), UnsetEntityPropertyError> {
    match res {
        Ok(_) => Ok(()),
        Err(err) => match err {
            DeleteEntityPropertyError::NotFound(_) => Ok(()),
            DeleteEntityPropertyError::Internal(e) => Err(UnsetEntityPropertyError::Internal(e)),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
