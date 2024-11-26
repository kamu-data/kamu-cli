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
    GetEntityPropertiesError,
    GetPropertiesError,
    InsertEntitiesRelationError,
    InsertRelationError,
    PropertyName,
    PropertyValue,
    RebacRepository,
    RebacService,
    Relation,
    SetEntityPropertyError,
    SubjectEntityRelationsError,
    UnsetEntityPropertyError,
    PROPERTY_VALUE_BOOLEAN_TRUE,
};
use opendatafabric as odf;

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
            .map_err(|err| match err {
                GetEntityPropertiesError::Internal(e) => e,
            })?;

        let account_properties = entity_properties
            .into_iter()
            .map(|(name, value)| match name {
                PropertyName::Dataset(_) => unreachable!(),
                PropertyName::Account(account_property_name) => (account_property_name, value),
            })
            .fold(AccountProperties::default(), |mut acc, (name, value)| {
                match name {
                    AccountPropertyName::IsAnAdmin => {
                        acc.is_admin = value == PROPERTY_VALUE_BOOLEAN_TRUE;
                    }
                };
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
            .map_err(|err| match err {
                GetEntityPropertiesError::Internal(e) => e,
            })?;

        let dataset_properties = entity_properties
            .into_iter()
            .map(|(name, value)| match name {
                PropertyName::Dataset(dataset_property_name) => (dataset_property_name, value),
                PropertyName::Account(_) => unreachable!(),
            })
            .fold(DatasetProperties::default(), |mut acc, (name, value)| {
                match name {
                    DatasetPropertyName::AllowsAnonymousRead => {
                        acc.allows_anonymous_read = value == PROPERTY_VALUE_BOOLEAN_TRUE;
                    }
                    DatasetPropertyName::AllowsPublicRead => {
                        acc.allows_public_read = value == PROPERTY_VALUE_BOOLEAN_TRUE;
                    }
                };
                acc
            });

        Ok(dataset_properties)
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
