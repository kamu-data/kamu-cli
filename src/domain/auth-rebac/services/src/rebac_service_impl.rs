// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_auth_rebac::*;
use tokio::sync::RwLock;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type DefaultAccountProperties = AccountProperties;
pub type DefaultDatasetProperties = DatasetProperties;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct AccountPropertiesCacheState {
    account_properties_cache_map: HashMap<String, AccountProperties>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RebacServiceImpl {
    cache_state: Arc<RwLock<AccountPropertiesCacheState>>,
    rebac_repo: Arc<dyn RebacRepository>,
    default_account_properties: Arc<DefaultAccountProperties>,
    default_dataset_properties: Arc<DefaultDatasetProperties>,
}

#[dill::component(pub)]
#[dill::interface(dyn RebacService)]
impl RebacServiceImpl {
    pub fn new(
        rebac_repo: Arc<dyn RebacRepository>,
        default_account_properties: Arc<DefaultAccountProperties>,
        default_dataset_properties: Arc<DefaultDatasetProperties>,
    ) -> Self {
        Self {
            cache_state: Arc::new(RwLock::new(AccountPropertiesCacheState::default())),
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
        let account_id = account_id.to_string();
        let account_entity = Entity::new_account(account_id.as_str());

        self.rebac_repo
            .set_entity_property(&account_entity, property_name.into(), property_value)
            .await?;

        // Update the cache state
        let mut writable_state = self.cache_state.write().await;
        let account_properties = writable_state
            .account_properties_cache_map
            .entry(account_id)
            .or_insert_with(|| (*self.default_account_properties).clone());

        account_properties.apply(property_name, property_value);

        Ok(())
    }

    async fn unset_account_property(
        &self,
        account_id: &odf::AccountID,
        property_name: AccountPropertyName,
    ) -> Result<(), UnsetEntityPropertyError> {
        use futures::FutureExt;
        use odf::metadata::AsStackString;

        let account_id = account_id.as_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        self.rebac_repo
            .delete_entity_property(&account_entity, property_name.into())
            .map(map_delete_entity_property_result)
            .await?;

        let mut writable_state = self.cache_state.write().await;
        writable_state
            .account_properties_cache_map
            .remove(account_id.as_str());

        Ok(())
    }

    async fn get_account_properties(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<AccountProperties, GetPropertiesError> {
        use odf::metadata::AsStackString;

        let account_id = account_id.as_stack_string();

        {
            let readable_state = self.cache_state.read().await;

            let maybe_cached_account_properties = readable_state
                .account_properties_cache_map
                .get(account_id.as_str())
                .cloned();

            if let Some(cached_account_properties) = maybe_cached_account_properties {
                return Ok(cached_account_properties);
            }
        }

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
            Err(e) => match e {
                DeleteEntityPropertiesError::NotFound(_) => Ok(()),
                e @ DeleteEntityPropertiesError::Internal(_) => Err(e.int_err().into()),
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

    async fn set_account_dataset_relation(
        &self,
        account_id: &odf::AccountID,
        relationship: AccountToDatasetRelation,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), UpsertEntitiesRelationsError> {
        use odf::metadata::AsStackString;

        let account_id_stack = account_id.as_stack_string();
        let account_entity = Entity::new_account(account_id_stack.as_str());

        let dataset_id_stack = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id_stack.as_str());

        self.rebac_repo
            .upsert_entities_relations(&[UpsertEntitiesRelationOperation {
                subject_entity: Cow::Owned(account_entity),
                relationship: Relation::AccountToDataset(relationship),
                object_entity: Cow::Owned(dataset_entity),
            }])
            .await
            .int_err()?;

        Ok(())
    }

    async fn set_account_dataset_relations(
        &self,
        operations: &[SetAccountDatasetRelationsOperation<'_>],
    ) -> Result<(), UpsertEntitiesRelationsError> {
        let upsert_operations = operations
            .iter()
            .map(|op| {
                let account_entity = Entity::new_account(op.account_id.to_string());
                let dataset_entity = Entity::new_dataset(op.dataset_id.to_string());

                UpsertEntitiesRelationOperation {
                    subject_entity: Cow::Owned(account_entity),
                    relationship: Relation::AccountToDataset(op.relationship),
                    object_entity: Cow::Owned(dataset_entity),
                }
            })
            .collect::<Vec<_>>();

        self.rebac_repo
            .upsert_entities_relations(&upsert_operations)
            .await
            .int_err()?;

        Ok(())
    }

    async fn unset_accounts_dataset_relations(
        &self,
        account_ids: &[&odf::AccountID],
        dataset_id: &odf::DatasetID,
    ) -> Result<(), DeleteEntitiesRelationsError> {
        let account_entities = account_ids
            .iter()
            .map(|id| Entity::new_account(id.to_string()))
            .collect::<Vec<_>>();

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id.as_str());

        let operations = account_entities
            .iter()
            .map(|account_entity| DeleteEntitiesRelationOperation {
                subject_entity: Cow::Borrowed(account_entity),
                object_entity: Cow::Borrowed(&dataset_entity),
            })
            .collect::<Vec<_>>();

        self.rebac_repo
            .delete_entities_relations(&operations)
            .await
            .int_err()?;

        Ok(())
    }

    async fn unset_account_dataset_relations(
        &self,
        operations: &[UnsetAccountDatasetRelationsOperation<'_>],
    ) -> Result<(), DeleteEntitiesRelationsError> {
        let delete_operations = operations
            .iter()
            .map(|op| DeleteEntitiesRelationOperation {
                subject_entity: Cow::Owned(Entity::new_account(op.account_id.to_string())),
                object_entity: Cow::Owned(Entity::new_dataset(op.dataset_id.to_string())),
            })
            .collect::<Vec<_>>();

        self.rebac_repo
            .delete_entities_relations(&delete_operations)
            .await
            .int_err()?;

        Ok(())
    }

    async fn get_account_dataset_relations(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsError> {
        use odf::metadata::AsStackString;

        let account_id = account_id.as_stack_string();
        let account_entity = Entity::new_account(account_id.as_str());

        let object_entities = self
            .rebac_repo
            .get_subject_entity_relations(&account_entity)
            .await?;

        Ok(object_entities)
    }

    async fn get_authorized_accounts(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<AuthorizedAccount>, GetObjectEntityRelationsError> {
        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id.as_str());

        let subject_entities = self
            .rebac_repo
            .get_object_entity_relations(&dataset_entity)
            .await?;

        let mut authorized_accounts = Vec::with_capacity(subject_entities.len());
        for EntityWithRelation { entity, relation } in subject_entities {
            let account_id = odf::AccountID::from_did_str(&entity.entity_id).int_err()?;

            match relation {
                Relation::AccountToDataset(role) => {
                    let authorized_account = AuthorizedAccount { account_id, role };

                    authorized_accounts.push(authorized_account);
                }
            }
        }

        Ok(authorized_accounts)
    }

    async fn get_authorized_accounts_by_ids(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<HashMap<odf::DatasetID, Vec<AuthorizedAccount>>, GetObjectEntityRelationsError>
    {
        let dataset_entities = dataset_ids
            .iter()
            .map(|id| Entity::new_dataset(id.to_string()))
            .collect::<Vec<_>>();

        let entities_with_relations = self
            .rebac_repo
            .get_object_entities_relations(&dataset_entities[..])
            .await
            .int_err()?;

        let mut dataset_accounts_relation_map = HashMap::new();

        for entities_with_relation in entities_with_relations {
            let EntitiesWithRelation {
                subject_entity,
                relation,
                object_entity,
            } = entities_with_relation;

            let account_id = odf::AccountID::from_did_str(&subject_entity.entity_id).int_err()?;
            let dataset_id = odf::DatasetID::from_did_str(&object_entity.entity_id).int_err()?;

            match relation {
                Relation::AccountToDataset(role) => {
                    let authorized_accounts = dataset_accounts_relation_map
                        .entry(dataset_id)
                        .or_insert_with(Vec::new);
                    let authorized_account = AuthorizedAccount { account_id, role };

                    authorized_accounts.push(authorized_account);
                }
            }
        }

        Ok(dataset_accounts_relation_map)
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
        Err(e) => match e {
            DeleteEntityPropertyError::NotFound(_) => Ok(()),
            e @ DeleteEntityPropertyError::Internal(_) => Err(e.int_err().into()),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
