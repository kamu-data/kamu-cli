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

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
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

struct CurrentAccountProperties<'a> {
    current_properties: AccountProperties,
    default_properties: &'a AccountProperties,
}

impl CurrentAccountProperties<'_> {
    fn matches(&self, property_name: AccountPropertyName, property_value: &PropertyValue) -> bool {
        self.current_properties.as_property_value(property_name) == *property_value
    }

    fn is_default(&self, property_name: AccountPropertyName) -> bool {
        self.current_properties.as_property_value(property_name)
            == self.default_properties.as_property_value(property_name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CurrentDatasetProperties<'a> {
    current_properties: DatasetProperties,
    default_properties: &'a DatasetProperties,
    has_explicit_overrides: bool,
}

impl CurrentDatasetProperties<'_> {
    fn matches(&self, property_name: DatasetPropertyName, property_value: &PropertyValue) -> bool {
        self.current_properties.as_property_value(property_name) == *property_value
    }

    fn is_default(&self, property_name: DatasetPropertyName) -> bool {
        self.current_properties.as_property_value(property_name)
            == self.default_properties.as_property_value(property_name)
    }

    fn has_explicit_overrides(&self) -> bool {
        self.has_explicit_overrides
    }
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

    async fn get_current_account_dataset_relations(
        &self,
        dataset_ids: &HashSet<odf::DatasetID>,
    ) -> Result<CurrentAccountDatasetRelations, InternalError> {
        if dataset_ids.is_empty() {
            return Ok(CurrentAccountDatasetRelations::default());
        }

        let dataset_entities = dataset_ids
            .iter()
            .map(|id| Entity::new_dataset(id.to_string()))
            .collect::<Vec<_>>();

        let entities_with_relations = self
            .rebac_repo
            .get_object_entities_relations(&dataset_entities)
            .await
            .int_err()?;

        let mut current_relations = CurrentAccountDatasetRelations::default();

        for EntitiesWithRelation {
            subject_entity,
            relation,
            object_entity,
        } in entities_with_relations
        {
            let account_id = odf::AccountID::from_did_str(&subject_entity.entity_id).int_err()?;
            let dataset_id = odf::DatasetID::from_did_str(&object_entity.entity_id).int_err()?;

            match relation {
                Relation::AccountToDataset(relation) => {
                    current_relations.insert(dataset_id, account_id, relation);
                }
            }
        }

        Ok(current_relations)
    }

    async fn get_current_account_properties(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<CurrentAccountProperties<'_>, GetPropertiesError> {
        let current_properties = self.get_account_properties(account_id).await?;

        Ok(CurrentAccountProperties {
            current_properties,
            default_properties: self.default_account_properties.as_ref(),
        })
    }

    async fn get_current_dataset_properties(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<CurrentDatasetProperties<'_>, GetPropertiesError> {
        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id.as_str());

        let entity_properties = self
            .rebac_repo
            .get_entity_properties(&dataset_entity)
            .await
            .int_err()?;

        let has_explicit_overrides = !entity_properties.is_empty();
        let current_properties = entity_properties
            .into_iter()
            .map(|(name, value)| match name {
                PropertyName::Dataset(dataset_property_name) => (dataset_property_name, value),
                PropertyName::Account(_) => unreachable!(),
            })
            .fold(
                (*self.default_dataset_properties).clone(),
                |mut acc, (name, value)| {
                    acc.apply(name, &value);
                    acc
                },
            );

        Ok(CurrentDatasetProperties {
            current_properties,
            default_properties: self.default_dataset_properties.as_ref(),
            has_explicit_overrides,
        })
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
    ) -> Result<PropertiesMutationResult, SetEntityPropertyError> {
        let current_properties = self
            .get_current_account_properties(account_id)
            .await
            .int_err()?;
        if current_properties.matches(property_name, property_value) {
            return Ok(PropertiesMutationResult::unchanged());
        }

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

        Ok(PropertiesMutationResult::changed())
    }

    async fn unset_account_property(
        &self,
        account_id: &odf::AccountID,
        property_name: AccountPropertyName,
    ) -> Result<PropertiesMutationResult, UnsetEntityPropertyError> {
        use futures::FutureExt;
        use odf::metadata::AsStackString;

        let current_properties = self
            .get_current_account_properties(account_id)
            .await
            .int_err()?;
        if current_properties.is_default(property_name) {
            return Ok(PropertiesMutationResult::unchanged());
        }

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

        Ok(PropertiesMutationResult::changed())
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
    ) -> Result<PropertiesMutationResult, SetEntityPropertyError> {
        let current_properties = self
            .get_current_dataset_properties(dataset_id)
            .await
            .int_err()?;
        if current_properties.matches(property_name, property_value) {
            return Ok(PropertiesMutationResult::unchanged());
        }

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .set_entity_property(&dataset_entity, property_name.into(), property_value)
            .await?;

        Ok(PropertiesMutationResult::changed())
    }

    async fn unset_dataset_property(
        &self,
        dataset_id: &odf::DatasetID,
        property_name: DatasetPropertyName,
    ) -> Result<PropertiesMutationResult, UnsetEntityPropertyError> {
        use futures::FutureExt;

        let current_properties = self
            .get_current_dataset_properties(dataset_id)
            .await
            .int_err()?;
        if current_properties.is_default(property_name) {
            return Ok(PropertiesMutationResult::unchanged());
        }

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id.as_str());

        self.rebac_repo
            .delete_entity_property(&dataset_entity, property_name.into())
            .map(map_delete_entity_property_result)
            .await?;

        Ok(PropertiesMutationResult::changed())
    }

    async fn delete_dataset_properties(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<PropertiesMutationResult, DeletePropertiesError> {
        let current_properties = self
            .get_current_dataset_properties(dataset_id)
            .await
            .int_err()?;
        if !current_properties.has_explicit_overrides() {
            return Ok(PropertiesMutationResult::unchanged());
        }

        let dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id.as_str());

        match self
            .rebac_repo
            .delete_entity_properties(&dataset_entity)
            .await
        {
            Ok(_) => Ok(PropertiesMutationResult::changed()),
            Err(e) => match e {
                DeleteEntityPropertiesError::NotFound(_) => {
                    Ok(PropertiesMutationResult::unchanged())
                }
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
    ) -> Result<AccountDatasetRelationsMutationResult, UpsertEntitiesRelationsError> {
        use odf::metadata::AsStackString;

        let account_id_stack = account_id.as_stack_string();
        let account_entity = Entity::new_account(account_id_stack.as_str());

        let dataset_id_stack = dataset_id.as_did_str().to_stack_string();
        let dataset_entity = Entity::new_dataset(dataset_id_stack.as_str());

        let existing_relation = self
            .rebac_repo
            .try_get_relation_between_entities(&account_entity, &dataset_entity)
            .await
            .int_err()?;

        if existing_relation == Some(Relation::AccountToDataset(relationship)) {
            return Ok(AccountDatasetRelationsMutationResult::default());
        }

        self.rebac_repo
            .upsert_entities_relations(&[UpsertEntitiesRelationOperation {
                subject_entity: Cow::Owned(account_entity),
                relationship: Relation::AccountToDataset(relationship),
                object_entity: Cow::Owned(dataset_entity),
            }])
            .await
            .int_err()?;

        Ok(AccountDatasetRelationsMutationResult::from_dataset_id(
            dataset_id.clone(),
        ))
    }

    async fn set_account_dataset_relations(
        &self,
        operations: &[SetAccountDatasetRelationsOperation<'_>],
    ) -> Result<AccountDatasetRelationsMutationResult, UpsertEntitiesRelationsError> {
        let dataset_ids = operations
            .iter()
            .map(|op| op.dataset_id.as_ref().clone())
            .collect::<HashSet<_>>();

        let current_relations = self
            .get_current_account_dataset_relations(&dataset_ids)
            .await
            .int_err()?;

        let mut upsert_operations = Vec::new();
        let mut changed_dataset_ids = HashSet::new();

        for op in operations {
            let account_entity = Entity::new_account(op.account_id.to_string());
            let dataset_entity = Entity::new_dataset(op.dataset_id.to_string());
            let relation = Relation::AccountToDataset(op.relationship);

            if current_relations.matches(
                op.dataset_id.as_ref(),
                op.account_id.as_ref(),
                op.relationship,
            ) {
                continue;
            }

            upsert_operations.push(UpsertEntitiesRelationOperation {
                subject_entity: Cow::Owned(account_entity),
                relationship: relation,
                object_entity: Cow::Owned(dataset_entity),
            });
            changed_dataset_ids.insert(op.dataset_id.as_ref().clone());
        }

        if upsert_operations.is_empty() {
            return Ok(AccountDatasetRelationsMutationResult::default());
        }

        self.rebac_repo
            .upsert_entities_relations(&upsert_operations)
            .await
            .int_err()?;

        Ok(AccountDatasetRelationsMutationResult::new(
            changed_dataset_ids,
        ))
    }

    async fn unset_accounts_dataset_relations(
        &self,
        account_ids: &[&odf::AccountID],
        dataset_id: &odf::DatasetID,
    ) -> Result<AccountDatasetRelationsMutationResult, DeleteEntitiesRelationsError> {
        let dataset_ids = HashSet::from([dataset_id.clone()]);

        let current_relations = self
            .get_current_account_dataset_relations(&dataset_ids)
            .await
            .int_err()?;

        let mut delete_operations = Vec::new();

        for account_id in account_ids {
            if !current_relations.contains(dataset_id, account_id) {
                continue;
            }

            delete_operations.push(DeleteEntitiesRelationOperation {
                subject_entity: Cow::Owned(Entity::new_account((*account_id).to_string())),
                object_entity: Cow::Owned(Entity::new_dataset(dataset_id.to_string())),
            });
        }

        if delete_operations.is_empty() {
            return Ok(AccountDatasetRelationsMutationResult::default());
        }

        self.rebac_repo
            .delete_entities_relations(&delete_operations)
            .await
            .int_err()?;

        Ok(AccountDatasetRelationsMutationResult::from_dataset_id(
            dataset_id.clone(),
        ))
    }

    async fn unset_account_dataset_relations(
        &self,
        operations: &[UnsetAccountDatasetRelationsOperation<'_>],
    ) -> Result<AccountDatasetRelationsMutationResult, DeleteEntitiesRelationsError> {
        let dataset_ids = operations
            .iter()
            .map(|op| op.dataset_id.as_ref().clone())
            .collect::<HashSet<_>>();

        let current_relations = self
            .get_current_account_dataset_relations(&dataset_ids)
            .await
            .int_err()?;

        let mut delete_operations = Vec::new();
        let mut changed_dataset_ids = HashSet::new();

        for op in operations {
            if !current_relations.contains(op.dataset_id.as_ref(), op.account_id.as_ref()) {
                continue;
            }

            delete_operations.push(DeleteEntitiesRelationOperation {
                subject_entity: Cow::Owned(Entity::new_account(op.account_id.to_string())),
                object_entity: Cow::Owned(Entity::new_dataset(op.dataset_id.to_string())),
            });
            changed_dataset_ids.insert(op.dataset_id.as_ref().clone());
        }

        if delete_operations.is_empty() {
            return Ok(AccountDatasetRelationsMutationResult::default());
        }

        self.rebac_repo
            .delete_entities_relations(&delete_operations)
            .await
            .int_err()?;

        Ok(AccountDatasetRelationsMutationResult::new(
            changed_dataset_ids,
        ))
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

    async fn get_authorized_datasets_by_account_ids(
        &self,
        account_ids: &[&odf::AccountID],
    ) -> Result<HashMap<odf::AccountID, Vec<AuthorizedDataset>>, GetObjectEntityRelationsError>
    {
        let account_entities = account_ids
            .iter()
            .map(|id| Entity::new_account(id.to_string()))
            .collect::<Vec<_>>();

        let entities_with_relations = self
            .rebac_repo
            .get_subject_entities_relations(&account_entities)
            .await
            .int_err()?;

        let mut account_datasets_relation_map = HashMap::new();

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
                    let authorized_datasets = account_datasets_relation_map
                        .entry(account_id)
                        .or_insert_with(Vec::new);
                    let authorized_dataset = AuthorizedDataset { dataset_id, role };

                    authorized_datasets.push(authorized_dataset);
                }
            }
        }

        Ok(account_datasets_relation_map)
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
            .get_object_entities_relations(&dataset_entities)
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

#[derive(Default)]
struct CurrentAccountDatasetRelations {
    relations: HashMap<odf::DatasetID, HashMap<odf::AccountID, AccountToDatasetRelation>>,
}

impl CurrentAccountDatasetRelations {
    fn insert(
        &mut self,
        dataset_id: odf::DatasetID,
        account_id: odf::AccountID,
        relation: AccountToDatasetRelation,
    ) {
        self.relations
            .entry(dataset_id)
            .or_default()
            .insert(account_id, relation);
    }

    fn contains(&self, dataset_id: &odf::DatasetID, account_id: &odf::AccountID) -> bool {
        self.relations
            .get(dataset_id)
            .and_then(|dataset_relations| dataset_relations.get(account_id))
            .is_some()
    }

    fn matches(
        &self,
        dataset_id: &odf::DatasetID,
        account_id: &odf::AccountID,
        relation: AccountToDatasetRelation,
    ) -> bool {
        self.relations
            .get(dataset_id)
            .and_then(|dataset_relations| dataset_relations.get(account_id))
            == Some(&relation)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
