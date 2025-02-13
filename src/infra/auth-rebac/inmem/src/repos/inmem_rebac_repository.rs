// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use dill::{component, interface, scope, Singleton};
use kamu_auth_rebac::{
    DeleteEntitiesRelationError,
    DeleteEntityPropertiesError,
    DeleteEntityPropertyError,
    Entity,
    EntityType,
    EntityWithRelation,
    GetEntityPropertiesError,
    GetRelationsBetweenEntitiesError,
    InsertEntitiesRelationError,
    PropertiesCountError,
    PropertyName,
    PropertyValue,
    RebacRepository,
    Relation,
    SetEntityPropertyError,
    SubjectEntityRelationsByObjectTypeError,
    SubjectEntityRelationsError,
};
use tokio::sync::RwLock;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type EntityProperties = HashMap<PropertyName, PropertyValue<'static>>;
type EntitiesPropertiesMap = HashMap<Entity<'static>, EntityProperties>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    entities_properties_map: EntitiesPropertiesMap,
    entities_relations_rows: HashSet<EntitiesRelationsRow>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryRebacRepository {
    state: Arc<RwLock<State>>,
}

#[component(pub)]
#[interface(dyn RebacRepository)]
#[scope(Singleton)]
impl InMemoryRebacRepository {
    pub fn new() -> Self {
        Self {
            state: Default::default(),
        }
    }

    async fn get_rows<F, R>(&self, filter_map_predicate: F) -> Vec<R>
    where
        F: Fn(&EntitiesRelationsRow) -> Option<R>,
    {
        let readable_state = self.state.read().await;

        readable_state
            .entities_relations_rows
            .iter()
            .filter_map(filter_map_predicate)
            .collect()
    }
}

#[async_trait::async_trait]
impl RebacRepository for InMemoryRebacRepository {
    async fn properties_count(&self) -> Result<usize, PropertiesCountError> {
        let readable_state = self.state.read().await;

        let count = readable_state
            .entities_properties_map
            .values()
            .fold(0, |acc, properties| acc + properties.len());

        Ok(count)
    }

    async fn set_entity_property(
        &self,
        entity: &Entity,
        property_name: PropertyName,
        property_value: &PropertyValue,
    ) -> Result<(), SetEntityPropertyError> {
        let mut writable_state = self.state.write().await;

        let entity_properties = writable_state
            .entities_properties_map
            .entry(entity.clone().into_owned())
            .or_insert_with(EntityProperties::new);

        entity_properties.insert(property_name, property_value.clone().into_owned().into());

        Ok(())
    }

    async fn delete_entity_property(
        &self,
        entity: &Entity,
        property_name: PropertyName,
    ) -> Result<(), DeleteEntityPropertyError> {
        let mut writable_state = self.state.write().await;

        let maybe_entity_properties = writable_state
            .entities_properties_map
            .get_mut(&entity.clone().into_owned());

        let Some(entity_properties) = maybe_entity_properties else {
            return Err(DeleteEntityPropertyError::not_found(entity, property_name));
        };

        let property_not_found = entity_properties.remove(&property_name).is_none();

        if property_not_found {
            return Err(DeleteEntityPropertyError::not_found(entity, property_name));
        }

        Ok(())
    }

    async fn delete_entity_properties(
        &self,
        entity: &Entity,
    ) -> Result<(), DeleteEntityPropertiesError> {
        let mut writable_state = self.state.write().await;

        let not_found = writable_state
            .entities_properties_map
            .remove(&entity.clone().into_owned())
            .is_none();

        if not_found {
            return Err(DeleteEntityPropertiesError::not_found(entity));
        };

        Ok(())
    }

    async fn get_entity_properties(
        &self,
        entity: &Entity,
    ) -> Result<Vec<(PropertyName, PropertyValue)>, GetEntityPropertiesError> {
        let readable_state = self.state.read().await;

        let maybe_entity_properties = readable_state.entities_properties_map.get(entity);

        let Some(entity_properties) = maybe_entity_properties else {
            return Ok(vec![]);
        };

        let properties = entity_properties
            .iter()
            .map(|(name, value)| (*name, value.clone()))
            .collect();

        Ok(properties)
    }

    async fn get_entity_properties_by_ids(
        &self,
        entities: &[Entity],
    ) -> Result<Vec<(Entity, PropertyName, PropertyValue)>, GetEntityPropertiesError> {
        let entities_set = entities.iter().cloned().collect::<HashSet<_>>();

        let readable_state = self.state.read().await;

        let entities_properties = readable_state
            .entities_properties_map
            .iter()
            .filter(|(entity, _)| entities_set.contains(entity))
            .map(|(entity, entity_properties)| {
                entity_properties
                    .iter()
                    .map(|(property_name, property_value)| {
                        (entity.clone(), *property_name, property_value.clone())
                    })
                    .collect::<Vec<_>>()
            })
            .fold(Vec::new(), |mut acc, entity_properties| {
                acc.extend(entity_properties);
                acc
            });

        Ok(entities_properties)
    }

    async fn insert_entities_relation(
        &self,
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> Result<(), InsertEntitiesRelationError> {
        let mut writable_state = self.state.write().await;

        let row = EntitiesRelationsRow::new(subject_entity, relationship, object_entity);
        let is_duplicate = writable_state.entities_relations_rows.contains(&row);

        if is_duplicate {
            return Err(InsertEntitiesRelationError::duplicate(
                subject_entity,
                relationship,
                object_entity,
            ));
        }

        writable_state.entities_relations_rows.insert(row);

        Ok(())
    }

    async fn delete_entities_relation(
        &self,
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> Result<(), DeleteEntitiesRelationError> {
        let mut writable_state = self.state.write().await;

        let row = EntitiesRelationsRow::new(subject_entity, relationship, object_entity);
        let not_found = !writable_state.entities_relations_rows.remove(&row);

        if not_found {
            return Err(DeleteEntitiesRelationError::not_found(
                subject_entity,
                relationship,
                object_entity,
            ));
        }

        Ok(())
    }

    async fn get_subject_entity_relations(
        &self,
        subject_entity: &Entity,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsError> {
        let res = self
            .get_rows(|row| {
                if row.subject_entity == *subject_entity {
                    Some(EntityWithRelation::new(
                        row.object_entity.clone(),
                        row.relationship,
                    ))
                } else {
                    None
                }
            })
            .await;

        Ok(res)
    }

    async fn get_subject_entity_relations_by_object_type(
        &self,
        subject_entity: &Entity,
        object_entity_type: EntityType,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsByObjectTypeError> {
        let res = self
            .get_rows(|row| {
                if row.subject_entity == *subject_entity
                    && row.object_entity.entity_type == object_entity_type
                {
                    Some(EntityWithRelation::new(
                        row.object_entity.clone(),
                        row.relationship,
                    ))
                } else {
                    None
                }
            })
            .await;

        Ok(res)
    }

    async fn get_relations_between_entities(
        &self,
        subject_entity: &Entity,
        object_entity: &Entity,
    ) -> Result<Vec<Relation>, GetRelationsBetweenEntitiesError> {
        let res = self
            .get_rows(|row| {
                if row.subject_entity == *subject_entity && row.object_entity == *object_entity {
                    Some(row.relationship)
                } else {
                    None
                }
            })
            .await;

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq, Hash)]
struct EntitiesRelationsRow {
    subject_entity: Entity<'static>,
    relationship: Relation,
    object_entity: Entity<'static>,
}

impl EntitiesRelationsRow {
    pub fn new(
        subject_entity: &Entity<'_>,
        relationship: Relation,
        object_entity: &Entity<'_>,
    ) -> Self {
        Self {
            subject_entity: subject_entity.clone().into_owned(),
            relationship,
            object_entity: object_entity.clone().into_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
