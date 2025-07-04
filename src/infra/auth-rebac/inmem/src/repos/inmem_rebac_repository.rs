// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dill::{Singleton, component, interface, scope};
use kamu_auth_rebac::*;
use tokio::sync::RwLock;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type EntityProperties = HashMap<PropertyName, PropertyValue<'static>>;
type EntitiesPropertiesMap = HashMap<Entity<'static>, EntityProperties>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    entities_properties_map: EntitiesPropertiesMap,
    entities_relations_rows: HashSet<EntitiesWithRelation<'static>>,
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
        F: Fn(&EntitiesWithRelation) -> Option<R>,
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
        }

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

    async fn get_entities_properties(
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

    async fn upsert_entities_relations(
        &self,
        operations: &[UpsertEntitiesRelationOperation<'_>],
    ) -> Result<(), UpsertEntitiesRelationsError> {
        if operations.is_empty() {
            return Ok(());
        }

        let mut writable_state = self.state.write().await;

        for op in operations {
            let maybe_existing_row = writable_state
                .entities_relations_rows
                .iter()
                .find(|row| {
                    row.subject_entity == *op.subject_entity
                        && row.object_entity == *op.object_entity
                })
                .cloned();
            if let Some(existing_row) = maybe_existing_row {
                writable_state.entities_relations_rows.remove(&existing_row);
            }

            let new_row = EntitiesWithRelation {
                subject_entity: op.subject_entity.as_ref().clone().into_owned(),
                relation: op.relationship,
                object_entity: op.object_entity.as_ref().clone().into_owned(),
            };

            writable_state.entities_relations_rows.insert(new_row);
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
                        row.object_entity.clone().into_owned(),
                        row.relation,
                    ))
                } else {
                    None
                }
            })
            .await;

        Ok(res)
    }

    async fn get_object_entity_relations(
        &self,
        object_entity: &Entity,
    ) -> Result<Vec<EntityWithRelation>, GetObjectEntityRelationsError> {
        let res = self
            .get_rows(|row| {
                if row.object_entity == *object_entity {
                    Some(EntityWithRelation::new(
                        row.subject_entity.clone().into_owned(),
                        row.relation,
                    ))
                } else {
                    None
                }
            })
            .await;

        Ok(res)
    }

    async fn get_object_entities_relations(
        &self,
        object_entities: &[Entity],
    ) -> Result<Vec<EntitiesWithRelation>, GetObjectEntityRelationsError> {
        let object_entities_set = object_entities.iter().collect::<HashSet<_>>();

        let res = self
            .get_rows(|row| {
                if object_entities_set.contains(&row.object_entity) {
                    Some(row.clone_owned())
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
                        row.object_entity.clone().into_owned(),
                        row.relation,
                    ))
                } else {
                    None
                }
            })
            .await;

        Ok(res)
    }

    async fn get_relation_between_entities(
        &self,
        subject_entity: &Entity,
        object_entity: &Entity,
    ) -> Result<Relation, GetRelationBetweenEntitiesError> {
        let readable_state = self.state.read().await;

        let maybe_row = readable_state
            .entities_relations_rows
            .iter()
            .find(|row| {
                row.subject_entity == *subject_entity && row.object_entity == *object_entity
            })
            .cloned();

        if let Some(row) = maybe_row {
            Ok(row.relation)
        } else {
            Err(GetRelationBetweenEntitiesError::not_found(
                subject_entity,
                object_entity,
            ))
        }
    }

    async fn delete_subject_entities_object_entity_relations(
        &self,
        subject_entities: Vec<Entity<'static>>,
        object_entity: &Entity,
    ) -> Result<(), DeleteSubjectEntitiesObjectEntityRelationsError> {
        if subject_entities.is_empty() {
            return Ok(());
        }

        let subject_entities_ids = subject_entities.iter().collect::<HashSet<_>>();

        let mut writable_state = self.state.write().await;

        let mut count_deleted = 0;
        let mut rows_after_deletion = HashSet::new();

        for row in writable_state.entities_relations_rows.drain() {
            if row.object_entity == *object_entity
                && subject_entities_ids.contains(&row.subject_entity)
            {
                count_deleted += 1;
                continue;
            }

            rows_after_deletion.insert(row);
        }

        writable_state.entities_relations_rows = rows_after_deletion;

        if count_deleted == 0 {
            return Err(DeleteSubjectEntitiesObjectEntityRelationsError::not_found(
                subject_entities,
                object_entity,
            ));
        }

        Ok(())
    }

    async fn delete_entities_relations(
        &self,
        operations: &[DeleteEntitiesRelationOperation<'_>],
    ) -> Result<(), DeleteEntitiesRelationsError> {
        if operations.is_empty() {
            return Ok(());
        }

        let mut writable_state = self.state.write().await;

        for op in operations {
            let maybe_existing_row = writable_state
                .entities_relations_rows
                .iter()
                .find(|row| {
                    row.subject_entity == *op.subject_entity
                        && row.object_entity == *op.object_entity
                })
                .cloned();

            if let Some(existing_row) = maybe_existing_row {
                writable_state.entities_relations_rows.remove(&existing_row);
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
