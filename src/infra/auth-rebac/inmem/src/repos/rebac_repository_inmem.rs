// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use dill::{component, interface, scope, Singleton};
use kamu_auth_rebac::{
    DeleteEntitiesRelationError,
    DeleteEntityPropertyError,
    Entity,
    EntityType,
    GetEntityPropertiesError,
    GetRelationsBetweenEntitiesError,
    InsertEntitiesRelationError,
    ObjectEntityWithRelation,
    Property,
    PropertyName,
    PropertyValue,
    RebacRepository,
    Relation,
    SetEntityPropertyError,
    SubjectEntityRelationsByObjectTypeError,
    SubjectEntityRelationsError,
};
use tokio::sync::{RwLock, RwLockReadGuard};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type EntityHash = u64;
type EntityProperties = HashMap<PropertyName, PropertyValue>;

type EntitiesPropertiesMap = HashMap<EntityHash, EntityProperties>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type IndexEntitiesRelationsRowHash = u64;
type RowId = u64; /* hash */

type RowsIndex = HashMap<IndexEntitiesRelationsRowHash, HashSet<RowId>>;

// From a usability point of view, we would prefer to store self-calculated
// hashes rather than fight with the borrow checker during using indexes.
#[derive(Default)]
struct EntitiesRelationsState {
    rows: HashMap<RowId, EntitiesRelationsRow>,
    index_subject_entity: RowsIndex,
    index_subject_entity_object_type: RowsIndex,
    index_subject_entity_object_entity: RowsIndex,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RebacRepositoryInMem {
    entities_properties_map: Arc<RwLock<EntitiesPropertiesMap>>,
    entities_relations_state: Arc<RwLock<EntitiesRelationsState>>,
}

#[component(pub)]
#[interface(dyn RebacRepository)]
#[scope(Singleton)]
impl RebacRepositoryInMem {
    pub fn new() -> Self {
        Self {
            entities_properties_map: Arc::new(RwLock::new(EntitiesPropertiesMap::new())),
            entities_relations_state: Arc::new(RwLock::new(EntitiesRelationsState::default())),
        }
    }

    fn get_entities_relations_by_row_ids<'a>(
        &self,
        row_ids: &'a HashSet<RowId>,
        rw_lock_read_guard: &'a RwLockReadGuard<'a, EntitiesRelationsState>,
    ) -> Vec<&'a EntitiesRelationsRow> {
        let mut rows = Vec::with_capacity(row_ids.len());

        for row_id in row_ids {
            let Some(row) = rw_lock_read_guard.rows.get(row_id) else {
                unreachable!()
            };

            rows.push(row);
        }

        rows
    }
}

#[async_trait::async_trait]
impl RebacRepository for RebacRepositoryInMem {
    async fn set_entity_property(
        &self,
        entity: &Entity,
        property: &Property,
    ) -> Result<(), SetEntityPropertyError> {
        let mut entities_properties_map = self.entities_properties_map.write().await;

        let entity_hash = EntityHasher::entity_hash(entity);
        let entity_properties = entities_properties_map
            .entry(entity_hash)
            .or_insert_with(EntityProperties::new);

        entity_properties
            .entry(property.name)
            .and_modify(|property_value| *property_value = property.value.to_string())
            .or_insert_with(|| property.value.to_string());

        Ok(())
    }

    async fn delete_entity_property(
        &self,
        entity: &Entity,
        property_name: PropertyName,
    ) -> Result<(), DeleteEntityPropertyError> {
        let mut entities_properties_map = self.entities_properties_map.write().await;

        let entity_hash = EntityHasher::entity_hash(entity);
        let maybe_entity_properties = entities_properties_map.get_mut(&entity_hash);

        let Some(entity_properties) = maybe_entity_properties else {
            return Err(DeleteEntityPropertyError::entity_not_found(entity));
        };

        let property_not_found = entity_properties.remove(&property_name).is_none();

        if property_not_found {
            return Err(DeleteEntityPropertyError::property_not_found(
                entity,
                property_name,
            ));
        }

        Ok(())
    }

    async fn get_entity_properties(
        &self,
        entity: &Entity,
    ) -> Result<Vec<Property>, GetEntityPropertiesError> {
        let entities_properties_map = self.entities_properties_map.read().await;

        let entity_hash = EntityHasher::entity_hash(entity);
        let maybe_entity_properties = entities_properties_map.get(&entity_hash);

        let Some(entity_properties) = maybe_entity_properties else {
            return Err(GetEntityPropertiesError::entity_not_found(entity));
        };

        let properties = entity_properties
            .iter()
            .map(|(name, value)| Property::new(*name, value.to_owned()))
            .collect::<Vec<_>>();

        Ok(properties)
    }

    async fn insert_entities_relation(
        &self,
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> Result<(), InsertEntitiesRelationError> {
        let row_id =
            EntityHasher::entities_relations_row_hash(subject_entity, relationship, object_entity);

        let mut state = self.entities_relations_state.write().await;

        let is_duplicate = state
            .index_subject_entity_object_entity
            .contains_key(&row_id);

        if is_duplicate {
            return Err(InsertEntitiesRelationError::duplicate(
                subject_entity,
                relationship,
                object_entity,
            ));
        }

        // Save a row

        let row =
            EntitiesRelationsRow::new(subject_entity.clone(), relationship, object_entity.clone());

        state.rows.insert(row_id, row);

        // Update indexes

        {
            let index_hash = EntityHasher::subject_entity_index_hash(subject_entity);

            state
                .index_subject_entity
                .entry(index_hash)
                .or_insert_with(HashSet::new)
                .insert(row_id);
        }
        {
            let index_hash = EntityHasher::subject_entity_object_type_index_hash(
                subject_entity,
                object_entity.entity_type,
            );

            state
                .index_subject_entity_object_type
                .entry(index_hash)
                .or_insert_with(HashSet::new)
                .insert(row_id);
        }
        {
            let index_hash = EntityHasher::subject_entity_object_entity_index_hash(
                subject_entity,
                object_entity,
            );

            state
                .index_subject_entity_object_entity
                .entry(index_hash)
                .or_insert_with(HashSet::new)
                .insert(row_id);
        }

        Ok(())
    }

    async fn delete_entities_relation(
        &self,
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> Result<(), DeleteEntitiesRelationError> {
        let row_id =
            EntityHasher::entities_relations_row_hash(subject_entity, relationship, object_entity);

        let mut state = self.entities_relations_state.write().await;

        let not_found = state.rows.remove(&row_id).is_none();

        if not_found {
            return Err(DeleteEntitiesRelationError::not_found(
                subject_entity,
                relationship,
                object_entity,
            ));
        }

        state.index_subject_entity.remove(&row_id);
        state.index_subject_entity_object_type.remove(&row_id);
        state.index_subject_entity_object_entity.remove(&row_id);

        Ok(())
    }

    async fn get_subject_entity_relations(
        &self,
        subject_entity: &Entity,
    ) -> Result<Vec<ObjectEntityWithRelation>, SubjectEntityRelationsError> {
        let index_hash = EntityHasher::subject_entity_index_hash(subject_entity);

        let state = self.entities_relations_state.read().await;

        let maybe_row_ids = state.index_subject_entity.get(&index_hash);

        let Some(row_ids) = maybe_row_ids else {
            return Err(SubjectEntityRelationsError::not_found(subject_entity));
        };

        let rows = self.get_entities_relations_by_row_ids(row_ids, &state);
        let res = rows
            .into_iter()
            .map(|row| ObjectEntityWithRelation {
                entity_type: row.object_entity.entity_type,
                entity_id: row.object_entity.entity_id.clone().into(),
                relation: row.relationship,
            })
            .collect();

        Ok(res)
    }

    async fn get_subject_entity_relations_by_object_type(
        &self,
        subject_entity: &Entity,
        object_entity_type: EntityType,
    ) -> Result<Vec<ObjectEntityWithRelation>, SubjectEntityRelationsByObjectTypeError> {
        let index_hash =
            EntityHasher::subject_entity_object_type_index_hash(subject_entity, object_entity_type);

        let state = self.entities_relations_state.read().await;

        let maybe_row_ids = state.index_subject_entity_object_type.get(&index_hash);

        let Some(row_ids) = maybe_row_ids else {
            return Err(SubjectEntityRelationsByObjectTypeError::not_found(
                subject_entity,
                object_entity_type,
            ));
        };

        let rows = self.get_entities_relations_by_row_ids(row_ids, &state);
        let res = rows
            .into_iter()
            .map(|row| ObjectEntityWithRelation {
                entity_type: row.object_entity.entity_type,
                entity_id: row.object_entity.entity_id.clone().into(),
                relation: row.relationship,
            })
            .collect();

        Ok(res)
    }

    async fn get_relations_between_entities(
        &self,
        subject_entity: &Entity,
        object_entity: &Entity,
    ) -> Result<Vec<Relation>, GetRelationsBetweenEntitiesError> {
        let index_hash =
            EntityHasher::subject_entity_object_entity_index_hash(subject_entity, object_entity);

        let state = self.entities_relations_state.read().await;

        let maybe_row_ids = state.index_subject_entity_object_entity.get(&index_hash);

        let Some(row_ids) = maybe_row_ids else {
            return Err(GetRelationsBetweenEntitiesError::not_found(
                subject_entity,
                object_entity,
            ));
        };

        let rows = self.get_entities_relations_by_row_ids(row_ids, &state);
        let res = rows.into_iter().map(|row| row.relationship).collect();

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct EntityRow {
    pub entity_type: EntityType,
    pub entity_id: String,
}

impl<'a> From<Entity<'a>> for EntityRow {
    fn from(value: Entity<'a>) -> Self {
        Self {
            entity_type: value.entity_type,
            entity_id: value.entity_id.to_string(),
        }
    }
}

struct EntitiesRelationsRow {
    #[allow(dead_code)]
    subject_entity: EntityRow,
    relationship: Relation,
    object_entity: EntityRow,
}

impl EntitiesRelationsRow {
    pub fn new(
        subject_entity: Entity<'_>,
        relationship: Relation,
        object_entity: Entity<'_>,
    ) -> Self {
        Self {
            subject_entity: subject_entity.into(),
            relationship,
            object_entity: object_entity.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EntityHasher
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct EntityHasher {}

impl EntityHasher {
    pub fn entity_hash(entity: &Entity) -> EntityHash {
        let mut hasher = DefaultHasher::new();

        entity.hash(&mut hasher);

        hasher.finish()
    }

    pub fn entities_relations_row_hash(
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> RowId {
        let mut hasher = DefaultHasher::new();

        subject_entity.hash(&mut hasher);
        relationship.hash(&mut hasher);
        object_entity.hash(&mut hasher);

        hasher.finish()
    }

    pub fn subject_entity_index_hash(subject_entity: &Entity) -> IndexEntitiesRelationsRowHash {
        let mut hasher = DefaultHasher::new();

        subject_entity.hash(&mut hasher);

        hasher.finish()
    }

    pub fn subject_entity_object_type_index_hash(
        subject_entity: &Entity,
        object_entity_type: EntityType,
    ) -> IndexEntitiesRelationsRowHash {
        let mut hasher = DefaultHasher::new();

        subject_entity.hash(&mut hasher);
        object_entity_type.hash(&mut hasher);

        hasher.finish()
    }

    pub fn subject_entity_object_entity_index_hash(
        subject_entity: &Entity,
        object_entity: &Entity,
    ) -> IndexEntitiesRelationsRowHash {
        let mut hasher = DefaultHasher::new();

        subject_entity.hash(&mut hasher);
        object_entity.hash(&mut hasher);

        hasher.finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
