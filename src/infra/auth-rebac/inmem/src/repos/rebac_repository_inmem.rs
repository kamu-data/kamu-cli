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

// TODO: remove
type EntityHash = u64;
type EntityProperties = HashMap<PropertyName, PropertyValue<'static>>;

type EntitiesPropertiesMap = HashMap<Entity<'static>, EntityProperties>;

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

#[derive(Default)]
struct State {
    entities_properties_map: EntitiesPropertiesMap,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RebacRepositoryInMem {
    state: Arc<RwLock<State>>,
    // TODO: absorb into state
    entities_relations_state: Arc<RwLock<EntitiesRelationsState>>,
}

#[component(pub)]
#[interface(dyn RebacRepository)]
#[scope(Singleton)]
impl RebacRepositoryInMem {
    pub fn new() -> Self {
        Self {
            state: Default::default(),
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

    fn remove_row_id_from_index(
        &self,
        row_id: RowId,
        index: &mut RowsIndex,
        index_hash: IndexEntitiesRelationsRowHash,
    ) {
        let row_ids = index.get_mut(&index_hash).unwrap();

        assert!(row_ids.remove(&row_id));

        if row_ids.is_empty() {
            index.remove(&index_hash);
        }
    }
}

#[async_trait::async_trait]
impl RebacRepository for RebacRepositoryInMem {
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
    ) -> Result<Vec<(PropertyName, PropertyValue)>, GetEntityPropertiesError> {
        let readable_state = self.state.read().await;

        let maybe_entity_properties = readable_state.entities_properties_map.get(entity);

        let Some(entity_properties) = maybe_entity_properties else {
            return Err(GetEntityPropertiesError::entity_not_found(entity));
        };

        let properties = entity_properties
            .iter()
            .map(|(name, value)| (*name, value.clone()))
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

        let mut writable_state = self.entities_relations_state.write().await;

        let is_duplicate = writable_state.rows.contains_key(&row_id);

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

        writable_state.rows.insert(row_id, row);

        // Update indexes

        {
            let index_hash = EntityHasher::subject_entity_index_hash(subject_entity);

            writable_state
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

            writable_state
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

            writable_state
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

        let mut writable_state = self.entities_relations_state.write().await;

        let not_found = writable_state.rows.remove(&row_id).is_none();

        if not_found {
            return Err(DeleteEntitiesRelationError::not_found(
                subject_entity,
                relationship,
                object_entity,
            ));
        }

        {
            let index_hash = EntityHasher::subject_entity_index_hash(subject_entity);

            self.remove_row_id_from_index(
                row_id,
                &mut writable_state.index_subject_entity,
                index_hash,
            );
        }
        {
            let index_hash = EntityHasher::subject_entity_object_type_index_hash(
                subject_entity,
                object_entity.entity_type,
            );

            self.remove_row_id_from_index(
                row_id,
                &mut writable_state.index_subject_entity_object_type,
                index_hash,
            );
        }
        {
            let index_hash = EntityHasher::subject_entity_object_entity_index_hash(
                subject_entity,
                object_entity,
            );

            self.remove_row_id_from_index(
                row_id,
                &mut writable_state.index_subject_entity_object_entity,
                index_hash,
            );
        }

        Ok(())
    }

    async fn get_subject_entity_relations(
        &self,
        subject_entity: &Entity,
    ) -> Result<Vec<ObjectEntityWithRelation>, SubjectEntityRelationsError> {
        let index_hash = EntityHasher::subject_entity_index_hash(subject_entity);

        let readable_state = self.entities_relations_state.read().await;

        let maybe_row_ids = readable_state.index_subject_entity.get(&index_hash);

        let Some(row_ids) = maybe_row_ids else {
            return Err(SubjectEntityRelationsError::not_found(subject_entity));
        };

        let rows = self.get_entities_relations_by_row_ids(row_ids, &readable_state);
        let res = rows
            .into_iter()
            .map(|row| {
                let entity = Entity::new(
                    row.object_entity.entity_type,
                    row.object_entity.entity_id.clone(),
                );

                ObjectEntityWithRelation::new(entity, row.relationship)
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

        let readable_state = self.entities_relations_state.read().await;

        let maybe_row_ids = readable_state
            .index_subject_entity_object_type
            .get(&index_hash);

        let Some(row_ids) = maybe_row_ids else {
            return Err(SubjectEntityRelationsByObjectTypeError::not_found(
                subject_entity,
                object_entity_type,
            ));
        };

        let rows = self.get_entities_relations_by_row_ids(row_ids, &readable_state);
        let res = rows
            .into_iter()
            .map(|row| {
                let entity = Entity::new(
                    row.object_entity.entity_type,
                    row.object_entity.entity_id.clone(),
                );

                ObjectEntityWithRelation::new(entity, row.relationship)
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

        let readable_state = self.entities_relations_state.read().await;

        let maybe_row_ids = readable_state
            .index_subject_entity_object_entity
            .get(&index_hash);

        let Some(row_ids) = maybe_row_ids else {
            return Err(GetRelationsBetweenEntitiesError::not_found(
                subject_entity,
                object_entity,
            ));
        };

        let rows = self.get_entities_relations_by_row_ids(row_ids, &readable_state);
        let res = rows.into_iter().map(|row| row.relationship).collect();

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
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

#[derive(Debug)]
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

// TODO: remove
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
