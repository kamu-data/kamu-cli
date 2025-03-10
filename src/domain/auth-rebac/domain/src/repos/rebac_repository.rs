// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::{Entity, EntityType, EntityWithRelation, PropertyName, PropertyValue, Relation};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RebacRepository: Send + Sync {
    // Properties
    async fn properties_count(&self) -> Result<usize, PropertiesCountError>;

    async fn set_entity_property(
        &self,
        entity: &Entity,
        property_name: PropertyName,
        property_value: &PropertyValue,
    ) -> Result<(), SetEntityPropertyError>;

    async fn delete_entity_property(
        &self,
        entity: &Entity,
        property_name: PropertyName,
    ) -> Result<(), DeleteEntityPropertyError>;

    async fn delete_entity_properties(
        &self,
        entity: &Entity,
    ) -> Result<(), DeleteEntityPropertiesError>;

    async fn get_entity_properties(
        &self,
        entity: &Entity,
    ) -> Result<Vec<(PropertyName, PropertyValue)>, GetEntityPropertiesError>;

    async fn get_entities_properties(
        &self,
        entities: &[Entity],
    ) -> Result<Vec<(Entity, PropertyName, PropertyValue)>, GetEntityPropertiesError>;

    // Relations

    async fn insert_entities_relation(
        &self,
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> Result<(), InsertEntitiesRelationError>;

    async fn delete_entities_relation(
        &self,
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> Result<(), DeleteEntitiesRelationError>;

    async fn get_subject_entity_relations(
        &self,
        subject_entity: &Entity,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsError>;

    // TODO: Private Datasets: tests
    async fn get_object_entity_relations(
        &self,
        object_entity: &Entity,
    ) -> Result<Vec<EntityWithRelation>, ObjectEntityRelationsError>;

    async fn get_subject_entity_relations_by_object_type(
        &self,
        subject_entity: &Entity,
        object_entity_type: EntityType,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsByObjectTypeError>;

    async fn get_relations_between_entities(
        &self,
        subject_entity: &Entity,
        object_entity: &Entity,
    ) -> Result<Vec<Relation>, GetRelationsBetweenEntitiesError>;

    async fn delete_subject_entities_object_entity_relations(
        &self,
        subject_entities: Vec<Entity<'static>>,
        object_entity: &Entity,
    ) -> Result<(), DeleteSubjectEntitiesObjectEntityRelationsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum PropertiesCountError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SetEntityPropertyError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteEntityPropertyError {
    #[error(transparent)]
    NotFound(EntityPropertyNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl DeleteEntityPropertyError {
    pub fn not_found(entity: &Entity, property_name: PropertyName) -> Self {
        Self::NotFound(EntityPropertyNotFoundError {
            entity: entity.clone().into_owned(),
            property_name,
        })
    }
}

#[derive(Error, Debug)]
#[error("Entity not found: {entity:?}")]
pub struct EntityNotFoundError {
    pub entity: Entity<'static>,
}

#[derive(Error, Debug)]
#[error("Entity property not found: {entity:?}, property_name='{property_name:?}'")]
pub struct EntityPropertyNotFoundError {
    pub entity: Entity<'static>,
    pub property_name: PropertyName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteEntityPropertiesError {
    #[error(transparent)]
    NotFound(EntityNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl DeleteEntityPropertiesError {
    pub fn not_found(entity: &Entity) -> Self {
        Self::NotFound(EntityNotFoundError {
            entity: entity.clone().into_owned(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetEntityPropertiesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum InsertEntitiesRelationError {
    #[error(transparent)]
    Duplicate(InsertEntitiesRelationDuplicateError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl InsertEntitiesRelationError {
    pub fn duplicate(
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> Self {
        Self::Duplicate(InsertEntitiesRelationDuplicateError {
            subject_entity: subject_entity.clone().into_owned(),
            relationship,
            object_entity: object_entity.clone().into_owned(),
        })
    }
}

#[derive(Error, Debug)]
#[error(
    "Duplicate entity relation not inserted: subject_entity='{subject_entity:?}', \
     relationship='{relationship:?}', object_entity='{object_entity:?}'"
)]
pub struct InsertEntitiesRelationDuplicateError {
    pub subject_entity: Entity<'static>,
    pub relationship: Relation,
    pub object_entity: Entity<'static>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteEntitiesRelationError {
    #[error(transparent)]
    NotFound(EntitiesRelationNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl DeleteEntitiesRelationError {
    pub fn not_found(
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> Self {
        Self::NotFound(EntitiesRelationNotFoundError {
            subject_entity: subject_entity.clone().into_owned(),
            relationship,
            object_entity: object_entity.clone().into_owned(),
        })
    }
}

#[derive(Error, Debug)]
#[error(
    "Entities relation not found: subject_entity='{subject_entity:?}', \
     relationship='{relationship:?}', object_entity='{object_entity:?}'"
)]
pub struct EntitiesRelationNotFoundError {
    pub subject_entity: Entity<'static>,
    pub relationship: Relation,
    pub object_entity: Entity<'static>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SubjectEntityRelationsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ObjectEntityRelationsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SubjectEntityRelationsByObjectTypeError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetRelationsBetweenEntitiesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteSubjectEntitiesObjectEntityRelationsError {
    #[error(transparent)]
    NotFound(#[from] SubjectEntitiesObjectEntityRelationsNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl DeleteSubjectEntitiesObjectEntityRelationsError {
    pub fn not_found(subject_entities: Vec<Entity<'static>>, object_entity: &Entity) -> Self {
        Self::NotFound(SubjectEntitiesObjectEntityRelationsNotFoundError {
            subject_entities,
            object_entity: object_entity.clone().into_owned(),
        })
    }
}

#[derive(Error, Debug)]
#[error(
    "Entities relations not found: subject_entities='{subject_entities:?}', \
     object_entity='{object_entity:?}'"
)]
pub struct SubjectEntitiesObjectEntityRelationsNotFoundError {
    pub subject_entities: Vec<Entity<'static>>,
    pub object_entity: Entity<'static>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
