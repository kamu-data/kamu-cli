// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use internal_error::InternalError;
use reusable::{reusable, reuse};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EntityType {
    Dataset,
    Account,
}

#[reusable(entity)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Entity<'a> {
    pub entity_type: EntityType,
    pub entity_id: Cow<'a, str>,
}

impl<'a> Entity<'a> {
    pub fn new(entity_type: EntityType, entity_id: impl Into<Cow<'a, str>>) -> Self {
        Self {
            entity_type,
            entity_id: entity_id.into(),
        }
    }

    pub fn new_account(entity_id: impl Into<Cow<'a, str>>) -> Self {
        Self::new(EntityType::Account, entity_id)
    }

    pub fn new_dataset(entity_id: impl Into<Cow<'a, str>>) -> Self {
        Self::new(EntityType::Dataset, entity_id)
    }
}

impl<'a> From<EntityWithRelation<'a>> for Entity<'a> {
    fn from(v: EntityWithRelation<'a>) -> Self {
        Self {
            entity_type: v.entity_type,
            entity_id: v.entity_id,
        }
    }
}

#[reuse(entity)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntityWithRelation<'a> {
    pub relation: Relation,
}

impl<'a> EntityWithRelation<'a> {
    pub fn new(
        entity_type: EntityType,
        entity_id: impl Into<Cow<'a, str>>,
        relation: Relation,
    ) -> Self {
        Self {
            entity_type,
            entity_id: entity_id.into(),
            relation,
        }
    }

    pub fn new_account(entity_id: impl Into<Cow<'a, str>>, relation: Relation) -> Self {
        Self::new(EntityType::Account, entity_id, relation)
    }

    pub fn new_dataset(entity_id: impl Into<Cow<'a, str>>, relation: Relation) -> Self {
        Self::new(EntityType::Dataset, entity_id, relation)
    }
}

pub type ObjectEntity<'a> = Entity<'a>;
pub type ObjectEntityWithRelation<'a> = EntityWithRelation<'a>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum PropertyName {
    DatasetAllowsAnonymousRead,
    DatasetAllowsPublicRead,
}

pub type PropertyValue = String;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Property<'a> {
    pub name: PropertyName,
    pub value: Cow<'a, str>,
}

impl<'a> Property<'a> {
    pub fn new(name: PropertyName, value: impl Into<Cow<'a, str>>) -> Self {
        Self {
            name,
            value: value.into(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Relation {
    AccountDatasetReader,
    AccountDatasetEditor,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RebacRepository: Send + Sync {
    // Properties

    async fn set_entity_property(
        &self,
        entity: &Entity,
        property: &Property,
    ) -> Result<(), SetEntityPropertyError>;

    async fn delete_entity_property(
        &self,
        entity: &Entity,
        property_name: PropertyName,
    ) -> Result<(), DeleteEntityPropertyError>;

    async fn get_entity_properties(
        &self,
        entity: &Entity,
    ) -> Result<Vec<Property>, GetEntityPropertiesError>;

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
    ) -> Result<Vec<ObjectEntityWithRelation>, SubjectEntityRelationsError>;

    async fn get_subject_entity_relations_by_object_type(
        &self,
        subject_entity: &Entity,
        object_entity_type: EntityType,
    ) -> Result<Vec<ObjectEntityWithRelation>, SubjectEntityRelationsByObjectTypeError>;

    async fn get_relations_between_entities(
        &self,
        subject_entity: &Entity,
        object_entity: &Entity,
    ) -> Result<Vec<Relation>, GetRelationsBetweenEntitiesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SetEntityPropertyError {
    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteEntityPropertyError {
    #[error(transparent)]
    EntityNotFound(EntityNotFoundError),

    #[error(transparent)]
    PropertyNotFound(EntityPropertyNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

impl DeleteEntityPropertyError {
    pub fn entity_not_found(entity: &Entity) -> Self {
        Self::EntityNotFound(entity.into())
    }

    pub fn property_not_found(entity: &Entity, property_name: PropertyName) -> Self {
        Self::PropertyNotFound(EntityPropertyNotFoundError {
            entity_type: entity.entity_type,
            entity_id: entity.entity_id.to_string(),
            property_name,
        })
    }
}

#[derive(Error, Debug)]
#[error("Entity not found: entity_type='{entity_type:?}', entity_id='{entity_id}'")]
pub struct EntityNotFoundError {
    pub entity_type: EntityType,
    pub entity_id: String,
}

impl From<&Entity<'_>> for EntityNotFoundError {
    fn from(entity: &Entity<'_>) -> Self {
        Self {
            entity_type: entity.entity_type,
            entity_id: entity.entity_id.to_string(),
        }
    }
}

#[derive(Error, Debug)]
#[error(
    "Entity property not found: entity_type='{entity_type:?}', entity_id='{entity_id}', \
     property_name='{property_name:?}'"
)]
pub struct EntityPropertyNotFoundError {
    pub entity_type: EntityType,
    pub entity_id: String,
    pub property_name: PropertyName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetEntityPropertiesError {
    #[error(transparent)]
    NotFound(EntityNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

impl GetEntityPropertiesError {
    pub fn entity_not_found(entity: &Entity<'_>) -> Self {
        Self::NotFound(entity.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum InsertEntitiesRelationError {
    #[error(transparent)]
    Duplicate(InsertEntitiesRelationDuplicateError),

    #[error(transparent)]
    Internal(InternalError),
}

impl InsertEntitiesRelationError {
    pub fn duplicate(
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> Self {
        Self::Duplicate(InsertEntitiesRelationDuplicateError {
            subject_entity_type: subject_entity.entity_type,
            subject_entity_id: subject_entity.entity_id.to_string(),
            relationship,
            object_entity_type: object_entity.entity_type,
            object_entity_id: object_entity.entity_id.to_string(),
        })
    }
}

#[derive(Error, Debug)]
#[error(
    "Duplicate entity relation not inserted: subject_entity_type='{subject_entity_type:?}', \
     subject_entity_id='{subject_entity_id}', relationship='{relationship:?}', \
     object_entity_type='{object_entity_type:?}', object_entity_id='{object_entity_id}'"
)]
pub struct InsertEntitiesRelationDuplicateError {
    pub subject_entity_type: EntityType,
    pub subject_entity_id: String,
    pub relationship: Relation,
    pub object_entity_type: EntityType,
    pub object_entity_id: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteEntitiesRelationError {
    #[error(transparent)]
    NotFound(EntitiesRelationNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

impl DeleteEntitiesRelationError {
    pub fn not_found(
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> Self {
        Self::NotFound(EntitiesRelationNotFoundError {
            subject_entity_type: subject_entity.entity_type,
            subject_entity_id: subject_entity.entity_id.to_string(),
            relationship,
            object_entity_type: object_entity.entity_type,
            object_entity_id: object_entity.entity_id.to_string(),
        })
    }
}

#[derive(Error, Debug)]
#[error(
    "Entities relation not found: subject_entity_type='{subject_entity_type:?}', \
     subject_entity_id='{subject_entity_id}', relationship='{relationship:?}', \
     object_entity_type='{object_entity_type:?}', object_entity_id='{object_entity_id}'"
)]
pub struct EntitiesRelationNotFoundError {
    pub subject_entity_type: EntityType,
    pub subject_entity_id: String,
    pub relationship: Relation,
    pub object_entity_type: EntityType,
    pub object_entity_id: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SubjectEntityRelationsError {
    #[error(transparent)]
    NotFound(ObjectEntitiesRelationsNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

impl SubjectEntityRelationsError {
    pub fn not_found(subject_entity: &Entity) -> Self {
        Self::NotFound(ObjectEntitiesRelationsNotFoundError {
            subject_entity_type: subject_entity.entity_type,
            subject_entity_id: subject_entity.entity_id.to_string(),
        })
    }
}

#[derive(Error, Debug)]
#[error(
    "Object entities relations not found: subject_entity_type='{subject_entity_type:?}', \
     subject_entity_id='{subject_entity_id}'"
)]
pub struct ObjectEntitiesRelationsNotFoundError {
    pub subject_entity_type: EntityType,
    pub subject_entity_id: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SubjectEntityRelationsByObjectTypeError {
    #[error(transparent)]
    NotFound(ObjectEntitiesRelationsByObjectTypeNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

impl SubjectEntityRelationsByObjectTypeError {
    pub fn not_found(subject_entity: &Entity, object_entity_type: EntityType) -> Self {
        Self::NotFound(ObjectEntitiesRelationsByObjectTypeNotFoundError {
            subject_entity_type: subject_entity.entity_type,
            subject_entity_id: subject_entity.entity_id.to_string(),
            object_entity_type,
        })
    }
}

#[derive(Error, Debug)]
#[error(
    "Object entities relations by object type not found: \
     subject_entity_type='{subject_entity_type:?}', subject_entity_id='{subject_entity_id}', \
     object_entity_type='{object_entity_type:?}'"
)]
pub struct ObjectEntitiesRelationsByObjectTypeNotFoundError {
    pub subject_entity_type: EntityType,
    pub subject_entity_id: String,
    pub object_entity_type: EntityType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetRelationsBetweenEntitiesError {
    #[error(transparent)]
    NotFound(RelationsBetweenEntitiesNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

impl GetRelationsBetweenEntitiesError {
    pub fn not_found(subject_entity: &Entity, object_entity: &Entity) -> Self {
        Self::NotFound(RelationsBetweenEntitiesNotFoundError {
            subject_entity_type: subject_entity.entity_type,
            subject_entity_id: subject_entity.entity_id.to_string(),
            object_entity_type: object_entity.entity_type,
            object_entity_id: object_entity.entity_id.to_string(),
        })
    }
}

#[derive(Error, Debug)]
#[error(
    "Relations between entities not found: subject_entity_type='{subject_entity_type:?}', \
     subject_entity_id='{subject_entity_id}', object_entity_type='{object_entity_type:?}', \
     object_entity_id='{object_entity_id}'"
)]
pub struct RelationsBetweenEntitiesNotFoundError {
    pub subject_entity_type: EntityType,
    pub subject_entity_id: String,
    pub object_entity_type: EntityType,
    pub object_entity_id: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
