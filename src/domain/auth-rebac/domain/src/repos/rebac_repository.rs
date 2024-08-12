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
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RebacRepository: Send + Sync {
    // Properties

    async fn set_entity_property(
        &self,
        entity: &Entity,
        property: PropertyName,
        property_value: &PropertyValue,
    ) -> Result<(), SetEntityPropertyError>;

    async fn delete_entity_property(
        &self,
        entity: &Entity,
        property_name: PropertyName,
    ) -> Result<(), DeleteEntityPropertyError>;

    async fn get_entity_properties(
        &self,
        entity: &Entity,
    ) -> Result<Vec<(PropertyName, PropertyValue)>, GetEntityPropertiesError>;

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
// DTOs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EntityType {
    Dataset,
    Account,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

    pub fn into_owned(self) -> Entity<'static> {
        Entity {
            entity_type: self.entity_type,
            entity_id: Cow::Owned(self.entity_id.into_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntityWithRelation<'a> {
    pub entity: Entity<'a>,
    pub relation: Relation,
}

impl<'a> EntityWithRelation<'a> {
    pub fn new(entity: Entity<'a>, relation: Relation) -> Self {
        Self { entity, relation }
    }

    pub fn new_account(entity_id: impl Into<Cow<'a, str>>, relation: Relation) -> Self {
        let account_entity = Entity::new_account(entity_id);

        Self::new(account_entity, relation)
    }

    pub fn new_dataset(entity_id: impl Into<Cow<'a, str>>, relation: Relation) -> Self {
        let dataset_entity = Entity::new_dataset(entity_id);

        Self::new(dataset_entity, relation)
    }
}

pub type ObjectEntity<'a> = Entity<'a>;
pub type ObjectEntityWithRelation<'a> = EntityWithRelation<'a>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type PropertyValue<'a> = Cow<'a, str>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PropertyName {
    Dataset(DatasetPropertyName),
    Account(AccountPropertyName),
}

impl PropertyName {
    pub fn dataset_allows_anonymous_read<'a>(allows: bool) -> (Self, PropertyValue<'a>) {
        let value = if allows { "true" } else { "false" };

        (
            Self::Dataset(DatasetPropertyName::AllowsAnonymousRead),
            value.into(),
        )
    }

    pub fn dataset_allows_public_read<'a>(allows: bool) -> (Self, PropertyValue<'a>) {
        let value = if allows { "true" } else { "false" };

        (
            Self::Dataset(DatasetPropertyName::AllowsPublicRead),
            value.into(),
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DatasetPropertyName {
    AllowsAnonymousRead,
    AllowsPublicRead,
}

impl From<DatasetPropertyName> for PropertyName {
    fn from(value: DatasetPropertyName) -> PropertyName {
        PropertyName::Dataset(value)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AccountPropertyName {
    // TBA
}

impl From<AccountPropertyName> for PropertyName {
    fn from(value: AccountPropertyName) -> PropertyName {
        PropertyName::Account(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Relation {
    AccountDatasetReader,
    AccountDatasetEditor,
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
        Self::EntityNotFound(EntityNotFoundError {
            entity: entity.clone().into_owned(),
        })
    }

    pub fn property_not_found(entity: &Entity, property_name: PropertyName) -> Self {
        Self::PropertyNotFound(EntityPropertyNotFoundError {
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
pub enum GetEntityPropertiesError {
    #[error(transparent)]
    NotFound(EntityNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

impl GetEntityPropertiesError {
    pub fn entity_not_found(entity: &Entity<'_>) -> Self {
        Self::NotFound(EntityNotFoundError {
            entity: entity.clone().into_owned(),
        })
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
    Internal(InternalError),
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
    NotFound(ObjectEntitiesRelationsNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

impl SubjectEntityRelationsError {
    pub fn not_found(subject_entity: &Entity) -> Self {
        Self::NotFound(ObjectEntitiesRelationsNotFoundError {
            subject_entity: subject_entity.clone().into_owned(),
        })
    }
}

#[derive(Error, Debug)]
#[error("Object entities relations not found: subject_entity='{subject_entity:?}' ")]
pub struct ObjectEntitiesRelationsNotFoundError {
    pub subject_entity: Entity<'static>,
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
            subject_entity: subject_entity.clone().into_owned(),
            object_entity_type,
        })
    }
}

#[derive(Error, Debug)]
#[error(
    "Object entities relations by object type not found: subject_entity='{subject_entity:?}', \
     object_entity_type='{object_entity_type:?}'"
)]
pub struct ObjectEntitiesRelationsByObjectTypeNotFoundError {
    pub subject_entity: Entity<'static>,
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
            subject_entity: subject_entity.clone().into_owned(),
            object_entity: object_entity.clone().into_owned(),
        })
    }
}

#[derive(Error, Debug)]
#[error(
    "Relations between entities not found: subject_entity='{subject_entity:?}', \
     object_entity='{object_entity:?}'"
)]
pub struct RelationsBetweenEntitiesNotFoundError {
    pub subject_entity: Entity<'static>,
    pub object_entity: Entity<'static>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
