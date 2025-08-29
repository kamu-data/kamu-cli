// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use internal_error::{ErrorIntoInternal, InternalError};
use thiserror::Error;

use crate::{
    EntitiesWithRelation,
    Entity,
    EntityType,
    EntityWithRelation,
    PropertyName,
    PropertyValue,
    Relation,
};

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
    async fn upsert_entities_relations(
        &self,
        operations: &[UpsertEntitiesRelationOperation<'_>],
    ) -> Result<(), UpsertEntitiesRelationsError>;

    async fn get_subject_entity_relations(
        &self,
        subject_entity: &Entity,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsError>;

    async fn get_object_entity_relations(
        &self,
        object_entity: &Entity,
    ) -> Result<Vec<EntityWithRelation>, GetObjectEntityRelationsError>;

    async fn get_object_entities_relations(
        &self,
        object_entities: &[Entity],
    ) -> Result<Vec<EntitiesWithRelation>, GetObjectEntityRelationsError>;

    async fn get_subject_entities_relations(
        &self,
        subject_entities: &[Entity],
    ) -> Result<Vec<EntitiesWithRelation>, GetObjectEntityRelationsError>;

    async fn get_subject_entity_relations_by_object_type(
        &self,
        subject_entity: &Entity,
        object_entity_type: EntityType,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsByObjectTypeError>;

    async fn get_relation_between_entities(
        &self,
        subject_entity: &Entity,
        object_entity: &Entity,
    ) -> Result<Relation, GetRelationBetweenEntitiesError>;

    async fn delete_subject_entities_object_entity_relations(
        &self,
        subject_entities: Vec<Entity<'static>>,
        object_entity: &Entity,
    ) -> Result<(), DeleteSubjectEntitiesObjectEntityRelationsError>;

    async fn delete_entities_relations(
        &self,
        operations: &[DeleteEntitiesRelationOperation<'_>],
    ) -> Result<(), DeleteEntitiesRelationsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct UpsertEntitiesRelationOperation<'a> {
    pub subject_entity: Cow<'a, Entity<'a>>,
    pub relationship: Relation,
    pub object_entity: Cow<'a, Entity<'a>>,
}

#[derive(Debug)]
pub struct DeleteEntitiesRelationOperation<'a> {
    pub subject_entity: Cow<'a, Entity<'a>>,
    pub object_entity: Cow<'a, Entity<'a>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RebacRepositoryExt: RebacRepository {
    async fn try_get_relation_between_entities(
        &self,
        subject_entity: &Entity,
        object_entity: &Entity,
    ) -> Result<Option<Relation>, InternalError>;
}

#[async_trait::async_trait]
impl<T> RebacRepositoryExt for T
where
    T: RebacRepository,
    T: ?Sized,
{
    async fn try_get_relation_between_entities(
        &self,
        subject_entity: &Entity,
        object_entity: &Entity,
    ) -> Result<Option<Relation>, InternalError> {
        match self
            .get_relation_between_entities(subject_entity, object_entity)
            .await
        {
            Ok(relation) => Ok(Some(relation)),
            Err(GetRelationBetweenEntitiesError::NotFound(_)) => Ok(None),
            Err(e) => Err(e.int_err()),
        }
    }
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
    SomeRoleIsAlreadyPresent(InsertEntitiesRelationSomeRoleIsAlreadyPresentError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl InsertEntitiesRelationError {
    pub fn some_role_is_already_present(subject_entity: &Entity, object_entity: &Entity) -> Self {
        Self::SomeRoleIsAlreadyPresent(InsertEntitiesRelationSomeRoleIsAlreadyPresentError {
            subject_entity: subject_entity.clone().into_owned(),
            object_entity: object_entity.clone().into_owned(),
        })
    }
}

#[derive(Error, Debug)]
#[error(
    "Some role is already present: subject_entity='{subject_entity:?}', \
     object_entity='{object_entity:?}'"
)]
pub struct InsertEntitiesRelationSomeRoleIsAlreadyPresentError {
    pub subject_entity: Entity<'static>,
    pub object_entity: Entity<'static>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UpsertEntitiesRelationsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
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
    pub fn not_found(subject_entity: &Entity, object_entity: &Entity) -> Self {
        Self::NotFound(EntitiesRelationNotFoundError {
            subject_entity: subject_entity.clone().into_owned(),
            object_entity: object_entity.clone().into_owned(),
        })
    }
}

#[derive(Error, Debug)]
#[error(
    "Entities relation not found: subject_entity='{subject_entity:?}', \
     object_entity='{object_entity:?}'"
)]
pub struct EntitiesRelationNotFoundError {
    pub subject_entity: Entity<'static>,
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
pub enum GetObjectEntityRelationsError {
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
pub enum GetRelationBetweenEntitiesError {
    #[error(transparent)]
    NotFound(EntitiesRelationNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl GetRelationBetweenEntitiesError {
    pub fn not_found(subject_entity: &Entity, object_entity: &Entity) -> Self {
        Self::NotFound(EntitiesRelationNotFoundError {
            subject_entity: subject_entity.clone().into_owned(),
            object_entity: object_entity.clone().into_owned(),
        })
    }
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

#[derive(Error, Debug)]
pub enum DeleteEntitiesRelationsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
