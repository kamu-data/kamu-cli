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

#[derive(Debug, Copy, Clone)]
pub enum EntityType {
    Dataset,
    Account,
}

#[reusable(entity)]
pub struct Entity<'a> {
    pub entity_type: EntityType,
    pub entity_id: Cow<'a, str>,
}

#[reuse(entity)]
pub struct EntityWithRelation<'a> {
    pub relation: Relation,
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

pub struct Property<'a> {
    pub name: PropertyName,
    pub value: Cow<'a, str>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Relation {
    AccountDatasetReader,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RebacRepository: Send + Sync {
    // Properties

    async fn set_entity_property(
        &self,
        entity: &Entity,
        property: &Property,
    ) -> Result<(), UpsertEntityPropertyError>;

    async fn delete_entity_property(
        &self,
        entity: &Entity,
        property_name: PropertyName,
    ) -> Result<(), DeleteEntityPropertyError>;

    async fn get_entity_properties(
        &self,
        entity: &Entity,
    ) -> Result<Vec<Property>, GetEntityRelationsError>;

    // Relations

    async fn insert_entities_relation(
        &self,
        subject_entity: &Entity,
        relationship: &Relation,
        object_entity: &Entity,
    ) -> Result<(), InsertEntitiesRelationError>;

    async fn delete_entities_relation(
        &self,
        subject_entity: &Entity,
        relationship: &Relation,
        object_entity: &Entity,
    ) -> Result<(), DeleteEntitiesRelationError>;

    async fn get_subject_entity_relations(
        &self,
        subject_entity: &Entity,
    ) -> Result<Vec<ObjectEntityWithRelation>, GetEntityRelationsError>;

    async fn get_subject_entity_relations_by_object_type(
        &self,
        subject_entity: &Entity,
        object_entity_type: EntityType,
    ) -> Result<Vec<ObjectEntityWithRelation>, GetEntityRelationsError>;

    async fn get_relations_between_entities(
        &self,
        subject_entity: &Entity,
        object_entity: &Entity,
    ) -> Result<Vec<Relation>, GetEntityRelationsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UpsertEntityPropertyError {
    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteEntityPropertyError {
    #[error(transparent)]
    NotFound(EntityPropertyNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Error, Debug)]
#[error("Entity property not found: {self}")]
pub struct EntityPropertyNotFoundError {
    pub entity_type: String,
    pub entity_id: String,
    pub property_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetEntityPropertiesError {
    #[error(transparent)]
    NotFound(EntityPropertiesNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Error, Debug)]
#[error("Entity properties not found: {self}")]
pub struct EntityPropertiesNotFoundError {
    pub entity_type: String,
    pub entity_id: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum InsertEntitiesRelationError {
    #[error(transparent)]
    Duplicate(InsertEntitiesRelationDuplicateError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Error, Debug)]
#[error("Duplicate entity relation not inserted: {self}")]
pub struct InsertEntitiesRelationDuplicateError {
    pub subject_entity_type: String,
    pub subject_entity_id: String,
    pub relationship: String,
    pub object_entity_type: String,
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

#[derive(Error, Debug)]
#[error("Entities relation not found: {self}")]
pub struct EntitiesRelationNotFoundError {
    pub subject_entity_type: String,
    pub subject_entity_id: String,
    pub relationship: String,
    pub object_entity_type: String,
    pub object_entity_id: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetEntityRelationsError {
    #[error(transparent)]
    NotFound(EntityPropertiesNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
