// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric::DidOdf;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: derives

pub enum EntityType {
    Dataset,
    Account,
}

pub type EntityId = String;

pub enum PropertyName {
    DatasetAllowsAnonymousRead,
    DatasetAllowsPublicRead,
}

pub type PropertyValue = String;

pub enum Relation {
    AccountDatasetReader,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RebacRepository: Send + Sync {
    async fn set_entity_property(
        &self,
        entity_type: EntityType,
        entity_id: &EntityId,
        property_name: &PropertyName,
        property_value: &PropertyValue,
    ) -> Result<(), UpsertEntityPropertyError>;

    async fn delete_entity_property(
        &self,
        entity_type: EntityType,
        entity_id: &EntityId,
        property_name: &PropertyName,
    ) -> Result<(), DeleteEntityPropertyError>;

    async fn get_entity_properties(
        &self,
        entity_type: EntityType,
        entity_id: &EntityId,
    ) -> Result<Vec<EntityRelation>, GetEntityRelationsError>;

    async fn insert_entities_relation(
        &self,
        subject_entity_type: EntityType,
        subject_entity_id: &EntityId,
        relationship: &Relation,
        object_entity_type: EntityType,
        object_entity_id: &EntityId,
    ) -> Result<(), InsertEntitiesRelationError>;

    async fn delete_entities_relation(
        &self,
        subject_entity_type: EntityType,
        subject_entity_id: &EntityId,
        relationship: &Relation,
        object_entity_type: EntityType,
        object_entity_id: &EntityId,
    ) -> Result<(), DeleteEntitiesRelationError>;

    async fn get_entity_relations(
        &self,
        entity_type: EntityType,
        entity_id: &EntityId,
    ) -> Result<Vec<EntityRelation>, GetEntityRelationsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EntityRelation {
    pub relation: Relation,
    pub entity_type: EntityType,
    pub entity_id: EntityId,
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
