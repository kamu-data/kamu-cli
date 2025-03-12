// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use crate::Relation;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(
    feature = "sqlx",
    derive(sqlx::Type),
    sqlx(type_name = "rebac_entity_type", rename_all = "lowercase")
)]
pub enum EntityType {
    Dataset,
    Account,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type EntityId<'a> = Cow<'a, str>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Entity<'a> {
    pub entity_type: EntityType,
    pub entity_id: EntityId<'a>,
}

impl<'a> Entity<'a> {
    pub fn new(entity_type: EntityType, entity_id: impl Into<EntityId<'a>>) -> Self {
        Self {
            entity_type,
            entity_id: entity_id.into(),
        }
    }

    pub fn new_account(entity_id: impl Into<EntityId<'a>>) -> Self {
        Self::new(EntityType::Account, entity_id)
    }

    pub fn new_dataset(entity_id: impl Into<EntityId<'a>>) -> Self {
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

    pub fn new_account(entity_id: impl Into<EntityId<'a>>, relation: Relation) -> Self {
        let account_entity = Entity::new_account(entity_id);

        Self::new(account_entity, relation)
    }

    pub fn new_dataset(entity_id: impl Into<EntityId<'a>>, relation: Relation) -> Self {
        let dataset_entity = Entity::new_dataset(entity_id);

        Self::new(dataset_entity, relation)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntitiesWithRelation<'a> {
    pub subject_entity: Entity<'a>,
    pub relation: Relation,
    pub object_entity: Entity<'a>,
}

impl EntitiesWithRelation<'_> {
    pub fn clone_owned(&self) -> EntitiesWithRelation<'static> {
        EntitiesWithRelation {
            subject_entity: self.subject_entity.clone().into_owned(),
            relation: self.relation,
            object_entity: self.object_entity.clone().into_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct EntityWithRelationRowModel {
    pub entity_type: EntityType,
    pub entity_id: String,
    pub relationship: String,
}

#[cfg(feature = "sqlx")]
impl TryFrom<EntityWithRelationRowModel> for EntityWithRelation<'static> {
    type Error = internal_error::InternalError;

    fn try_from(row_model: EntityWithRelationRowModel) -> Result<Self, Self::Error> {
        let relationship = row_model.relationship.parse()?;
        let entity = Entity::new(row_model.entity_type, row_model.entity_id);

        Ok(Self::new(entity, relationship))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct EntitiesWithRelationRowModel {
    pub subject_entity_type: EntityType,
    pub subject_entity_id: String,
    pub relationship: String,
    pub object_entity_type: EntityType,
    pub object_entity_id: String,
}

#[cfg(feature = "sqlx")]
impl TryFrom<EntitiesWithRelationRowModel> for EntitiesWithRelation<'static> {
    type Error = internal_error::InternalError;

    fn try_from(row_model: EntitiesWithRelationRowModel) -> Result<Self, Self::Error> {
        let relation = row_model.relationship.parse()?;
        let subject_entity =
            Entity::new(row_model.subject_entity_type, row_model.subject_entity_id);
        let object_entity = Entity::new(row_model.object_entity_type, row_model.object_entity_id);

        Ok(Self {
            subject_entity,
            relation,
            object_entity,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
