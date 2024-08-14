// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{TransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_auth_rebac::{
    DeleteEntitiesRelationError,
    DeleteEntityPropertyError,
    Entity,
    EntityType,
    EntityWithRelation,
    GetEntityPropertiesError,
    GetRelationsBetweenEntitiesError,
    InsertEntitiesRelationError,
    PropertyName,
    PropertyRowModel,
    PropertyValue,
    RebacRepository,
    Relation,
    SetEntityPropertyError,
    SubjectEntityRelationsByObjectTypeError,
    SubjectEntityRelationsError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteRebacRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[component(pub)]
#[interface(dyn RebacRepository)]
impl SqliteRebacRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl RebacRepository for SqliteRebacRepository {
    async fn set_entity_property(
        &self,
        entity: &Entity,
        property_name: PropertyName,
        property_value: &PropertyValue,
    ) -> Result<(), SetEntityPropertyError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(SetEntityPropertyError::Internal)?;

        let entity_id_as_str = entity.entity_id.as_ref();
        let property_name_as_str = property_name.to_string();
        let property_value_as_str = property_value.as_ref();

        sqlx::query!(
            r#"
            INSERT INTO auth_rebac_properties (entity_type, entity_id, property_name, property_value) VALUES ($1, $2, $3, $4)
            ON CONFLICT(entity_type, entity_id, property_name)
                DO UPDATE SET property_value = excluded.property_value
            "#,
            entity.entity_type,
            entity_id_as_str,
            property_name_as_str,
            property_value_as_str,
        )
        .execute(connection_mut)
        .await
        .map_int_err(SetEntityPropertyError::Internal)?;

        Ok(())
    }

    async fn delete_entity_property(
        &self,
        entity: &Entity,
        property_name: PropertyName,
    ) -> Result<(), DeleteEntityPropertyError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(DeleteEntityPropertyError::Internal)?;

        let entity_id_as_str = entity.entity_id.as_ref();
        let property_name_as_str = property_name.to_string();

        let delete_result = sqlx::query!(
            r#"
            DELETE
            FROM auth_rebac_properties
            WHERE entity_type = $1
              AND entity_id = $2
              AND property_name = $3
            "#,
            entity.entity_type,
            entity_id_as_str,
            property_name_as_str,
        )
        .execute(&mut *connection_mut)
        .await
        .map_int_err(DeleteEntityPropertyError::Internal)?;

        if delete_result.rows_affected() == 0 {
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
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetEntityPropertiesError::Internal)?;

        let entity_id_as_str = entity.entity_id.as_ref();

        let maybe_dataset_entry_rows = sqlx::query_as!(
            PropertyRowModel,
            r#"
            SELECT property_name, property_value
            FROM auth_rebac_properties
            WHERE entity_type = $1
              AND entity_id = $2
            "#,
            entity.entity_type,
            entity_id_as_str,
        )
        .fetch_all(connection_mut)
        .await
        .map_int_err(GetEntityPropertiesError::Internal)?;

        maybe_dataset_entry_rows
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, InternalError>>()
            .map_err(GetEntityPropertiesError::Internal)
    }

    async fn insert_entities_relation(
        &self,
        _subject_entity: &Entity,
        _relationship: Relation,
        _object_entity: &Entity,
    ) -> Result<(), InsertEntitiesRelationError> {
        todo!()
    }

    async fn delete_entities_relation(
        &self,
        _subject_entity: &Entity,
        _relationship: Relation,
        _object_entity: &Entity,
    ) -> Result<(), DeleteEntitiesRelationError> {
        todo!()
    }

    async fn get_subject_entity_relations(
        &self,
        _subject_entity: &Entity,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsError> {
        todo!()
    }

    async fn get_subject_entity_relations_by_object_type(
        &self,
        _subject_entity: &Entity,
        _object_entity_type: EntityType,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsByObjectTypeError> {
        todo!()
    }

    async fn get_relations_between_entities(
        &self,
        _subject_entity: &Entity,
        _object_entity: &Entity,
    ) -> Result<Vec<Relation>, GetRelationsBetweenEntitiesError> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
