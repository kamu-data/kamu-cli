// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use database_common::{TransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_auth_rebac::*;
use sqlx::Row;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresRebacRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn RebacRepository)]
impl PostgresRebacRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl RebacRepository for PostgresRebacRepository {
    async fn properties_count(&self) -> Result<usize, PropertiesCountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let properties_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)
            FROM auth_rebac_properties
            "#,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(usize::try_from(properties_count.unwrap()).unwrap())
    }

    async fn set_entity_property(
        &self,
        entity: &Entity,
        property_name: PropertyName,
        property_value: &PropertyValue,
    ) -> Result<(), SetEntityPropertyError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            INSERT INTO auth_rebac_properties (entity_type, entity_id, property_name, property_value) VALUES ($1, $2, $3, $4)
            ON CONFLICT(entity_type, entity_id, property_name)
                DO UPDATE SET property_value = excluded.property_value
            "#,
            entity.entity_type as EntityType,
            entity.entity_id.as_ref(),
            property_name.to_string(),
            property_value.as_ref(),
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn delete_entity_property(
        &self,
        entity: &Entity,
        property_name: PropertyName,
    ) -> Result<(), DeleteEntityPropertyError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let delete_result = sqlx::query!(
            r#"
            DELETE
            FROM auth_rebac_properties
            WHERE entity_type = $1
              AND entity_id = $2
              AND property_name = $3
            "#,
            entity.entity_type as EntityType,
            entity.entity_id.as_ref(),
            property_name.to_string(),
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        if delete_result.rows_affected() == 0 {
            return Err(DeleteEntityPropertyError::not_found(entity, property_name));
        }

        Ok(())
    }

    async fn delete_entity_properties(
        &self,
        entity: &Entity,
    ) -> Result<(), DeleteEntityPropertiesError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let delete_result = sqlx::query!(
            r#"
            DELETE
            FROM auth_rebac_properties
            WHERE entity_type = $1
              AND entity_id = $2
            "#,
            entity.entity_type as EntityType,
            entity.entity_id.as_ref(),
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        if delete_result.rows_affected() == 0 {
            return Err(DeleteEntityPropertiesError::not_found(entity));
        }

        Ok(())
    }

    async fn get_entity_properties(
        &self,
        entity: &Entity,
    ) -> Result<Vec<(PropertyName, PropertyValue)>, GetEntityPropertiesError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let row_models = sqlx::query_as!(
            PropertyRowModel,
            r#"
            SELECT property_name, property_value
            FROM auth_rebac_properties
            WHERE entity_type = $1
              AND entity_id = $2
            "#,
            entity.entity_type as EntityType,
            entity.entity_id.as_ref(),
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        row_models
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(GetEntityPropertiesError::Internal)
    }

    async fn get_entities_properties(
        &self,
        entities: &[Entity],
    ) -> Result<Vec<(Entity, PropertyName, PropertyValue)>, GetEntityPropertiesError> {
        if entities.is_empty() {
            return Ok(vec![]);
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let placeholder_list = {
            (1..=entities.len())
                .map(|i| {
                    // i | idxs
                    // 1 | 1, 2
                    // 2 | 3, 4
                    // 3 | 5, 6
                    // ...
                    let entity_type_idx = i * 2 - 1;
                    let entity_id_idx = i * 2;

                    format!("(${entity_type_idx},${entity_id_idx})")
                })
                .intersperse(",".to_string())
                .collect::<String>()
        };

        // TODO: replace it by macro once sqlx will support it
        // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
        let query_str = format!(
            r#"
            SELECT entity_type, entity_id, property_name, property_value
            FROM auth_rebac_properties
            WHERE (entity_type, entity_id) IN ({placeholder_list})
            "#,
        );

        let mut query = sqlx::query(&query_str);
        for entity in entities {
            query = query.bind(entity.entity_type);
            query = query.bind(entity.entity_id.to_string());
        }

        let raw_rows = query.fetch_all(connection_mut).await.int_err()?;
        let entity_properties: Vec<_> = raw_rows
            .into_iter()
            .map(|row| {
                let entity_type = row.get_unchecked("entity_type");
                let entity_id = row.get_unchecked::<String, _>("entity_id");
                let property_name = row.get_unchecked::<String, _>("property_name").parse()?;
                let property_value = Cow::Owned(row.get_unchecked("property_value"));

                Ok((
                    Entity::new(entity_type, entity_id),
                    property_name,
                    property_value,
                ))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(GetEntityPropertiesError::Internal)?;

        Ok(entity_properties)
    }

    async fn insert_entities_relation(
        &self,
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> Result<(), InsertEntitiesRelationError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            INSERT INTO auth_rebac_relations (
                subject_entity_type, subject_entity_id, relationship, object_entity_type, object_entity_id
            )
            VALUES ($1, $2, $3, $4, $5)
            "#,
            subject_entity.entity_type as EntityType,
            subject_entity.entity_id.as_ref(),
            relationship.to_string(),
            object_entity.entity_type as EntityType,
            object_entity.entity_id.as_ref(),
        )
        .execute(connection_mut)
        .await
            .map_err(|e| match e {
                sqlx::Error::Database(e) if e.is_unique_violation() => {
                    InsertEntitiesRelationError::duplicate(
                        subject_entity,
                        relationship,
                        object_entity,
                    )
                }
                _ => InsertEntitiesRelationError::Internal(e.int_err()),
            })?;

        Ok(())
    }

    async fn delete_entities_relation(
        &self,
        subject_entity: &Entity,
        relationship: Relation,
        object_entity: &Entity,
    ) -> Result<(), DeleteEntitiesRelationError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let delete_result = sqlx::query!(
            r#"
            DELETE
            FROM auth_rebac_relations
            WHERE subject_entity_type = $1
              AND subject_entity_id = $2
              AND relationship = $3
              AND object_entity_type = $4
              AND object_entity_id = $5
            "#,
            subject_entity.entity_type as EntityType,
            subject_entity.entity_id.as_ref(),
            relationship.to_string(),
            object_entity.entity_type as EntityType,
            object_entity.entity_id.as_ref(),
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        if delete_result.rows_affected() == 0 {
            return Err(DeleteEntitiesRelationError::not_found(
                subject_entity,
                relationship,
                object_entity,
            ));
        }

        Ok(())
    }

    async fn get_subject_entity_relations(
        &self,
        subject_entity: &Entity,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let row_models = sqlx::query_as!(
            EntityWithRelationRowModel,
            r#"
            SELECT object_entity_type as "entity_type: EntityType",
                   object_entity_id as entity_id,
                   relationship
            FROM auth_rebac_relations
            WHERE subject_entity_type = $1
              AND subject_entity_id = $2
            "#,
            subject_entity.entity_type as EntityType,
            subject_entity.entity_id.as_ref(),
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        row_models
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(SubjectEntityRelationsError::Internal)
    }

    async fn get_object_entity_relations(
        &self,
        _object_entity: &Entity,
    ) -> Result<Vec<EntityWithRelation>, ObjectEntityRelationsError> {
        todo!("TODO: Private Datasets: implementation")
    }

    async fn get_subject_entity_relations_by_object_type(
        &self,
        subject_entity: &Entity,
        object_entity_type: EntityType,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsByObjectTypeError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(SubjectEntityRelationsByObjectTypeError::Internal)?;

        let row_models = sqlx::query_as!(
            EntityWithRelationRowModel,
            r#"
            SELECT object_entity_type as "entity_type: EntityType",
                   object_entity_id as entity_id,
                   relationship
            FROM auth_rebac_relations
            WHERE subject_entity_type = $1
              AND subject_entity_id = $2
              AND object_entity_type = $3
            "#,
            subject_entity.entity_type as EntityType,
            subject_entity.entity_id.as_ref(),
            object_entity_type as EntityType,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        row_models
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(SubjectEntityRelationsByObjectTypeError::Internal)
    }

    async fn get_relations_between_entities(
        &self,
        subject_entity: &Entity,
        object_entity: &Entity,
    ) -> Result<Vec<Relation>, GetRelationsBetweenEntitiesError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetRelationsBetweenEntitiesError::Internal)?;

        let row_models = sqlx::query_as!(
            RelationRowModel,
            r#"
            SELECT relationship
            FROM auth_rebac_relations
            WHERE subject_entity_type = $1
              AND subject_entity_id = $2
              AND object_entity_type = $3
              AND object_entity_id = $4
            "#,
            subject_entity.entity_type as EntityType,
            subject_entity.entity_id.as_ref(),
            object_entity.entity_type as EntityType,
            object_entity.entity_id.as_ref(),
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        row_models
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(GetRelationsBetweenEntitiesError::Internal)
    }

    async fn delete_subject_entities_object_entity_relations(
        &self,
        _subject_entities: Vec<Entity<'static>>,
        _object_entity: &Entity,
    ) -> Result<(), DeleteSubjectEntitiesObjectEntityRelationsError> {
        todo!("TODO: Private Datasets: implementation")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
