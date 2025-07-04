// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::num::NonZeroUsize;

use database_common::{
    TransactionRef,
    TransactionRefT,
    postgres_generate_placeholders_tuple_list_2,
};
use dill::{component, interface};
use internal_error::ResultIntoInternal;
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
            &entity.entity_id,
            property_name.to_string(),
            property_value,
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
            &entity.entity_id,
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
            &entity.entity_id,
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
            &entity.entity_id,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        row_models
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
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

        // TODO: replace it by macro once sqlx will support it
        // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
        let query_str = format!(
            r#"
            SELECT entity_type, entity_id, property_name, property_value
            FROM auth_rebac_properties
            WHERE (entity_type, entity_id) IN ({})
            "#,
            postgres_generate_placeholders_tuple_list_2(
                entities.len(),
                NonZeroUsize::new(1).unwrap()
            )
        );

        let mut query = sqlx::query(&query_str);
        for entity in entities {
            query = query.bind(entity.entity_type);
            query = query.bind(&entity.entity_id);
        }

        let raw_rows = query.fetch_all(connection_mut).await.int_err()?;
        let entity_properties = raw_rows
            .into_iter()
            .map(|row| {
                let entity_type = row.get(0);
                let entity_id = row.get::<String, _>(1);
                let property_name = row.get::<String, _>(2).parse()?;
                let property_value = Cow::Owned(row.get(3));

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

    async fn upsert_entities_relations(
        &self,
        operations: &[UpsertEntitiesRelationOperation<'_>],
    ) -> Result<(), UpsertEntitiesRelationsError> {
        if operations.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let mut subject_entity_types = Vec::with_capacity(operations.len());
        let mut subject_entity_ids = Vec::with_capacity(operations.len());
        let mut relationships = Vec::with_capacity(operations.len());
        let mut object_entity_types = Vec::with_capacity(operations.len());
        let mut object_entity_ids = Vec::with_capacity(operations.len());

        for op in operations {
            subject_entity_types.push(op.subject_entity.entity_type);
            subject_entity_ids.push(op.subject_entity.entity_id.as_ref());
            relationships.push(op.relationship.to_string());
            object_entity_types.push(op.object_entity.entity_type);
            object_entity_ids.push(op.object_entity.entity_id.as_ref());
        }

        sqlx::query!(
            r#"
            INSERT INTO auth_rebac_relations (subject_entity_type,
                                              subject_entity_id,
                                              relationship,
                                              object_entity_type,
                                              object_entity_id)
            SELECT *
            FROM UNNEST($1::rebac_entity_type[],
                        $2::TEXT[],
                        $3::TEXT[],
                        $4::rebac_entity_type[],
                        $5::TEXT[])
            ON CONFLICT(subject_entity_type,
                        subject_entity_id,
                        object_entity_type,
                        object_entity_id)
                DO UPDATE SET relationship = excluded.relationship
            "#,
            subject_entity_types as Vec<EntityType>,
            subject_entity_ids.as_slice() as &[&str],
            &relationships,
            object_entity_types as Vec<EntityType>,
            object_entity_ids.as_slice() as &[&str],
        )
        .execute(connection_mut)
        .await
        .int_err()?;

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
            SELECT object_entity_type AS "entity_type: EntityType",
                   object_entity_id AS entity_id,
                   relationship
            FROM auth_rebac_relations
            WHERE subject_entity_type = $1
              AND subject_entity_id = $2
            ORDER BY entity_id
            "#,
            subject_entity.entity_type as EntityType,
            &subject_entity.entity_id,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        row_models
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    async fn get_object_entity_relations(
        &self,
        object_entity: &Entity,
    ) -> Result<Vec<EntityWithRelation>, GetObjectEntityRelationsError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let row_models = sqlx::query_as!(
            EntityWithRelationRowModel,
            r#"
            SELECT subject_entity_type AS "entity_type: EntityType",
                   subject_entity_id AS entity_id,
                   relationship
            FROM auth_rebac_relations
            WHERE object_entity_type = $1
              AND object_entity_id = $2
            ORDER BY entity_id
            "#,
            object_entity.entity_type as EntityType,
            &object_entity.entity_id,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        row_models
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    async fn get_object_entities_relations(
        &self,
        object_entities: &[Entity],
    ) -> Result<Vec<EntitiesWithRelation>, GetObjectEntityRelationsError> {
        if object_entities.is_empty() {
            return Ok(vec![]);
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        // TODO: replace it by macro once sqlx will support it
        // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
        let query_str = format!(
            r#"
            SELECT subject_entity_type,
                   subject_entity_id,
                   relationship,
                   object_entity_type,
                   object_entity_id
            FROM auth_rebac_relations
            WHERE (object_entity_type, object_entity_id) IN ({})
            ORDER BY subject_entity_id, object_entity_id
            "#,
            postgres_generate_placeholders_tuple_list_2(
                object_entities.len(),
                NonZeroUsize::new(1).unwrap()
            )
        );

        let mut query = sqlx::query(&query_str);
        for entity in object_entities {
            query = query.bind(entity.entity_type);
            query = query.bind(&entity.entity_id);
        }

        let raw_rows = query.fetch_all(connection_mut).await.int_err()?;
        let rows = raw_rows
            .into_iter()
            .map(|row| {
                let raw_entity = EntitiesWithRelationRowModel {
                    subject_entity_type: row.get(0),
                    subject_entity_id: row.get(1),
                    relationship: row.get(2),
                    object_entity_type: row.get(3),
                    object_entity_id: row.get(4),
                };
                let entity = raw_entity.try_into()?;

                Ok(entity)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(GetObjectEntityRelationsError::Internal)?;

        Ok(rows)
    }

    async fn get_subject_entity_relations_by_object_type(
        &self,
        subject_entity: &Entity,
        object_entity_type: EntityType,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsByObjectTypeError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let row_models = sqlx::query_as!(
            EntityWithRelationRowModel,
            r#"
            SELECT object_entity_type AS "entity_type: EntityType",
                   object_entity_id AS entity_id,
                   relationship
            FROM auth_rebac_relations
            WHERE subject_entity_type = $1
              AND subject_entity_id = $2
              AND object_entity_type = $3
            ORDER BY entity_id
            "#,
            subject_entity.entity_type as EntityType,
            &subject_entity.entity_id,
            object_entity_type as EntityType,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        row_models
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    async fn get_relation_between_entities(
        &self,
        subject_entity: &Entity,
        object_entity: &Entity,
    ) -> Result<Relation, GetRelationBetweenEntitiesError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let maybe_row_model = sqlx::query_as!(
            RelationRowModel,
            r#"
            SELECT relationship
            FROM auth_rebac_relations
            WHERE subject_entity_type = $1
              AND subject_entity_id = $2
              AND object_entity_type = $3
              AND object_entity_id = $4
            ORDER BY relationship
            "#,
            subject_entity.entity_type as EntityType,
            &subject_entity.entity_id,
            object_entity.entity_type as EntityType,
            &object_entity.entity_id,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(row_model) = maybe_row_model {
            let relation = row_model.try_into()?;
            Ok(relation)
        } else {
            Err(GetRelationBetweenEntitiesError::not_found(
                subject_entity,
                object_entity,
            ))
        }
    }

    async fn delete_subject_entities_object_entity_relations(
        &self,
        subject_entities: Vec<Entity<'static>>,
        object_entity: &Entity,
    ) -> Result<(), DeleteSubjectEntitiesObjectEntityRelationsError> {
        if subject_entities.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        // TODO: replace it by macro once sqlx will support it
        // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
        let query_str = format!(
            r#"
            DELETE
            FROM auth_rebac_relations
            WHERE object_entity_type = $1
              AND object_entity_id = $2
              AND (subject_entity_type, subject_entity_id) in ({})
            "#,
            postgres_generate_placeholders_tuple_list_2(
                subject_entities.len(),
                NonZeroUsize::new(3).unwrap()
            )
        );

        let mut query = sqlx::query(&query_str)
            .bind(object_entity.entity_type)
            .bind(&object_entity.entity_id);
        for subject_entity in &subject_entities {
            query = query.bind(subject_entity.entity_type);
            query = query.bind(&subject_entity.entity_id);
        }

        let delete_result = query.execute(&mut *connection_mut).await.int_err()?;

        if delete_result.rows_affected() == 0 {
            return Err(DeleteSubjectEntitiesObjectEntityRelationsError::not_found(
                subject_entities,
                object_entity,
            ));
        }

        Ok(())
    }

    async fn delete_entities_relations(
        &self,
        operations: &[DeleteEntitiesRelationOperation<'_>],
    ) -> Result<(), DeleteEntitiesRelationsError> {
        if operations.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let mut subject_entity_types = Vec::with_capacity(operations.len());
        let mut subject_entity_ids = Vec::with_capacity(operations.len());
        let mut object_entity_types = Vec::with_capacity(operations.len());
        let mut object_entity_ids = Vec::with_capacity(operations.len());

        for op in operations {
            subject_entity_types.push(op.subject_entity.entity_type);
            subject_entity_ids.push(op.subject_entity.entity_id.as_ref());
            object_entity_types.push(op.object_entity.entity_type);
            object_entity_ids.push(op.object_entity.entity_id.as_ref());
        }

        sqlx::query!(
            r#"
            DELETE
            FROM auth_rebac_relations
            WHERE (subject_entity_type, subject_entity_id, object_entity_type, object_entity_id) IN
                  (SELECT *
                   FROM UNNEST($1::rebac_entity_type[],
                               $2::TEXT[],
                               $3::rebac_entity_type[],
                               $4::TEXT[]));
            "#,
            subject_entity_types as Vec<EntityType>,
            subject_entity_ids.as_slice() as &[&str],
            object_entity_types as Vec<EntityType>,
            object_entity_ids.as_slice() as &[&str],
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
