// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use opendatafabric::{AccountID, DatasetID};

use crate::{
    DeleteEntitiesRelationError,
    DeleteEntityPropertyError,
    GetEntityPropertiesError,
    GetEntityRelationsError,
    InsertEntitiesRelationError,
    ObjectEntity,
    Property,
    PropertyName,
    PropertyValue,
    Relation,
    UpsertEntityPropertyError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RebacService: Send + Sync {
    // Account
    async fn set_account_property(
        &self,
        account_id: &AccountID,
        property: &Property,
    ) -> Result<(), UpsertEntityPropertyError>;

    async fn unset_account_property(
        &self,
        account_id: &AccountID,
        property_name: PropertyName,
    ) -> Result<(), DeleteEntityPropertyError>;

    async fn get_account_properties(
        &self,
        account_id: &AccountID,
    ) -> Result<HashMap<PropertyName, PropertyValue>, GetEntityPropertiesError>;

    // Dataset
    async fn set_dataset_property(
        &self,
        dataset_id: &DatasetID,
        property: &Property,
    ) -> Result<(), UpsertEntityPropertyError>;

    async fn unset_dataset_property(
        &self,
        dataset_id: &DatasetID,
        property_name: PropertyName,
    ) -> Result<(), DeleteEntityPropertyError>;

    async fn get_dataset_properties(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<HashMap<PropertyName, PropertyValue>, GetEntityPropertiesError>;

    // Relations
    async fn insert_account_dataset_relation(
        &self,
        account_id: &AccountID,
        relationship: Relation,
        dataset_id: &DatasetID,
    ) -> Result<(), InsertEntitiesRelationError>;

    async fn delete_account_dataset_relation(
        &self,
        account_id: &AccountID,
        relationship: Relation,
        dataset_id: &DatasetID,
    ) -> Result<(), DeleteEntitiesRelationError>;

    async fn get_account_dataset_relations(
        &self,
        account_id: &AccountID,
    ) -> Result<HashMap<Relation, Vec<ObjectEntity>>, GetEntityRelationsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
