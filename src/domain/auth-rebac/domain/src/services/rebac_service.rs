// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use internal_error::InternalError;
use opendatafabric as odf;
use thiserror::Error;

use crate::{
    AccountPropertyName,
    AccountToDatasetRelation,
    DatasetPropertyName,
    EntityNotFoundError,
    EntityWithRelation,
    PropertiesCountError,
    PropertyValue,
    SetEntityPropertyError,
    SubjectEntityRelationsError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RebacService: Send + Sync {
    async fn properties_count(&self) -> Result<usize, PropertiesCountError>;

    // Account
    async fn set_account_property(
        &self,
        account_id: &odf::AccountID,
        property_name: AccountPropertyName,
        property_value: &PropertyValue,
    ) -> Result<(), SetEntityPropertyError>;

    async fn unset_account_property(
        &self,
        account_id: &odf::AccountID,
        property_name: AccountPropertyName,
    ) -> Result<(), UnsetEntityPropertyError>;

    async fn get_account_properties(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<AccountProperties, GetPropertiesError>;

    // Dataset
    async fn set_dataset_property(
        &self,
        dataset_id: &odf::DatasetID,
        property_name: DatasetPropertyName,
        property_value: &PropertyValue,
    ) -> Result<(), SetEntityPropertyError>;

    async fn unset_dataset_property(
        &self,
        dataset_id: &odf::DatasetID,
        property_name: DatasetPropertyName,
    ) -> Result<(), UnsetEntityPropertyError>;

    async fn delete_dataset_properties(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), DeletePropertiesError>;

    async fn get_dataset_properties(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetProperties, GetPropertiesError>;

    async fn get_dataset_properties_by_ids(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<HashMap<odf::DatasetID, DatasetProperties>, GetPropertiesError>;

    // Relations
    async fn insert_account_dataset_relation(
        &self,
        account_id: &odf::AccountID,
        relationship: AccountToDatasetRelation,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InsertRelationError>;

    async fn delete_account_dataset_relation(
        &self,
        account_id: &odf::AccountID,
        relationship: AccountToDatasetRelation,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), DeleteRelationError>;

    async fn get_account_dataset_relations(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct AccountProperties {
    pub is_admin: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct DatasetProperties {
    pub allows_anonymous_read: bool,
    pub allows_public_read: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UnsetEntityPropertyError {
    #[error(transparent)]
    NotFound(EntityNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeletePropertiesError {
    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetPropertiesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum InsertRelationError {
    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteRelationError {
    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
