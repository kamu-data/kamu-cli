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
use kamu_core::auth::{DatasetAction, DatasetsActionNotEnoughPermissionsError};
use thiserror::Error;

use crate::{
    AccountPropertyName,
    AccountToDatasetRelation,
    DatasetPropertyName,
    EntityNotFoundError,
    EntityWithRelation,
    GetObjectEntityRelationsError,
    PROPERTY_VALUE_BOOLEAN_TRUE,
    PropertiesCountError,
    PropertyValue,
    SetEntityPropertyError,
    SubjectEntityRelationsError,
    boolean_property_value,
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
    async fn set_account_dataset_relation(
        &self,
        account_id: &odf::AccountID,
        relationship: AccountToDatasetRelation,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), SetRelationError>;

    async fn unset_accounts_dataset_relations(
        &self,
        account_ids: &[&odf::AccountID],
        dataset_id: &odf::DatasetID,
    ) -> Result<(), UnsetRelationError>;

    async fn apply_roles_matrix(
        &self,
        account_ids: &[&odf::AccountID],
        datasets_with_maybe_roles: &[(odf::DatasetID, Option<AccountToDatasetRelation>)],
    ) -> Result<(), ApplyRelationMatrixError>;

    async fn get_account_dataset_relations(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Vec<EntityWithRelation>, SubjectEntityRelationsError>;

    async fn get_authorized_accounts(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<AuthorizedAccount>, GetObjectEntityRelationsError>;

    async fn get_authorized_accounts_by_ids(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<HashMap<odf::DatasetID, Vec<AuthorizedAccount>>, GetObjectEntityRelationsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RebacServiceExt {
    async fn is_account_admin(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<bool, GetPropertiesError>;

    async fn can_provision_accounts(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<bool, GetPropertiesError>;
}

#[async_trait::async_trait]
impl<T: RebacService + ?Sized> RebacServiceExt for T {
    async fn is_account_admin(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<bool, GetPropertiesError> {
        let account_properties = self.get_account_properties(account_id).await?;
        Ok(account_properties.is_admin)
    }

    async fn can_provision_accounts(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<bool, GetPropertiesError> {
        let account_properties = self.get_account_properties(account_id).await?;
        Ok(account_properties.is_admin || account_properties.can_provision_accounts)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct AccountProperties {
    pub is_admin: bool,
    pub can_provision_accounts: bool,
}

impl AccountProperties {
    pub fn apply(&mut self, name: AccountPropertyName, value: &PropertyValue) {
        match name {
            AccountPropertyName::IsAdmin => {
                self.is_admin = value == PROPERTY_VALUE_BOOLEAN_TRUE;
            }
            AccountPropertyName::CanProvisionAccounts => {
                self.can_provision_accounts = value == PROPERTY_VALUE_BOOLEAN_TRUE;
            }
        }
    }

    pub fn as_property_value<'a>(&self, name: AccountPropertyName) -> PropertyValue<'a> {
        let value = match name {
            AccountPropertyName::IsAdmin => self.is_admin,
            AccountPropertyName::CanProvisionAccounts => self.can_provision_accounts,
        };

        boolean_property_value(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct DatasetProperties {
    pub allows_anonymous_read: bool,
    pub allows_public_read: bool,
}

impl DatasetProperties {
    pub fn apply(&mut self, name: DatasetPropertyName, value: &PropertyValue) {
        match name {
            DatasetPropertyName::AllowsAnonymousRead => {
                self.allows_anonymous_read = value == PROPERTY_VALUE_BOOLEAN_TRUE;
            }
            DatasetPropertyName::AllowsPublicRead => {
                self.allows_public_read = value == PROPERTY_VALUE_BOOLEAN_TRUE;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct AuthorizedAccount {
    pub account_id: odf::AccountID,
    pub role: AccountToDatasetRelation,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UnsetEntityPropertyError {
    #[error(transparent)]
    NotFound(EntityNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeletePropertiesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetPropertiesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SetRelationError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UnsetRelationError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ApplyRelationMatrixError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl ApplyRelationMatrixError {
    pub fn not_enough_permissions(
        dataset_refs: Vec<odf::DatasetRef>,
        action: DatasetAction,
    ) -> Self {
        Self::Access(odf::AccessError::Unauthorized(
            DatasetsActionNotEnoughPermissionsError {
                action,
                dataset_refs,
            }
            .into(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
