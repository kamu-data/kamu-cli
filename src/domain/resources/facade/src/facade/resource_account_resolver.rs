// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_resources::{ResourceManifestAccount, ResourceView};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResolvedAccount {
    pub id: odf::AccountID,
    pub name: odf::AccountName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceAccountResolver: Send + Sync {
    async fn resolve_target_account(
        &self,
        selector: Option<&ResourceManifestAccount>,
    ) -> Result<ResolvedAccount, ResolveManifestAccountError>;

    async fn hydrate_resource_view_account(
        &self,
        view: ResourceView,
        target_account: Option<&ResolvedAccount>,
    ) -> Result<ResourceView, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResolveManifestAccountError {
    #[error("Anonymous subject cannot resolve a target account")]
    AnonymousSubject,

    #[error("Account selector must contain either id or name")]
    EmptySelector,

    #[error(transparent)]
    AccountNotFoundById(kamu_accounts::AccountNotFoundByIdError),

    #[error(transparent)]
    AccountNotFoundByName(kamu_accounts::AccountNotFoundByNameError),

    #[error(
        "Account selector mismatch: id '{account_id}' belongs to '{actual_name}', not \
         '{expected_name}'"
    )]
    IdNameMismatch {
        account_id: odf::AccountID,
        expected_name: odf::AccountName,
        actual_name: odf::AccountName,
    },

    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<kamu_accounts::GetAccountByIdError> for ResolveManifestAccountError {
    fn from(value: kamu_accounts::GetAccountByIdError) -> Self {
        match value {
            kamu_accounts::GetAccountByIdError::NotFound(err) => Self::AccountNotFoundById(err),
            kamu_accounts::GetAccountByIdError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<kamu_accounts::GetAccountByNameError> for ResolveManifestAccountError {
    fn from(value: kamu_accounts::GetAccountByNameError) -> Self {
        match value {
            kamu_accounts::GetAccountByNameError::NotFound(err) => Self::AccountNotFoundByName(err),
            kamu_accounts::GetAccountByNameError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
