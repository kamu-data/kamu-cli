// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_resources::ResourceAccountRef;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceAccountResolver: Send + Sync {
    async fn resolve_target_account(
        &self,
        selector: Option<&ResourceAccountRef>,
    ) -> Result<odf::AccountHandle, ResolveManifestAccountError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResolveManifestAccountError {
    #[error("Anonymous subject cannot resolve a target account")]
    AnonymousSubject,

    #[error("Account selector must specify at least one of `id`, `did`, or `name`")]
    EmptySelector,

    #[error(transparent)]
    AccountNotFoundById(kamu_accounts::AccountNotFoundByIdError),

    #[error(transparent)]
    AccountNotFoundByName(kamu_accounts::AccountNotFoundByNameError),

    #[error(
        "Account selector mismatch: resolved account '{did}' ({actual_name}) does not match the \
         provided selector (expected resource id: {expected_resource_id:?}, expected did: \
         {expected_did:?}, expected name: {expected_name:?})"
    )]
    SelectorMismatch {
        did: odf::AccountID,
        actual_name: odf::AccountName,
        expected_resource_id: Option<odf::ResourceID>,
        expected_did: Option<odf::AccountID>,
        expected_name: Option<odf::AccountName>,
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
