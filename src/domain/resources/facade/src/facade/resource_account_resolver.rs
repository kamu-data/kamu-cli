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

    /// Resolves several account refs at once, for calls whose selectors each
    /// name their own account.
    ///
    /// Returns one handle per input, in order, so callers can zip the result
    /// back onto their selectors. **Any denial fails the whole call** — a
    /// partial result would silently narrow the caller's request.
    ///
    /// Deduplicated by spelling: the same account named twice resolves once.
    /// Two spellings of one account (by id and by name) still resolve twice,
    /// which costs a lookup but cannot produce a wrong answer.
    async fn resolve_target_accounts(
        &self,
        selectors: &[Option<ResourceAccountRef>],
    ) -> Result<Vec<odf::AccountHandle>, ResolveManifestAccountError> {
        let mut resolved: Vec<(Option<ResourceAccountRef>, odf::AccountHandle)> = Vec::new();
        let mut out = Vec::with_capacity(selectors.len());

        for selector in selectors {
            if let Some((_, handle)) = resolved.iter().find(|(seen, _)| seen == selector) {
                out.push(handle.clone());
                continue;
            }

            let handle = self.resolve_target_account(selector.as_ref()).await?;
            resolved.push((selector.clone(), handle.clone()));
            out.push(handle);
        }

        Ok(out)
    }
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
