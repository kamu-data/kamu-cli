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

use crate::{ResourceAccountResolutionError, ResourceAccountResolutionProblemCode};

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

/// Why an account selector could not be resolved to a concrete account.
///
/// Split along the line that matters to callers: [`Self::Resolution`] carries
/// the user-facing selector problems, which are reported to clients as a typed
/// problem, while the remaining variants are authentication, authorization, and
/// infrastructure failures, which are not. Composing
/// [`ResourceAccountResolutionError`] rather than listing its cases inline
/// means that split is made once, here, instead of being re-derived by every
/// consumer.
#[derive(Debug, Error)]
pub enum ResolveManifestAccountError {
    #[error(transparent)]
    Resolution(#[from] ResourceAccountResolutionError),

    #[error(transparent)]
    AccountAccess(#[from] ResourceAccountAccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

/// The caller may not act on the target account at all — distinct from the
/// selector being unresolvable, and never reported to clients as a typed
/// problem.
#[derive(Debug, Error)]
pub enum ResourceAccountAccessError {
    #[error("Anonymous subject cannot resolve a target account")]
    AnonymousSubject,

    #[error(transparent)]
    Access(#[from] odf::AccessError),
}

impl ResolveManifestAccountError {
    pub fn empty_selector() -> Self {
        Self::Resolution(ResourceAccountResolutionError {
            code: ResourceAccountResolutionProblemCode::EmptySelector,
            message: "Account selector must specify at least one of `id`, `did`, or `name`"
                .to_string(),
        })
    }

    pub fn selector_mismatch(
        did: &odf::AccountID,
        actual_name: &odf::AccountName,
        expected_resource_id: Option<odf::ResourceID>,
        expected_did: Option<&odf::AccountID>,
        expected_name: Option<&odf::AccountName>,
    ) -> Self {
        Self::Resolution(ResourceAccountResolutionError {
            code: ResourceAccountResolutionProblemCode::SelectorMismatch,
            message: format!(
                "Account selector mismatch: resolved account '{did}' ({actual_name}) does not \
                 match the provided selector (expected resource id: {expected_resource_id:?}, \
                 expected did: {expected_did:?}, expected name: {expected_name:?})"
            ),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<kamu_accounts::GetAccountByIdError> for ResolveManifestAccountError {
    fn from(value: kamu_accounts::GetAccountByIdError) -> Self {
        match value {
            kamu_accounts::GetAccountByIdError::NotFound(err) => {
                Self::Resolution(ResourceAccountResolutionError {
                    code: ResourceAccountResolutionProblemCode::AccountNotFoundById,
                    message: err.to_string(),
                })
            }
            kamu_accounts::GetAccountByIdError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<kamu_accounts::GetAccountByNameError> for ResolveManifestAccountError {
    fn from(value: kamu_accounts::GetAccountByNameError) -> Self {
        match value {
            kamu_accounts::GetAccountByNameError::NotFound(err) => {
                Self::Resolution(ResourceAccountResolutionError {
                    code: ResourceAccountResolutionProblemCode::AccountNotFoundByName,
                    message: err.to_string(),
                })
            }
            kamu_accounts::GetAccountByNameError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
