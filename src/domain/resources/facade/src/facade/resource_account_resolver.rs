// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_accounts::{AnonymousPrincipalError, Principal};
use kamu_resources::{ResourceOperationError, ResourceOwnerRef};
use thiserror::Error;

use crate::{ResourceAccountResolutionError, ResourceAccountResolutionProblemCode};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves the account selectors of facade calls, and checks the default
/// rule on them (`ResourceAccountAccessPort`) where no type's
/// `ResourceOperationGuard` decides instead.
#[async_trait::async_trait]
pub trait ResourceAccountResolver: Send + Sync {
    /// Resolves `selector`, or the principal's own account without one,
    /// without checking access. Rejects an anonymous principal
    async fn resolve_account(
        &self,
        principal: &Principal,
        selector: Option<&ResourceOwnerRef>,
    ) -> Result<odf::ResourceOwnerHandle, ResolveManifestAccountError>;

    /// Checks the default rule
    async fn ensure_can_use_account(
        &self,
        principal: &Principal,
        account: &odf::ResourceOwnerHandle,
    ) -> Result<(), ResolveManifestAccountError>;

    async fn resolve_target_account(
        &self,
        principal: &Principal,
        selector: Option<&ResourceOwnerRef>,
    ) -> Result<odf::ResourceOwnerHandle, ResolveManifestAccountError> {
        let account = self.resolve_account(principal, selector).await?;
        self.ensure_can_use_account(principal, &account).await?;
        Ok(account)
    }

    /// Resolves several account refs at once, for calls whose selectors each
    /// name their own account, without checking access.
    ///
    /// Returns one handle per input, in order, so callers can zip the result
    /// back onto their selectors. Any failure fails the whole call.
    ///
    /// Deduplicated by spelling: the same account named twice resolves once.
    /// Two spellings of one account (by id and by name) still resolve twice,
    /// which costs a lookup but cannot produce a wrong answer.
    async fn resolve_accounts(
        &self,
        principal: &Principal,
        selectors: &[Option<ResourceOwnerRef>],
    ) -> Result<Vec<odf::ResourceOwnerHandle>, ResolveManifestAccountError> {
        let mut resolved: Vec<(Option<ResourceOwnerRef>, odf::ResourceOwnerHandle)> = Vec::new();
        let mut out = Vec::with_capacity(selectors.len());

        for selector in selectors {
            if let Some((_, handle)) = resolved.iter().find(|(seen, _)| seen == selector) {
                out.push(handle.clone());
                continue;
            }

            let handle = self.resolve_account(principal, selector.as_ref()).await?;
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

impl From<AnonymousPrincipalError> for ResourceAccountAccessError {
    fn from(_: AnonymousPrincipalError) -> Self {
        Self::AnonymousSubject
    }
}

impl From<AnonymousPrincipalError> for ResolveManifestAccountError {
    fn from(err: AnonymousPrincipalError) -> Self {
        ResourceAccountAccessError::from(err).into()
    }
}

impl ResolveManifestAccountError {
    /// A denial of a guard deciding on `account` alone. A resource hidden
    /// from the principal there means they may not use the account
    pub(crate) fn from_operation_error(
        account: &odf::ResourceOwnerHandle,
        err: ResourceOperationError,
    ) -> Self {
        match err {
            ResourceOperationError::Access(err) => ResourceAccountAccessError::Access(err).into(),
            ResourceOperationError::NotFound(_) => {
                ResourceAccountAccessError::Access(odf::AccessError::Unauthorized(
                    format!(
                        "Current subject is not allowed to use resources of account '{}'",
                        account.name
                    )
                    .into(),
                ))
                .into()
            }
            ResourceOperationError::Internal(err) => Self::Internal(err),
        }
    }

    pub fn empty_selector() -> Self {
        Self::Resolution(ResourceAccountResolutionError {
            code: ResourceAccountResolutionProblemCode::EmptySelector,
            message: "Account selector must specify `did` or `name`; `id` is only checked for \
                      agreement"
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

#[cfg(test)]
mod tests {
    use kamu_resources::{ResourceID, ResourceIDNotFoundError};
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn guard_hiding_a_resource_denies_the_account() {
        let account = odf::ResourceOwnerHandle::new_test("bob");

        let err = ResolveManifestAccountError::from_operation_error(
            &account,
            ResourceOperationError::NotFound(ResourceIDNotFoundError(
                "00000000-0000-0000-0000-000000000001"
                    .parse::<ResourceID>()
                    .unwrap(),
            )),
        );

        let ResolveManifestAccountError::AccountAccess(ResourceAccountAccessError::Access(
            odf::AccessError::Unauthorized(source),
        )) = err
        else {
            panic!("expected an account access denial, got: {err:?}");
        };
        assert_eq!(
            source.to_string(),
            "Current subject is not allowed to use resources of account 'bob'"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
