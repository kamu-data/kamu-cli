// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_accounts::{AccountHandle, AccountService, Principal};
use kamu_auth_rebac::{RebacService, RebacServiceExt};
use kamu_resources::ResourceOwnerRef;

use crate::{ResolveManifestAccountError, ResourceAccountAccessError, ResourceAccountResolver};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ResourceAccountResolver)]
pub(crate) struct ResourceAccountResolverImpl {
    account_service: Arc<dyn AccountService>,
    rebac_service: Arc<dyn RebacService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceAccountResolver for ResourceAccountResolverImpl {
    async fn resolve_target_account(
        &self,
        principal: &Principal,
        selector: Option<&ResourceOwnerRef>,
    ) -> Result<odf::ResourceOwnerHandle, ResolveManifestAccountError> {
        let target_account = self.resolve_account(principal, selector).await?;
        self.ensure_can_use_account_resources(principal, &target_account)
            .await?;
        Ok(target_account)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceAccountResolverImpl {
    async fn resolve_account(
        &self,
        principal: &Principal,
        selector: Option<&ResourceOwnerRef>,
    ) -> Result<odf::ResourceOwnerHandle, ResolveManifestAccountError> {
        let Some(selector) = selector else {
            return Self::resolve_principal_account(principal);
        };

        let account = match (&selector.did, &selector.name) {
            (Some(did), _) => self
                .account_service
                .get_account_by_id(did)
                .await
                .map_err(ResolveManifestAccountError::from)?,
            (None, Some(name)) => self
                .account_service
                .get_account_by_name(name)
                .await
                .map_err(ResolveManifestAccountError::from)?,
            (None, None) => return Err(ResolveManifestAccountError::empty_selector()),
        };

        let resource_id_matches = selector
            .id
            .as_ref()
            .is_none_or(|id| *id == account.resource_id);
        let did_matches = selector.did.as_ref().is_none_or(|did| *did == account.id);
        let name_matches = selector
            .name
            .as_ref()
            .is_none_or(|name| *name == account.account_name);

        if !resource_id_matches || !did_matches || !name_matches {
            return Err(ResolveManifestAccountError::selector_mismatch(
                &account.id,
                &account.account_name,
                selector.id,
                selector.did.as_ref(),
                selector.name.as_ref(),
            ));
        }

        Ok(AccountHandle::from(&account).into())
    }

    /// The account a manifest without a selector is placed in: the principal's
    /// own; the node has none, so it must name one
    fn resolve_principal_account(
        principal: &Principal,
    ) -> Result<odf::ResourceOwnerHandle, ResolveManifestAccountError> {
        match principal {
            Principal::User(account) => Ok(account.into()),
            Principal::Node => Err(ResolveManifestAccountError::empty_selector()),
            Principal::Anonymous => Err(ResourceAccountAccessError::AnonymousSubject.into()),
        }
    }

    async fn ensure_can_use_account_resources(
        &self,
        principal: &Principal,
        target_account: &odf::ResourceOwnerHandle,
    ) -> Result<(), ResolveManifestAccountError> {
        match principal {
            Principal::Node => Ok(()),
            Principal::User(account_handle) if account_handle.did == target_account.did => Ok(()),
            Principal::User(account_handle) => {
                let is_admin = self
                    .rebac_service
                    .is_account_admin(&account_handle.did)
                    .await
                    .int_err()?;
                if is_admin {
                    Ok(())
                } else {
                    Err(
                        ResourceAccountAccessError::Access(odf::AccessError::Unauthorized(
                            format!(
                                "Current subject is not allowed to use resources of account '{}'",
                                target_account.name
                            )
                            .into(),
                        ))
                        .into(),
                    )
                }
            }
            Principal::Anonymous => Err(ResourceAccountAccessError::AnonymousSubject.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
