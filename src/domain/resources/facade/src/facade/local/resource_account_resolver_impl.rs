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
use kamu_accounts::{AccountService, CurrentAccountSubject};
use kamu_auth_rebac::{RebacService, RebacServiceExt};
use kamu_resources::ResourceAccountRef;

use crate::{ResolveManifestAccountError, ResourceAccountResolver};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ResourceAccountResolver)]
pub(crate) struct ResourceAccountResolverImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    account_service: Arc<dyn AccountService>,
    rebac_service: Arc<dyn RebacService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceAccountResolver for ResourceAccountResolverImpl {
    async fn resolve_target_account(
        &self,
        selector: Option<&ResourceAccountRef>,
    ) -> Result<odf::AccountHandle, ResolveManifestAccountError> {
        let target_account = self.resolve_account(selector).await?;
        self.ensure_can_use_account_resources(&target_account)
            .await?;
        Ok(target_account)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceAccountResolverImpl {
    async fn resolve_account(
        &self,
        selector: Option<&ResourceAccountRef>,
    ) -> Result<odf::AccountHandle, ResolveManifestAccountError> {
        let Some(selector) = selector else {
            return self.resolve_current_subject_account();
        };

        match selector {
            ResourceAccountRef::Id(id) => {
                let account = self
                    .account_service
                    .get_account_by_id(id)
                    .await
                    .map_err(ResolveManifestAccountError::from)?;

                Ok((&account).into())
            }
            ResourceAccountRef::Name(name) => {
                let account = self
                    .account_service
                    .get_account_by_name(name)
                    .await
                    .map_err(ResolveManifestAccountError::from)?;

                Ok((&account).into())
            }
            ResourceAccountRef::IdAndName(odf::metadata::auth::AccountRefByIdAndName {
                did,
                name,
            }) => {
                let account = self
                    .account_service
                    .get_account_by_id(did)
                    .await
                    .map_err(ResolveManifestAccountError::from)?;

                if account.account_name != *name {
                    return Err(ResolveManifestAccountError::IdNameMismatch {
                        account_id: account.id,
                        expected_name: name.clone(),
                        actual_name: account.account_name,
                    });
                }

                Ok((&account).into())
            }
        }
    }

    fn resolve_current_subject_account(
        &self,
    ) -> Result<odf::AccountHandle, ResolveManifestAccountError> {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(account) => Ok(account.account_handle.clone()),
            CurrentAccountSubject::Anonymous(_) => {
                Err(ResolveManifestAccountError::AnonymousSubject)
            }
        }
    }

    async fn ensure_can_use_account_resources(
        &self,
        target_account: &odf::AccountHandle,
    ) -> Result<(), ResolveManifestAccountError> {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(current)
                if current.account_handle.did == target_account.did =>
            {
                Ok(())
            }
            CurrentAccountSubject::Logged(current) => {
                let is_admin = self
                    .rebac_service
                    .is_account_admin(&current.account_handle.did)
                    .await
                    .int_err()?;
                if is_admin {
                    Ok(())
                } else {
                    Err(ResolveManifestAccountError::Access(
                        odf::AccessError::Unauthorized(
                            format!(
                                "Current subject is not allowed to use resources of account '{}'",
                                target_account.name
                            )
                            .into(),
                        ),
                    ))
                }
            }
            CurrentAccountSubject::Anonymous(_) => {
                Err(ResolveManifestAccountError::AnonymousSubject)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
