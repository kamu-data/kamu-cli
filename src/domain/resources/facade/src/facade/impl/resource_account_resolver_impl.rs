// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{AccountService, CurrentAccountSubject};
use kamu_auth_rebac::{RebacService, RebacServiceExt};
use kamu_resources::ResourceManifestAccount;

use crate::{ResolveManifestAccountError, ResolvedAccount, ResourceAccountResolver, ResourceView};

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
        selector: Option<&ResourceManifestAccount>,
    ) -> Result<ResolvedAccount, ResolveManifestAccountError> {
        let target_account = self.resolve_account(selector).await?;
        self.ensure_can_use_account_resources(&target_account)
            .await?;
        Ok(target_account)
    }

    async fn hydrate_resource_view_account(
        &self,
        mut view: ResourceView,
        target_account: Option<&ResolvedAccount>,
    ) -> Result<ResourceView, InternalError> {
        if let Some(account) = target_account
            && view.account.id == account.id
        {
            view.account.name = Some(account.name.clone());
            return Ok(view);
        }

        if view.account.name.is_none() {
            let maybe_name = self
                .account_service
                .find_account_name_by_id(&view.account.id)
                .await?;

            let Some(account_name) = maybe_name else {
                return Err(InternalError::new(format!(
                    "Account {} not found while hydrating resource view",
                    view.account.id
                )));
            };

            view.account.name = Some(account_name);
        }

        Ok(view)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceAccountResolverImpl {
    async fn resolve_account(
        &self,
        selector: Option<&ResourceManifestAccount>,
    ) -> Result<ResolvedAccount, ResolveManifestAccountError> {
        let Some(selector) = selector else {
            return self.resolve_current_subject_account();
        };

        match (&selector.id, &selector.name) {
            (Some(id), maybe_name) => {
                let account_id = odf::AccountID::from_did_str(id)
                    .or_else(|_| odf::AccountID::parse_id_without_did_prefix(id))
                    .map_err(ResolveManifestAccountError::InvalidAccountId)?;
                let account = self
                    .account_service
                    .get_account_by_id(&account_id)
                    .await
                    .map_err(ResolveManifestAccountError::from)?;

                if let Some(expected_name) = maybe_name {
                    let expected_name = odf::AccountName::new_unchecked(expected_name);
                    if account.account_name != expected_name {
                        return Err(ResolveManifestAccountError::IdNameMismatch {
                            account_id: account.id,
                            expected_name,
                            actual_name: account.account_name,
                        });
                    }
                }

                Ok(ResolvedAccount {
                    id: account.id,
                    name: account.account_name,
                })
            }
            (None, Some(name)) => {
                let account_name = odf::AccountName::new_unchecked(name);
                let account = self
                    .account_service
                    .get_account_by_name(&account_name)
                    .await
                    .map_err(ResolveManifestAccountError::from)?;

                Ok(ResolvedAccount {
                    id: account.id,
                    name: account.account_name,
                })
            }
            (None, None) => Err(ResolveManifestAccountError::EmptySelector),
        }
    }

    fn resolve_current_subject_account(
        &self,
    ) -> Result<ResolvedAccount, ResolveManifestAccountError> {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(account) => Ok(ResolvedAccount {
                id: account.account_id.clone(),
                name: account.account_name.clone(),
            }),
            CurrentAccountSubject::Anonymous(_) => {
                Err(ResolveManifestAccountError::AnonymousSubject)
            }
        }
    }

    async fn ensure_can_use_account_resources(
        &self,
        target_account: &ResolvedAccount,
    ) -> Result<(), ResolveManifestAccountError> {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(current) if current.account_id == target_account.id => {
                Ok(())
            }
            CurrentAccountSubject::Logged(current) => {
                let is_admin = self
                    .rebac_service
                    .is_account_admin(&current.account_id)
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
