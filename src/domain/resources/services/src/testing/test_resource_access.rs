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
use kamu_accounts::{AccountHandle, AccountService};
use kamu_resources::{
    DeclarativeResource,
    ResourceAccountAccess,
    ResourceAccountOperation,
    ResourceIDNotFoundError,
    ResourceOperation,
    ResourceOperationError,
    ResourceOperationGuard,
    ResourceOperationRequest,
    ResourcePlacement,
    ResourcePlacementOutcome,
};

use crate::DefaultResourceOperationGuard;
use crate::testing::{TestResource, TestResourceSpec};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Registers [`TestResourcePlacement`] and [`TestResourceOperationGuard`],
/// which grant access by tokens of the spec value rather than by the account
pub fn register_test_resource_access_rules(catalog_builder: &mut dill::CatalogBuilder) {
    catalog_builder.add::<TestResourcePlacement>();
    catalog_builder.add::<TestResourceOperationGuard>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Places a resource whose value has a `place:<account>` token in that account
#[dill::component(pub)]
#[dill::interface(dyn ResourcePlacement<TestResource>)]
pub struct TestResourcePlacement {
    account_service: Arc<dyn AccountService>,
}

#[async_trait::async_trait]
impl ResourcePlacement<TestResource> for TestResourcePlacement {
    async fn place(
        &self,
        _principal: &odf::metadata::auth::PrincipalHandle,
        spec: &TestResourceSpec,
    ) -> Result<ResourcePlacementOutcome, InternalError> {
        let Some(name) = token_values(spec, "place").next() else {
            return Ok(ResourcePlacementOutcome::Default);
        };

        let account = self
            .account_service
            .get_account_by_name(&odf::AccountName::new_unchecked(name))
            .await
            .int_err()?;
        Ok(ResourcePlacementOutcome::Account(
            AccountHandle::from(&account).into(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The default rule, widened by tokens of the spec value:
///
/// | Operation | Also allowed for |
/// | --- | --- |
/// | Create, Update | Anyone, in the account the spec places the resource in |
/// | Read | A principal named by a `read:` or `delete:` token |
/// | Delete | A principal named by a `delete:` token |
/// | List | Anyone, with a name pattern |
#[dill::component(pub)]
#[dill::interface(dyn ResourceOperationGuard<TestResource>)]
pub struct TestResourceOperationGuard {
    default_guard: Arc<DefaultResourceOperationGuard>,
}

impl TestResourceOperationGuard {
    fn is_granted(
        principal: &odf::metadata::auth::PrincipalHandle,
        resource: &TestResource,
        tokens: &[&str],
    ) -> bool {
        tokens.iter().any(|token| {
            token_values(resource.spec(), token).any(|name| name == principal.name.as_str())
        })
    }
}

#[async_trait::async_trait]
impl ResourceOperationGuard<TestResource> for TestResourceOperationGuard {
    async fn check_account(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        account: &odf::ResourceOwnerHandle,
        _operation: ResourceAccountOperation,
    ) -> Result<ResourceAccountAccess, ResourceOperationError> {
        match self
            .default_guard
            .check_account_rule(principal, account)
            .await
        {
            Ok(()) => Ok(ResourceAccountAccess::Allowed),
            Err(ResourceOperationError::Access(_) | ResourceOperationError::NotFound(_)) => {
                Ok(ResourceAccountAccess::PerResource)
            }
            Err(err @ ResourceOperationError::Internal(_)) => Err(err),
        }
    }

    async fn check_operation(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        request: &ResourceOperationRequest<'_, TestResource>,
    ) -> Result<(), ResourceOperationError> {
        let denial = match self
            .default_guard
            .check_account_rule(principal, request.account)
            .await
        {
            Ok(()) => return Ok(()),
            Err(err @ ResourceOperationError::Internal(_)) => return Err(err),
            Err(
                err @ (ResourceOperationError::Access(_) | ResourceOperationError::NotFound(_)),
            ) => err,
        };

        let hidden = |resource: &TestResource| {
            ResourceOperationError::NotFound(ResourceIDNotFoundError(resource.headers().id))
        };

        match &request.operation {
            ResourceOperation::Create { spec }
            | ResourceOperation::Update { new_spec: spec, .. } => {
                if token_values(spec, "place").any(|name| name == request.account.name.as_str()) {
                    Ok(())
                } else {
                    Err(denial)
                }
            }
            ResourceOperation::Read { resource } => {
                if Self::is_granted(principal, resource, &["read", "delete"]) {
                    Ok(())
                } else {
                    Err(hidden(resource))
                }
            }
            ResourceOperation::Delete { resource } => {
                if Self::is_granted(principal, resource, &["delete"]) {
                    Ok(())
                } else if Self::is_granted(principal, resource, &["read"]) {
                    Err(denial)
                } else {
                    Err(hidden(resource))
                }
            }
            ResourceOperation::List { scope } => {
                if scope.name_pattern.is_some() {
                    Ok(())
                } else {
                    Err(denial)
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The values of the `<token>:<value>` words of the spec value
fn token_values<'a>(spec: &'a TestResourceSpec, token: &'a str) -> impl Iterator<Item = &'a str> {
    spec.value
        .split_whitespace()
        .filter_map(move |word| word.strip_prefix(token)?.strip_prefix(':'))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
