// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_graphql::{Context, ErrorExtensions};
use internal_error::*;
use kamu_accounts::{CurrentAccountSubject, GetAccessTokenError, LoggedAccount};
use kamu_core::auth::DatasetActionUnauthorizedError;
use kamu_core::{Dataset, DatasetRepository};
use kamu_datasets::DatasetEnvVarsConfig;
use kamu_task_system as ts;
use opendatafabric::{AccountName as OdfAccountName, DatasetHandle};

use crate::prelude::{AccessTokenID, AccountID, AccountName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Return gql-specific error and get rid of unwraps
pub(crate) fn from_catalog<T>(ctx: &Context<'_>) -> Result<Arc<T>, dill::InjectionError>
where
    T: ?Sized + Send + Sync + 'static,
{
    let cat = ctx.data::<dill::Catalog>().unwrap();
    cat.get_one::<T>()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn get_dataset(ctx: &Context<'_>, dataset_handle: &DatasetHandle) -> Arc<dyn Dataset> {
    let dataset_repo = from_catalog::<dyn DatasetRepository>(ctx).unwrap();
    dataset_repo.get_dataset_by_handle(dataset_handle)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn get_logged_account(ctx: &Context<'_>) -> LoggedAccount {
    let current_account_subject = from_catalog::<CurrentAccountSubject>(ctx).unwrap();
    match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(la) => la.clone(),
        CurrentAccountSubject::Anonymous(_) => {
            unreachable!("We are not expecting anonymous accounts")
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn check_dataset_read_access(
    ctx: &Context<'_>,
    dataset_handle: &DatasetHandle,
) -> Result<(), GqlError> {
    let dataset_action_authorizer =
        from_catalog::<dyn kamu_core::auth::DatasetActionAuthorizer>(ctx).int_err()?;

    dataset_action_authorizer
        .check_action_allowed(dataset_handle, kamu_core::auth::DatasetAction::Read)
        .await
        .map_err(|e| match e {
            DatasetActionUnauthorizedError::Access(_) => GqlError::Gql(
                async_graphql::Error::new("Dataset access error")
                    .extend_with(|_, eev| eev.set("alias", dataset_handle.alias.to_string())),
            ),
            DatasetActionUnauthorizedError::Internal(e) => GqlError::Internal(e),
        })?;

    Ok(())
}

pub(crate) async fn check_dataset_write_access(
    ctx: &Context<'_>,
    dataset_handle: &DatasetHandle,
) -> Result<(), GqlError> {
    let dataset_action_authorizer =
        from_catalog::<dyn kamu_core::auth::DatasetActionAuthorizer>(ctx).int_err()?;

    dataset_action_authorizer
        .check_action_allowed(dataset_handle, kamu_core::auth::DatasetAction::Write)
        .await
        .map_err(|e| match e {
            DatasetActionUnauthorizedError::Access(_) => make_dataset_access_error(dataset_handle),
            DatasetActionUnauthorizedError::Internal(e) => GqlError::Internal(e),
        })?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_dataset_access_error(dataset_handle: &DatasetHandle) -> GqlError {
    GqlError::Gql(
        async_graphql::Error::new("Dataset access error")
            .extend_with(|_, eev| eev.set("alias", dataset_handle.alias.to_string())),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_task(
    ctx: &Context<'_>,
    task_id: ts::TaskID,
) -> Result<ts::TaskState, InternalError> {
    let task_event_store = from_catalog::<dyn ts::TaskEventStore>(ctx).unwrap();
    let task = ts::Task::load(task_id, task_event_store.as_ref())
        .await
        .int_err()?;
    Ok(task.into())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn ensure_dataset_env_vars_enabled(ctx: &Context<'_>) -> Result<(), GqlError> {
    let dataset_env_vars_config = from_catalog::<DatasetEnvVarsConfig>(ctx).unwrap();
    if !dataset_env_vars_config.as_ref().is_enabled() {
        return Err(GqlError::Gql(async_graphql::Error::new(
            "API is unavailable",
        )));
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn check_logged_account_id_match(
    ctx: &Context<'_>,
    account_id: &AccountID,
) -> Result<(), GqlError> {
    let current_account_subject = from_catalog::<CurrentAccountSubject>(ctx).unwrap();

    if let CurrentAccountSubject::Logged(logged_account) = current_account_subject.as_ref() {
        if logged_account.account_id == account_id.clone().into() {
            return Ok(());
        }
    };
    Err(GqlError::Gql(
        async_graphql::Error::new("Account access error")
            .extend_with(|_, eev| eev.set("account_id", account_id.to_string())),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn check_access_token_valid(
    ctx: &Context<'_>,
    token_id: &AccessTokenID,
) -> Result<(), GqlError> {
    let current_account_subject = from_catalog::<CurrentAccountSubject>(ctx).unwrap();
    let access_token_service = from_catalog::<dyn kamu_accounts::AccessTokenService>(ctx).unwrap();

    let existing_access_token = access_token_service
        .get_token_by_id(token_id)
        .await
        .map_err(|err| match err {
            GetAccessTokenError::NotFound(_) => {
                GqlError::Gql(async_graphql::Error::new("Access token not found"))
            }
            GetAccessTokenError::Internal(e) => GqlError::Internal(e),
        })?;

    if let CurrentAccountSubject::Logged(logged_account) = current_account_subject.as_ref() {
        if logged_account.account_id == existing_access_token.account_id.clone() {
            return Ok(());
        }
    };
    Err(GqlError::Gql(
        async_graphql::Error::new("Access token access error").extend_with(|_, eev| {
            eev.set("account_id", existing_access_token.account_id.to_string());
        }),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn check_logged_account_name_match(
    ctx: &Context<'_>,
    account_name: &AccountName,
) -> Result<(), GqlError> {
    let current_account_subject = from_catalog::<CurrentAccountSubject>(ctx).unwrap();

    if let CurrentAccountSubject::Logged(logged_account) = current_account_subject.as_ref() {
        if logged_account.account_name == OdfAccountName::from(account_name.clone()) {
            return Ok(());
        }
    };
    Err(GqlError::Gql(
        async_graphql::Error::new("Account access error")
            .extend_with(|_, eev| eev.set("account_name", account_name.to_string())),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This wrapper is unfortunately necessary because of poor error handling
/// strategy of async-graphql that:
///
/// - prevents ? operator from quietly wrapping any Display value in query
///   handler into an error thus putting us in danger of leaking sensitive info
///
/// - ensures that only `InternalError` can be returned via ? operator
///
/// - ensures that original error is preserved as `source` so it can be
///   inspected and logged by the tracing middleware

#[derive(Debug)]
pub enum GqlError {
    Internal(InternalError),
    Gql(async_graphql::Error),
}

impl From<InternalError> for GqlError {
    fn from(value: InternalError) -> Self {
        Self::Internal(value)
    }
}

impl From<async_graphql::Error> for GqlError {
    fn from(value: async_graphql::Error) -> Self {
        Self::Gql(value)
    }
}

impl From<GqlError> for async_graphql::Error {
    fn from(val: GqlError) -> Self {
        match val {
            GqlError::Internal(err) => async_graphql::Error::new_with_source(err),
            GqlError::Gql(err) => err,
        }
    }
}
