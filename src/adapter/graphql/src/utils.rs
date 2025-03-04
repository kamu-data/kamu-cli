// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{Context, ErrorExtensions};
use internal_error::*;
use kamu_accounts::{CurrentAccountSubject, GetAccessTokenError, LoggedAccount};
use kamu_core::auth::DatasetActionUnauthorizedError;
use kamu_core::{auth, DatasetRegistry, ResolvedDataset};
use kamu_datasets::DatasetEnvVarsConfig;
use kamu_task_system as ts;

use crate::prelude::{AccessTokenID, AccountID, AccountName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `unwrap()`-free helper macro to hide the logic for extracting DI components
/// from a catalog using [`async_graphql::Context`] that is present for each GQL
/// request.
///
/// If one of the required DI components is not found, `.int_err()?` will be
/// initiated.
///
/// There is also a variant of the macro for exceptional situations that uses
/// `unwrap()` internally: [`unsafe_from_catalog_n!`].
///
/// # Examples
///
/// ```
/// // Most often, we extract only one component:
/// let current_account_subject = from_catalog_n!(ctx, CurrentAccountSubject);
///
/// // But sometimes, three at once:
/// let (dataset_registry, polling_ingest_svc, dataset_changes_svc) = from_catalog_n!(
///     ctx,
///     dyn DatasetRegistry,
///     dyn PollingIngestService,
///     dyn DatasetChangesService
/// );
/// ```
macro_rules! from_catalog_n {
    ($gql_ctx:ident, $T:ty ) => {{
        let catalog = $gql_ctx.data::<dill::Catalog>().unwrap();

        catalog.get_one::<$T>().unwrap()
    }};
    ($gql_ctx:ident, $T:ty, $($Ts:ty),+) => {{
        let catalog = $gql_ctx.data::<dill::Catalog>().unwrap();

        ( catalog.get_one::<$T>().unwrap(), $( catalog.get_one::<$Ts>().unwrap() ),+ )
    }};
}

pub(crate) use from_catalog_n;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Unsafe variant of [`from_catalog_n!`] macro.
///
/// Try to avoid using it.
macro_rules! unsafe_from_catalog_n {
    ($gql_ctx:ident, $T:ty ) => {{
        let catalog = $gql_ctx.data::<dill::Catalog>().unwrap();

        catalog.get_one::<$T>().unwrap()
    }};
    ($gql_ctx:ident, $T:ty, $($Ts:ty),+) => {{
        let catalog = $gql_ctx.data::<dill::Catalog>().unwrap();

        ( catalog.get_one::<$T>().unwrap(), $( catalog.get_one::<$Ts>().unwrap() ),+ )
    }};
}

pub(crate) use unsafe_from_catalog_n;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_dataset(
    ctx: &Context<'_>,
    dataset_handle: &odf::DatasetHandle,
) -> ResolvedDataset {
    let dataset_registry = from_catalog_n!(ctx, dyn DatasetRegistry);
    dataset_registry.get_dataset_by_handle(dataset_handle).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn get_logged_account(ctx: &Context<'_>) -> LoggedAccount {
    let current_account_subject = from_catalog_n!(ctx, CurrentAccountSubject);

    match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(la) => la.clone(),
        CurrentAccountSubject::Anonymous(_) => {
            unreachable!("We are not expecting anonymous accounts")
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Private Datasets: migrate to the DatasetState usage
#[deprecated(note = "use DatasetState::check_dataset_read_access()")]
pub(crate) async fn check_dataset_read_access(
    ctx: &Context<'_>,
    dataset_handle: &odf::DatasetHandle,
) -> Result<(), GqlError> {
    check_dataset_access(ctx, dataset_handle, auth::DatasetAction::Read).await
}

#[deprecated(note = "use DatasetState::check_dataset_write_access()")]
pub(crate) async fn check_dataset_write_access(
    ctx: &Context<'_>,
    dataset_handle: &odf::DatasetHandle,
) -> Result<(), GqlError> {
    check_dataset_access(ctx, dataset_handle, auth::DatasetAction::Write).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn check_dataset_access(
    ctx: &Context<'_>,
    dataset_handle: &odf::DatasetHandle,
    action: auth::DatasetAction,
) -> Result<(), GqlError> {
    let dataset_action_authorizer = from_catalog_n!(ctx, dyn auth::DatasetActionAuthorizer);

    dataset_action_authorizer
        .check_action_allowed(&dataset_handle.id, action)
        .await
        .map_err(|e| match e {
            DatasetActionUnauthorizedError::Access(_) => make_dataset_access_error(dataset_handle),
            DatasetActionUnauthorizedError::Internal(e) => GqlError::Internal(e),
        })?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_dataset_access_error(dataset_handle: &odf::DatasetHandle) -> GqlError {
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
    let task_event_store = from_catalog_n!(ctx, dyn ts::TaskEventStore);
    let task = ts::Task::load(task_id, task_event_store.as_ref())
        .await
        .int_err()?;
    Ok(task.into())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn ensure_dataset_env_vars_enabled(ctx: &Context<'_>) -> Result<(), GqlError> {
    let dataset_env_vars_config = from_catalog_n!(ctx, DatasetEnvVarsConfig);
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
    let current_account_subject = from_catalog_n!(ctx, CurrentAccountSubject);

    if let CurrentAccountSubject::Logged(logged_account) = current_account_subject.as_ref() {
        if logged_account.account_id == **account_id {
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
    token_id: &AccessTokenID<'_>,
) -> Result<(), GqlError> {
    let (current_account_subject, access_token_service) = from_catalog_n!(
        ctx,
        CurrentAccountSubject,
        dyn kamu_accounts::AccessTokenService
    );

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
        if logged_account.account_id == existing_access_token.account_id {
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
    let current_account_subject = from_catalog_n!(ctx, CurrentAccountSubject);

    if let CurrentAccountSubject::Logged(logged_account) = current_account_subject.as_ref() {
        if logged_account.account_name == **account_name {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
