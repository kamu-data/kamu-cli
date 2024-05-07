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
use kamu_accounts::{CurrentAccountSubject, LoggedAccount};
use kamu_core::auth::DatasetActionUnauthorizedError;
use kamu_core::{Dataset, DatasetRepository};
use kamu_task_system as ts;
use opendatafabric::{AccountName as OdfAccountName, DatasetHandle};

use crate::prelude::{AccountID, AccountName};

///////////////////////////////////////////////////////////////////////////////

// TODO: Return gql-specific error and get rid of unwraps
pub(crate) fn from_catalog<T>(ctx: &Context<'_>) -> Result<Arc<T>, dill::InjectionError>
where
    T: ?Sized + Send + Sync + 'static,
{
    let cat = ctx.data::<dill::Catalog>().unwrap();
    cat.get_one::<T>()
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_dataset(
    ctx: &Context<'_>,
    dataset_handle: &DatasetHandle,
) -> Result<Arc<dyn Dataset>, InternalError> {
    let dataset_repo = from_catalog::<dyn DatasetRepository>(ctx).unwrap();
    let dataset = dataset_repo
        .get_dataset(&dataset_handle.as_local_ref())
        .await
        .int_err()?;
    Ok(dataset)
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) fn get_logged_account(ctx: &Context<'_>) -> LoggedAccount {
    let current_account_subject = from_catalog::<CurrentAccountSubject>(ctx).unwrap();
    match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(la) => la.clone(),
        CurrentAccountSubject::Anonymous(_) => {
            unreachable!("We are not expecting anonymous accounts")
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

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
            DatasetActionUnauthorizedError::Access(_) => GqlError::Gql(
                async_graphql::Error::new("Dataset access error")
                    .extend_with(|_, eev| eev.set("alias", dataset_handle.alias.to_string())),
            ),
            DatasetActionUnauthorizedError::Internal(e) => GqlError::Internal(e),
        })?;

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_task(
    ctx: &Context<'_>,
    task_id: ts::TaskID,
) -> Result<ts::TaskState, InternalError> {
    let task_scheduler = from_catalog::<dyn ts::TaskScheduler>(ctx).unwrap();
    task_scheduler.get_task(task_id).await.int_err()
}

///////////////////////////////////////////////////////////////////////////////

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
        async_graphql::Error::new("Account datasets access error")
            .extend_with(|_, eev| eev.set("account_id", account_id.to_string())),
    ))
}

///////////////////////////////////////////////////////////////////////////////

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
        async_graphql::Error::new("Account datasets access error")
            .extend_with(|_, eev| eev.set("account_name", account_name.to_string())),
    ))
}

///////////////////////////////////////////////////////////////////////////////

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
