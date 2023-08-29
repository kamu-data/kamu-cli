// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_graphql::Context;
use internal_error::*;

use crate::AccessToken;

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

pub(crate) fn extract_access_token(ctx: &Context<'_>) -> Option<AccessToken> {
    match ctx.data::<AccessToken>() {
        Ok(access_token) => Some(access_token.clone()),
        Err(_) => None,
    }
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

impl Into<async_graphql::Error> for GqlError {
    fn into(self) -> async_graphql::Error {
        match self {
            Self::Internal(err) => async_graphql::Error::new_with_source(err),
            Self::Gql(err) => err,
        }
    }
}
