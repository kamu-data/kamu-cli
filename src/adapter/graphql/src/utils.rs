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

// TODO: Return gql-specific error and get rid of unwraps
pub(crate) fn from_catalog<T>(ctx: &Context<'_>) -> Result<Arc<T>, dill::InjectionError>
where
    T: ?Sized + Send + Sync + 'static,
{
    let cat = ctx.data::<dill::Catalog>().unwrap();
    cat.get_one::<T>()
}
