// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::*;
use crate::queries::*;
use crate::scalars::DataQueryErrorResult;

use async_graphql::*;

////////////////////////////////////////////////////////////////////////////////////////
// Query
////////////////////////////////////////////////////////////////////////////////////////

pub struct Query;

#[Object]
impl Query {
    /// Returns the version of the GQL API
    async fn api_version(&self) -> String {
        "0.1".to_string()
    }

    /// Dataset-related functionality group
    async fn datasets(&self) -> Datasets {
        Datasets
    }

    /// Account-related functionality group
    async fn accounts(&self) -> Accounts {
        Accounts
    }

    /// Search-related functionality group
    async fn search(&self) -> Search {
        Search
    }

    /// Querying and data manipulations
    async fn data(&self) -> DataQueries {
        DataQueries
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// Mutation
////////////////////////////////////////////////////////////////////////////////////////

pub struct Mutation;

#[Object]
impl Mutation {
    async fn auth(&self) -> Auth {
        Auth
    }
}

pub type Schema = async_graphql::Schema<Query, Mutation, EmptySubscription>;

pub fn schema(catalog: dill::Catalog) -> Schema {
    Schema::build(Query, Mutation, EmptySubscription)
        .extension(extensions::ApolloTracing)
        .register_output_type::<DataQueryErrorResult>()
        .data(catalog)
        .finish()
}
