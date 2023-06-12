// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;

use crate::mutations::*;
use crate::queries::*;

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

    /// Dataset-related functionality group.
    ///
    /// Datasets are historical streams of events recorded under a cetrain
    /// schema.
    async fn datasets(&self) -> Datasets {
        Datasets
    }

    /// Account-related functionality group.
    ///
    /// Accounts can be individual users or organizations registered in the
    /// system. This groups deals with their identities and permissions.
    async fn accounts(&self) -> Accounts {
        Accounts
    }

    /// Task-related functionality group.
    ///
    /// Tasks are units of scheduling that can perform many functions like
    /// ingesting new data, running dataset transformations, answering ad-hoc
    /// queries etc.
    async fn tasks(&self) -> Tasks {
        Tasks
    }

    /// Search-related functionality group.
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
        .extension(extensions::Tracing)
        .data(catalog)
        .finish()
}
