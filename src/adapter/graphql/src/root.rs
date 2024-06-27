// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::extensions::*;
use crate::mutations::*;
use crate::prelude::*;
use crate::queries::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Query
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Query;

#[Object]
impl Query {
    /// Returns the version of the GQL API
    async fn api_version(&self) -> String {
        "0.1".to_string()
    }

    /// Authentication and authorization-related functionality group
    async fn auth(&self) -> Auth {
        Auth
    }

    /// Dataset-related functionality group.
    ///
    /// Datasets are historical streams of events recorded under a certain
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

    /// Search-related functionality group
    async fn search(&self) -> Search {
        Search
    }

    /// Querying and data manipulations
    async fn data(&self) -> DataQueries {
        DataQueries
    }

    /// Admin-related functionality group
    async fn admin(&self) -> Admin {
        Admin
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Mutation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Mutation;

#[Object]
impl Mutation {
    /// Authentication and authorization-related functionality group
    async fn auth(&self) -> AuthMut {
        AuthMut
    }

    /// Dataset-related functionality group.
    ///
    /// Datasets are historical streams of events recorded under a certain
    /// schema.
    async fn datasets(&self) -> DatasetsMut {
        DatasetsMut
    }

    /// Account-related functionality group.
    ///
    /// Accounts can be individual users or organizations registered in the
    /// system. This groups deals with their identities and permissions.
    async fn accounts(&self) -> AccountsMut {
        AccountsMut
    }

    /// Tasks-related functionality group.
    ///
    /// Tasks are units of work scheduled and executed by the system to query
    /// and process data.
    async fn tasks(&self) -> TasksMut {
        TasksMut
    }
}

pub type Schema = async_graphql::Schema<Query, Mutation, EmptySubscription>;
pub type SchemaBuilder = async_graphql::SchemaBuilder<Query, Mutation, EmptySubscription>;

/// Returns schema builder without any extensions
pub fn schema_builder() -> SchemaBuilder {
    Schema::build(Query, Mutation, EmptySubscription)
}

/// Returns schema preconfigured with default extensions
pub fn schema() -> Schema {
    schema_builder()
        .extension(Tracing)
        .extension(extensions::ApolloTracing)
        .finish()
}

/// Returns schema preconfigured schema without tracing extensions
pub fn schema_quiet() -> Schema {
    schema_builder().finish()
}
