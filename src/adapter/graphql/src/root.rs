// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::auth::GetAccountInfoError;
use kamu_core::{auth, CurrentAccountSubject};

use crate::extensions::*;
use crate::mutations::*;
use crate::prelude::*;
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
    /// Authentication and authorization-related functionality group
    async fn auth(&self) -> AuthMut {
        AuthMut
    }

    /// Dataset-related functionality group.
    ///
    /// Datasets are historical streams of events recorded under a cetrain
    /// schema.
    async fn datasets(&self) -> DatasetsMut {
        DatasetsMut
    }

    /// Tasks-related functionality group.
    ///
    /// Tasks are units of work scheduled and executed by the system to query
    /// and process data.
    async fn tasks(&self) -> TasksMut {
        TasksMut
    }
}

#[derive(Clone, Debug)]
pub struct AccessToken {
    pub token: String,
}

impl AccessToken {
    pub fn new<S>(token: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            token: token.into(),
        }
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

/// Executes GraphQL query
pub async fn execute_query(
    schema: Schema,
    base_catalog: dill::Catalog,
    maybe_access_token: Option<AccessToken>,
    req: impl Into<Request>,
) -> async_graphql::Response {
    let graphql_request = req.into();

    let current_account_subject =
        match current_account_subject(&base_catalog, maybe_access_token).await {
            Ok(current_account_subject) => current_account_subject,
            Err(response) => return response,
        };

    let graphql_catalog = dill::CatalogBuilder::new_chained(&base_catalog)
        .add_value(current_account_subject)
        .build();

    let graphql_request = graphql_request.data(graphql_catalog);

    schema.execute(graphql_request).await
}

async fn current_account_subject(
    base_catalog: &dill::Catalog,
    maybe_access_token: Option<AccessToken>,
) -> Result<CurrentAccountSubject, Response> {
    if let Some(access_token) = maybe_access_token {
        let authentication_service = base_catalog
            .get_one::<dyn auth::AuthenticationService>()
            .unwrap();

        match authentication_service
            .get_account_info(access_token.token)
            .await
        {
            Ok(account_info) => Ok(CurrentAccountSubject::new(account_info.account_name, false)),
            Err(GetAccountInfoError::AccessToken(_)) => Ok(CurrentAccountSubject::new(
                opendatafabric::AccountName::new_unchecked(auth::ANONYMOUS_ACCOUNT_NAME),
                true,
            )),
            Err(e) => {
                return Err(async_graphql::Response::from_errors(vec![
                    async_graphql::ServerError::new(e.to_string(), None),
                ]))
            }
        }
    } else {
        Ok(CurrentAccountSubject::new(
            opendatafabric::AccountName::new_unchecked(auth::ANONYMOUS_ACCOUNT_NAME),
            true,
        ))
    }
}
