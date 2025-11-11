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

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Query {
    /// Returns the version of the GQL API
    async fn api_version(&self) -> String {
        "1".to_string()
    }

    /// Returns server's version and build configuration information
    async fn build_info(&self, ctx: &Context<'_>) -> BuildInfo {
        BuildInfo::new(ctx)
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

    /// Webhook-related functionality group
    ///
    /// Webhooks are used to send notifications about events happening in the
    /// system. This groups deals with their management and subscriptions.
    async fn webhooks(&self) -> Webhooks {
        Webhooks
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

    // Federation:
    // - These methods must be in Query root.
    // - Methods won't be shown in the GQL schema.

    #[graphql(entity)]
    #[tracing::instrument(level = "info", name = Query_find_account_by_id, skip_all, fields(%id))]
    async fn find_account_by_id(
        &self,
        ctx: &Context<'_>,
        id: AccountID<'_>,
    ) -> Result<Option<Account>> {
        // NOTE: DataLoader-friendly
        Accounts.by_id(ctx, id).await
    }

    #[graphql(entity)]
    #[tracing::instrument(level = "info", name = Query_find_dataset_by_id, skip_all, fields(%id))]
    async fn find_dataset_by_id(
        &self,
        ctx: &Context<'_>,
        id: DatasetID<'_>,
    ) -> Result<Option<Dataset>> {
        // NOTE: DataLoader-friendly
        Datasets.by_id(ctx, id).await
    }

    #[graphql(entity)]
    #[tracing::instrument(level = "info", name = Query_find_versioned_file_entry_by_id, skip_all, fields(%id))]
    async fn find_versioned_file_entry_by_id(
        &self,
        ctx: &Context<'_>,
        id: DatasetID<'_>,
    ) -> Result<Option<VersionedFileEntry>> {
        let Some(dataset) = Datasets.by_id(ctx, id).await? else {
            return Ok(None);
        };
        // TODO: omit this check?
        let Some(versioned_file) = dataset.as_versioned_file(ctx).await? else {
            // TODO: error?
            return Ok(None);
        };

        // TODO: version? hash?
        versioned_file.get_entry(ctx, None, None).await
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

    /// Temporary: Molecule-specific functionality group
    async fn molecule(&self) -> MoleculeMut {
        MoleculeMut
    }

    /// Collaboration-related functionality group
    ///
    /// Allows setting permissions for multiple datasets in batch mode
    async fn collaboration(&self) -> CollaborationMut {
        CollaborationMut
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        .enable_federation()
        .finish()
}

/// Returns schema preconfigured schema without apollo tracing extension
pub fn schema_quiet() -> Schema {
    schema_builder()
        .extension(Tracing)
        .enable_federation()
        .finish()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
