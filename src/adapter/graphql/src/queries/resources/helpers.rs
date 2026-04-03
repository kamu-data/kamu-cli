// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::Context;
use database_common::PaginationOpts;

use crate::prelude::*;
use crate::queries::{
    Resource,
    ResourceConnection,
    ResourceKindInput,
    ResourceSelectorInput,
    ResourceSummary,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_resource(
    ctx: &Context<'_>,
    selector: ResourceSelectorInput,
    account: Option<kamu_resources::ResourceManifestAccount>,
) -> Result<Option<Resource>> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let ResourceSelectorInput {
        kind,
        api_version,
        resource_ref,
    } = selector;

    let kind = kind.into_resource_type();

    let resource = resource_facade
        .get(kamu_resources_facade::GetResourceRequest {
            kind,
            api_version,
            account,
            resource_ref: resource_ref.into(),
        })
        .await;

    match resource {
        Ok(resource) => Ok(Some(resource.into())),
        Err(
            kamu_resources_facade::GetResourceError::UIDNotFound(_)
            | kamu_resources_facade::GetResourceError::NameNotFound(_),
        ) => Ok(None),
        Err(kamu_resources_facade::GetResourceError::ApiVersionMismatch(error)) => {
            Err(GqlError::gql(error.to_string()))
        }
        Err(kamu_resources_facade::GetResourceError::KindMismatch(error)) => {
            Err(GqlError::gql(error.to_string()))
        }
        Err(kamu_resources_facade::GetResourceError::UnsupportedDescriptor(_)) => {
            Err(GqlError::gql("Unsupported resource kind"))
        }
        Err(kamu_resources_facade::GetResourceError::BadAccount(error)) => {
            Err(map_resolve_manifest_account_error(error))
        }
        Err(kamu_resources_facade::GetResourceError::Internal(error)) => Err(error.into()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn list_resources_connection(
    ctx: &Context<'_>,
    kind: ResourceKindInput,
    account: Option<kamu_resources::ResourceManifestAccount>,
    page: usize,
    per_page: usize,
) -> Result<ResourceConnection> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let items = resource_facade
        .list(kamu_resources_facade::ListResourcesRequest {
            kind: kind.into_resource_type(),
            account,
            pagination: PaginationOpts::from_page(page, per_page),
        })
        .await
        .map_err(map_list_resources_error)?;

    let total_count = items.len();
    let items = items.into_iter().map(ResourceSummary::from).collect();

    Ok(ResourceConnection::new(items, page, per_page, total_count))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn list_all_resources_connection(
    ctx: &Context<'_>,
    account: Option<kamu_resources::ResourceManifestAccount>,
    page: usize,
    per_page: usize,
) -> Result<ResourceConnection> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let items = resource_facade
        .list_all(kamu_resources_facade::ListAllResourcesRequest {
            account,
            pagination: PaginationOpts::from_page(page, per_page),
        })
        .await
        .map_err(map_list_all_resources_error)?;

    let total_count = items.len();
    let items = items.into_iter().map(ResourceSummary::from).collect();

    Ok(ResourceConnection::new(items, page, per_page, total_count))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_list_resources_error(error: kamu_resources_facade::ListResourcesError) -> GqlError {
    use kamu_resources_facade::ListResourcesError as E;

    match error {
        E::UnsupportedDescriptor(_) => GqlError::gql("Unsupported resource kind"),
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
        E::Internal(error) => error.into(),
    }
}

fn map_list_all_resources_error(error: kamu_resources_facade::ListAllResourcesError) -> GqlError {
    use kamu_resources_facade::ListAllResourcesError as E;

    match error {
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
        E::Internal(error) => error.into(),
    }
}

fn map_resolve_manifest_account_error(
    error: kamu_resources_facade::ResolveManifestAccountError,
) -> GqlError {
    use kamu_resources_facade::ResolveManifestAccountError as E;

    match error {
        E::AnonymousSubject => {
            unreachable!("GraphQL resource resolvers should enforce logged-in access")
        }
        E::EmptySelector
        | E::IdNameMismatch { .. }
        | E::AccountNotFoundById(_)
        | E::AccountNotFoundByName(_) => {
            unreachable!("GraphQL resource selectors should be validated before facade calls")
        }
        E::Access(error) => error.into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
