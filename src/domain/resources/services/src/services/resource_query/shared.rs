// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use internal_error::InternalError;
use tokio_stream::StreamExt;

use crate::domain::{
    DeclarativeResource,
    DeleteResourcesError,
    ResourceDescriptorProvider,
    ResourceID,
    ResourceMetadataInput,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    TypedResourceQueryError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn load_snapshot_by_id<R>(
    resource_repository: &dyn ResourceRepository,
    resource_id: &ResourceID,
) -> Result<ResourceSnapshot, TypedResourceQueryError>
where
    R: ResourceDescriptorProvider,
{
    let snapshot = resource_repository
        .find_resource_snapshot_by_id(resource_id)
        .await?
        .ok_or(TypedResourceQueryError::NotFound(*resource_id))?;

    if snapshot.kind != R::DESCRIPTOR.resource_type
        || snapshot.api_version != R::DESCRIPTOR.api_version
    {
        return Err(TypedResourceQueryError::TypeMismatch {
            resource_id: *resource_id,
            expected_kind: R::DESCRIPTOR.resource_type.to_string(),
            expected_api_version: R::DESCRIPTOR.api_version.to_string(),
            actual_kind: snapshot.kind,
            actual_api_version: snapshot.api_version,
        });
    }

    Ok(snapshot)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_snapshot_by_query<R>(
    resource_repository: &dyn ResourceRepository,
    resource_id: &ResourceID,
) -> Result<Option<ResourceSnapshot>, InternalError>
where
    R: ResourceDescriptorProvider,
{
    let query = ResourceRawEventQuery {
        kind: R::DESCRIPTOR.resource_type.to_string(),
        id: *resource_id,
    };

    resource_repository.find_resource_snapshot(&query).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn type_mismatch<R>(resource_snapshot: &ResourceSnapshot) -> TypedResourceQueryError
where
    R: ResourceDescriptorProvider,
{
    TypedResourceQueryError::TypeMismatch {
        resource_id: resource_snapshot.resource_id,
        expected_kind: R::DESCRIPTOR.resource_type.to_string(),
        expected_api_version: R::DESCRIPTOR.api_version.to_string(),
        actual_kind: resource_snapshot.kind.clone(),
        actual_api_version: resource_snapshot.api_version.clone(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn find_existing_id_by_name<R>(
    resource_repository: &dyn ResourceRepository,
    resource_id: Option<ResourceID>,
    metadata: &ResourceMetadataInput,
) -> Result<Option<ResourceID>, InternalError>
where
    R: ResourceDescriptorProvider,
{
    match resource_id {
        Some(resource_id) => Ok(Some(resource_id)),
        None => {
            resource_repository
                .find_resource_id_by_name(
                    metadata.account.clone(),
                    R::DESCRIPTOR.resource_type,
                    &metadata.name,
                )
                .await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn find_owned_snapshot<R>(
    resource_repository: &dyn ResourceRepository,
    account_id: &odf::AccountID,
    resource_id: ResourceID,
) -> Result<Option<ResourceSnapshot>, DeleteResourcesError>
where
    R: ResourceDescriptorProvider,
{
    let Some(resource_snapshot) =
        get_snapshot_by_query::<R>(resource_repository, &resource_id).await?
    else {
        return Ok(None);
    };

    if resource_snapshot.metadata.account != *account_id {
        return Err(DeleteResourcesError::not_enough_permissions(
            resource_snapshot.resource_id,
            R::DESCRIPTOR.resource_type,
        ));
    }

    Ok(Some(resource_snapshot))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_state_by_id<R>(
    resource_repository: &dyn ResourceRepository,
    account_id: odf::AccountID,
    resource_id: &ResourceID,
) -> Result<R::ResourceState, TypedResourceQueryError>
where
    R: DeclarativeResource + ResourceDescriptorProvider,
{
    let Some(resource_snapshot) =
        get_snapshot_by_query::<R>(resource_repository, resource_id).await?
    else {
        return Err(TypedResourceQueryError::NotFound(*resource_id));
    };

    if resource_snapshot.metadata.account != account_id {
        return Err(TypedResourceQueryError::NotFound(*resource_id));
    }

    if resource_snapshot.kind != R::DESCRIPTOR.resource_type
        || resource_snapshot.api_version != R::DESCRIPTOR.api_version
    {
        return Err(type_mismatch::<R>(&resource_snapshot));
    }

    R::decode_snapshot(resource_snapshot).map_err(TypedResourceQueryError::DecodeFailed)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn list_states_by_kind<R>(
    resource_repository: &dyn ResourceRepository,
    account_id: odf::AccountID,
    pagination: PaginationOpts,
) -> Result<Vec<R::ResourceState>, TypedResourceQueryError>
where
    R: DeclarativeResource + ResourceDescriptorProvider,
{
    let mut resource_snapshots_stream = resource_repository.list_resource_snapshots_by_kind(
        account_id,
        R::DESCRIPTOR.resource_type,
        pagination,
    );

    let mut resource_states = Vec::new();
    while let Some(resource_snapshot) = resource_snapshots_stream.next().await {
        let resource_snapshot = resource_snapshot?;

        if resource_snapshot.kind != R::DESCRIPTOR.resource_type
            || resource_snapshot.api_version != R::DESCRIPTOR.api_version
        {
            return Err(type_mismatch::<R>(&resource_snapshot));
        }

        resource_states.push(
            R::decode_snapshot(resource_snapshot).map_err(TypedResourceQueryError::DecodeFailed)?,
        );
    }

    Ok(resource_states)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! declare_resource_query_service {
    (
        service = $service:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::domain::ResourceQueryService<$resource>)]
        pub struct $service {
            resource_repository: std::sync::Arc<dyn $crate::domain::ResourceRepository>,
        }

        #[async_trait::async_trait]
        impl $crate::domain::ResourceQueryService<$resource> for $service {
            async fn allocate_id(
                &self,
            ) -> Result<$crate::domain::ResourceID, internal_error::InternalError> {
                self.resource_repository.new_resource_id().await
            }

            async fn find_existing_id_by_name(
                &self,
                resource_id: Option<$crate::domain::ResourceID>,
                metadata: &$crate::domain::ResourceMetadataInput,
            ) -> Result<Option<$crate::domain::ResourceID>, internal_error::InternalError> {
                super::shared::find_existing_id_by_name::<$resource>(
                    self.resource_repository.as_ref(),
                    resource_id,
                    metadata,
                )
                .await
            }

            async fn ensure_resource_id_matches_type(
                &self,
                resource_id: &$crate::domain::ResourceID,
            ) -> Result<(), $crate::domain::TypedResourceQueryError> {
                super::shared::load_snapshot_by_id::<$resource>(
                    self.resource_repository.as_ref(),
                    resource_id,
                )
                .await
                .map(|_| ())
            }

            async fn find_owned_snapshot(
                &self,
                account_id: &odf::AccountID,
                resource_id: $crate::domain::ResourceID,
            ) -> Result<
                Option<$crate::domain::ResourceSnapshot>,
                $crate::domain::DeleteResourcesError,
            > {
                super::shared::find_owned_snapshot::<$resource>(
                    self.resource_repository.as_ref(),
                    account_id,
                    resource_id,
                )
                .await
            }

            async fn get_state_by_id(
                &self,
                account_id: odf::AccountID,
                resource_id: &$crate::domain::ResourceID,
            ) -> Result<
                <$resource as $crate::domain::DeclarativeResource>::ResourceState,
                $crate::domain::TypedResourceQueryError,
            > {
                super::shared::get_state_by_id::<$resource>(
                    self.resource_repository.as_ref(),
                    account_id,
                    resource_id,
                )
                .await
            }

            async fn list_states_by_kind(
                &self,
                account_id: odf::AccountID,
                pagination: database_common::PaginationOpts,
            ) -> Result<
                Vec<<$resource as $crate::domain::DeclarativeResource>::ResourceState>,
                $crate::domain::TypedResourceQueryError,
            > {
                super::shared::list_states_by_kind::<$resource>(
                    self.resource_repository.as_ref(),
                    account_id,
                    pagination,
                )
                .await
            }
        }
    };
}

pub(crate) use declare_resource_query_service;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
