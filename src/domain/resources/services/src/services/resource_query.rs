// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use internal_error::{ErrorIntoInternal, InternalError};
use tokio_stream::StreamExt;

use crate::domain::{
    DeclarativeResource,
    FindOwnedResourceError,
    ResourceDescriptorProvider,
    ResourceID,
    ResourceMetadataInput,
    ResourceNotFoundError,
    ResourceNotOwnedByAccountError,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceTypeMismatchError,
    TypedResourceQueryError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceQueryServiceHelper<'a, R>
where
    R: ResourceDescriptorProvider,
{
    resource_repository: &'a dyn ResourceRepository,
    _marker: std::marker::PhantomData<R>,
}

impl<'a, R> ResourceQueryServiceHelper<'a, R>
where
    R: ResourceDescriptorProvider,
{
    pub fn new(resource_repository: &'a dyn ResourceRepository) -> Self {
        Self {
            resource_repository,
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn load_snapshot_by_id(
        &self,
        resource_id: &ResourceID,
    ) -> Result<ResourceSnapshot, TypedResourceQueryError> {
        let snapshot = self
            .resource_repository
            .find_resource_snapshot_by_id(resource_id)
            .await?
            .ok_or(ResourceNotFoundError(*resource_id))?;

        if snapshot.kind != R::DESCRIPTOR.resource_type
            || snapshot.api_version != R::DESCRIPTOR.api_version
        {
            return Err(ResourceTypeMismatchError::new(
                *resource_id,
                R::DESCRIPTOR.resource_type.to_string(),
                R::DESCRIPTOR.api_version.to_string(),
                snapshot.kind,
                snapshot.api_version,
            )
            .into());
        }

        Ok(snapshot)
    }

    pub async fn find_existing_id_by_name(
        &self,
        resource_id: Option<ResourceID>,
        metadata: &ResourceMetadataInput,
    ) -> Result<Option<ResourceID>, InternalError> {
        match resource_id {
            Some(resource_id) => Ok(Some(resource_id)),
            None => {
                self.resource_repository
                    .find_resource_id_by_name(
                        metadata.account.clone(),
                        R::DESCRIPTOR.resource_type,
                        &metadata.name,
                    )
                    .await
            }
        }
    }

    pub async fn find_owned_snapshot(
        &self,
        account_id: &odf::AccountID,
        resource_id: ResourceID,
    ) -> Result<Option<ResourceSnapshot>, FindOwnedResourceError> {
        let Some(resource_snapshot) = self.get_snapshot_by_query(&resource_id).await? else {
            return Ok(None);
        };

        if resource_snapshot.metadata.account != *account_id {
            return Err(odf::AccessError::Unauthorized(
                ResourceNotOwnedByAccountError {
                    resource_id: resource_snapshot.resource_id,
                    resource_type: R::DESCRIPTOR.resource_type,
                }
                .into(),
            )
            .into());
        }

        Ok(Some(resource_snapshot))
    }

    async fn get_snapshot_by_query(
        &self,
        resource_id: &ResourceID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let query = ResourceRawEventQuery {
            kind: R::DESCRIPTOR.resource_type.to_string(),
            id: *resource_id,
        };

        self.resource_repository
            .find_resource_snapshot(&query)
            .await
    }
}

impl<R> ResourceQueryServiceHelper<'_, R>
where
    R: DeclarativeResource + ResourceDescriptorProvider,
{
    pub async fn get_state_by_id(
        &self,
        account_id: odf::AccountID,
        resource_id: &ResourceID,
    ) -> Result<R::ResourceState, TypedResourceQueryError> {
        let Some(resource_snapshot) = self.get_snapshot_by_query(resource_id).await? else {
            return Err(ResourceNotFoundError(*resource_id).into());
        };

        if resource_snapshot.metadata.account != account_id {
            return Err(ResourceNotFoundError(*resource_id).into());
        }

        if resource_snapshot.kind != R::DESCRIPTOR.resource_type
            || resource_snapshot.api_version != R::DESCRIPTOR.api_version
        {
            return Err(Self::type_mismatch(&resource_snapshot));
        }

        R::ResourceState::try_from(resource_snapshot).map_err(TypedResourceQueryError::Internal)
    }

    pub async fn list_states_by_kind(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<Vec<R::ResourceState>, InternalError> {
        let mut resource_snapshots_stream = self
            .resource_repository
            .list_resource_snapshots_by_kind(account_id, R::DESCRIPTOR.resource_type, pagination);

        let mut resource_states = Vec::new();
        while let Some(resource_snapshot) = resource_snapshots_stream.next().await {
            let resource_snapshot = resource_snapshot?;

            if resource_snapshot.kind != R::DESCRIPTOR.resource_type
                || resource_snapshot.api_version != R::DESCRIPTOR.api_version
            {
                return Err(Self::type_mismatch(&resource_snapshot).int_err());
            }

            resource_states.push(R::ResourceState::try_from(resource_snapshot)?);
        }

        Ok(resource_states)
    }

    fn type_mismatch(resource_snapshot: &ResourceSnapshot) -> TypedResourceQueryError {
        ResourceTypeMismatchError::new(
            resource_snapshot.resource_id,
            R::DESCRIPTOR.resource_type.to_string(),
            R::DESCRIPTOR.api_version.to_string(),
            resource_snapshot.kind.clone(),
            resource_snapshot.api_version.clone(),
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_resource_query_service {
    (
        service = $service:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ResourceQueryService<$resource>)]
        pub struct $service {
            resource_repository: std::sync::Arc<dyn kamu_resources::ResourceRepository>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::ResourceQueryService<$resource> for $service {
            async fn allocate_id(
                &self,
            ) -> Result<kamu_resources::ResourceID, internal_error::InternalError> {
                self.resource_repository.new_resource_id().await
            }

            async fn find_existing_id_by_name(
                &self,
                resource_id: Option<kamu_resources::ResourceID>,
                metadata: &kamu_resources::ResourceMetadataInput,
            ) -> Result<Option<kamu_resources::ResourceID>, internal_error::InternalError> {
                let helper = $crate::ResourceQueryServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                );

                helper.find_existing_id_by_name(resource_id, metadata).await
            }

            async fn ensure_resource_id_matches_type(
                &self,
                resource_id: &kamu_resources::ResourceID,
            ) -> Result<(), kamu_resources::TypedResourceQueryError> {
                let helper = $crate::ResourceQueryServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                );

                helper.load_snapshot_by_id(resource_id).await.map(|_| ())
            }

            async fn find_owned_snapshot(
                &self,
                account_id: &odf::AccountID,
                resource_id: kamu_resources::ResourceID,
            ) -> Result<
                Option<kamu_resources::ResourceSnapshot>,
                kamu_resources::FindOwnedResourceError,
            > {
                let helper = $crate::ResourceQueryServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                );

                helper.find_owned_snapshot(account_id, resource_id).await
            }

            async fn get_state_by_id(
                &self,
                account_id: odf::AccountID,
                resource_id: &kamu_resources::ResourceID,
            ) -> Result<
                <$resource as kamu_resources::DeclarativeResource>::ResourceState,
                kamu_resources::TypedResourceQueryError,
            > {
                let helper = $crate::ResourceQueryServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                );

                helper.get_state_by_id(account_id, resource_id).await
            }

            async fn list_states_by_kind(
                &self,
                account_id: odf::AccountID,
                pagination: database_common::PaginationOpts,
            ) -> Result<
                Vec<<$resource as kamu_resources::DeclarativeResource>::ResourceState>,
                internal_error::InternalError,
            > {
                let helper = $crate::ResourceQueryServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                );

                helper.list_states_by_kind(account_id, pagination).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
