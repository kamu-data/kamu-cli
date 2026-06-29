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
    ResourceDescriptorProvider,
    ResourceID,
    ResourceIDNotFoundError,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceTypeMismatchError,
    TypedResourceQueryError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TypedResourceQueryServiceHelper<'a, R>
where
    R: ResourceDescriptorProvider,
{
    resource_repository: &'a dyn ResourceRepository,
    _marker: std::marker::PhantomData<R>,
}

impl<'a, R> TypedResourceQueryServiceHelper<'a, R>
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
        id: &ResourceID,
    ) -> Result<ResourceSnapshot, TypedResourceQueryError> {
        let snapshot = self
            .resource_repository
            .find_resource_snapshot_by_id(id)
            .await?
            .ok_or(ResourceIDNotFoundError(*id))?;

        if snapshot.schema != R::DESCRIPTOR.schema {
            return Err(ResourceTypeMismatchError::new(
                *id,
                R::DESCRIPTOR.schema.to_string(),
                snapshot.schema,
            )
            .into());
        }

        Ok(snapshot)
    }

    async fn get_snapshot_by_query(
        &self,
        id: &ResourceID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let query = ResourceRawEventQuery {
            schema: R::DESCRIPTOR.schema.to_string(),
            id: *id,
        };

        self.resource_repository
            .find_resource_snapshot(&query)
            .await
    }
}

impl<R> TypedResourceQueryServiceHelper<'_, R>
where
    R: DeclarativeResource + ResourceDescriptorProvider,
{
    pub async fn get_state_by_id(
        &self,
        account_id: odf::AccountID,
        id: &ResourceID,
    ) -> Result<R::ResourceState, TypedResourceQueryError> {
        let Some(resource_snapshot) = self.get_snapshot_by_query(id).await? else {
            return Err(ResourceIDNotFoundError(*id).into());
        };

        if resource_snapshot.headers.account != account_id {
            return Err(ResourceIDNotFoundError(*id).into());
        }

        if resource_snapshot.schema != R::DESCRIPTOR.schema {
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
            .list_resource_snapshots_by_schema(account_id, R::DESCRIPTOR.schema, pagination);

        let mut resource_states = Vec::new();
        while let Some(resource_snapshot) = resource_snapshots_stream.next().await {
            let resource_snapshot = resource_snapshot?;

            if resource_snapshot.schema != R::DESCRIPTOR.schema {
                return Err(Self::type_mismatch(&resource_snapshot).int_err());
            }

            resource_states.push(R::ResourceState::try_from(resource_snapshot)?);
        }

        Ok(resource_states)
    }

    fn type_mismatch(resource_snapshot: &ResourceSnapshot) -> TypedResourceQueryError {
        ResourceTypeMismatchError::new(
            resource_snapshot.id,
            R::DESCRIPTOR.schema.to_string(),
            resource_snapshot.schema.clone(),
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_typed_resource_query_service {
    (
        service = $service:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::TypedResourceQueryService<$resource>)]
        pub struct $service {
            resource_repository: std::sync::Arc<dyn kamu_resources::ResourceRepository>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::TypedResourceQueryService<$resource> for $service {
            async fn ensure_resource_id_matches_type(
                &self,
                id: &kamu_resources::ResourceID,
            ) -> Result<(), kamu_resources::TypedResourceQueryError> {
                let helper = $crate::TypedResourceQueryServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                );

                helper.load_snapshot_by_id(id).await.map(|_| ())
            }

            async fn get_state_by_id(
                &self,
                account_id: odf::AccountID,
                id: &kamu_resources::ResourceID,
            ) -> Result<
                <$resource as kamu_resources::DeclarativeResource>::ResourceState,
                kamu_resources::TypedResourceQueryError,
            > {
                let helper = $crate::TypedResourceQueryServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                );

                helper.get_state_by_id(account_id, id).await
            }

            async fn list_states_by_kind(
                &self,
                account_id: odf::AccountID,
                pagination: database_common::PaginationOpts,
            ) -> Result<
                Vec<<$resource as kamu_resources::DeclarativeResource>::ResourceState>,
                internal_error::InternalError,
            > {
                let helper = $crate::TypedResourceQueryServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                );

                helper.list_states_by_kind(account_id, pagination).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
