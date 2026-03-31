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
    ResourceMetadataInput,
    ResourceNotFoundError,
    ResourceNotOwnedByAccountError,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceTypeMismatchError,
    ResourceUID,
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

    pub async fn load_snapshot_by_uid(
        &self,
        uid: &ResourceUID,
    ) -> Result<ResourceSnapshot, TypedResourceQueryError> {
        let snapshot = self
            .resource_repository
            .find_resource_snapshot_by_uid(uid)
            .await?
            .ok_or(ResourceNotFoundError(*uid))?;

        if snapshot.kind != R::DESCRIPTOR.resource_type
            || snapshot.api_version != R::DESCRIPTOR.api_version
        {
            return Err(ResourceTypeMismatchError::new(
                *uid,
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
        uid: Option<ResourceUID>,
        metadata: &ResourceMetadataInput,
    ) -> Result<Option<ResourceUID>, InternalError> {
        match uid {
            Some(uid) => Ok(Some(uid)),
            None => {
                self.resource_repository
                    .find_resource_uid_by_name(
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
        uid: ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, FindOwnedResourceError> {
        let Some(resource_snapshot) = self.get_snapshot_by_query(&uid).await? else {
            return Ok(None);
        };

        if resource_snapshot.metadata.account != *account_id {
            return Err(odf::AccessError::Unauthorized(
                ResourceNotOwnedByAccountError {
                    uid: resource_snapshot.uid,
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
        uid: &ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let query = ResourceRawEventQuery {
            kind: R::DESCRIPTOR.resource_type.to_string(),
            uid: *uid,
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
    pub async fn get_state_by_uid(
        &self,
        account_id: odf::AccountID,
        uid: &ResourceUID,
    ) -> Result<R::ResourceState, TypedResourceQueryError> {
        let Some(resource_snapshot) = self.get_snapshot_by_query(uid).await? else {
            return Err(ResourceNotFoundError(*uid).into());
        };

        if resource_snapshot.metadata.account != account_id {
            return Err(ResourceNotFoundError(*uid).into());
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
            resource_snapshot.uid,
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
            async fn allocate_uid(
                &self,
            ) -> Result<kamu_resources::ResourceUID, internal_error::InternalError> {
                self.resource_repository.new_resource_uid().await
            }

            async fn find_existing_id_by_name(
                &self,
                uid: Option<kamu_resources::ResourceUID>,
                metadata: &kamu_resources::ResourceMetadataInput,
            ) -> Result<Option<kamu_resources::ResourceUID>, internal_error::InternalError> {
                let helper = $crate::ResourceQueryServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                );

                helper.find_existing_id_by_name(uid, metadata).await
            }

            async fn ensure_resource_uid_matches_type(
                &self,
                uid: &kamu_resources::ResourceUID,
            ) -> Result<(), kamu_resources::TypedResourceQueryError> {
                let helper = $crate::ResourceQueryServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                );

                helper.load_snapshot_by_uid(uid).await.map(|_| ())
            }

            async fn find_owned_snapshot(
                &self,
                account_id: &odf::AccountID,
                uid: kamu_resources::ResourceUID,
            ) -> Result<
                Option<kamu_resources::ResourceSnapshot>,
                kamu_resources::FindOwnedResourceError,
            > {
                let helper = $crate::ResourceQueryServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                );

                helper.find_owned_snapshot(account_id, uid).await
            }

            async fn get_state_by_uid(
                &self,
                account_id: odf::AccountID,
                uid: &kamu_resources::ResourceUID,
            ) -> Result<
                <$resource as kamu_resources::DeclarativeResource>::ResourceState,
                kamu_resources::TypedResourceQueryError,
            > {
                let helper = $crate::ResourceQueryServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                );

                helper.get_state_by_uid(account_id, uid).await
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
