// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use crate::domain::{
    DeclarativeResource,
    GetResourceByIdError,
    ReconcilableResource,
    ResourceDescriptorProvider,
    ResourceID,
    ResourceRawEventQuery,
    ResourceRepository,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct GetReconcilableResourceByIdHelper<R>
where
    R: ReconcilableResource + ResourceDescriptorProvider,
{
    _phantom: PhantomData<R>,
}

impl<R> GetReconcilableResourceByIdHelper<R>
where
    R: DeclarativeResource + ReconcilableResource + ResourceDescriptorProvider,
{
    pub async fn execute(
        resource_repository: &dyn ResourceRepository,
        account_id: odf::AccountID,
        resource_id: &ResourceID,
    ) -> Result<R::ResourceState, GetResourceByIdError> {
        let query = ResourceRawEventQuery {
            kind: R::DESCRIPTOR.resource_type.to_string(),
            id: *resource_id,
        };

        let resource_snapshot = resource_repository
            .find_resource_snapshot(&query)
            .await?
            .ok_or(GetResourceByIdError::NotFound)?;

        // TODO: ReBAC integration
        if resource_snapshot.metadata.account != account_id {
            return Err(GetResourceByIdError::NotFound);
        }

        if resource_snapshot.kind != R::DESCRIPTOR.resource_type {
            return InternalError::bail(format!(
                "Unexpected resource kind in resource repository row: expected='{}', actual='{}'",
                R::DESCRIPTOR.resource_type,
                resource_snapshot.kind
            ))
            .map_err(GetResourceByIdError::Internal);
        }

        if resource_snapshot.api_version != R::DESCRIPTOR.api_version {
            return InternalError::bail(format!(
                "Unexpected resource API version in resource repository row: expected='{}', \
                 actual='{}'",
                R::DESCRIPTOR.api_version,
                resource_snapshot.api_version
            ))
            .map_err(GetResourceByIdError::Internal);
        }

        R::decode_snapshot(resource_snapshot).map_err(GetResourceByIdError::Internal)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! declare_get_resource_by_id_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::domain::GetResourceByIdUseCase<$resource>)]
        pub struct $use_case {
            resource_repository: std::sync::Arc<dyn $crate::domain::ResourceRepository>,
        }

        #[async_trait::async_trait]
        impl $crate::domain::GetResourceByIdUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                account_id: odf::AccountID,
                id: &$crate::domain::ResourceID,
            ) -> Result<
                <$resource as $crate::domain::DeclarativeResource>::ResourceState,
                $crate::domain::GetResourceByIdError,
            > {
                $crate::get_by_id::GetReconcilableResourceByIdHelper::<$resource>::execute(
                    self.resource_repository.as_ref(),
                    account_id,
                    id,
                )
                .await
            }
        }
    };
}

pub(crate) use declare_get_resource_by_id_use_case;
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
