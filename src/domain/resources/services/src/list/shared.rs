// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use database_common::PaginationOpts;
use tokio_stream::StreamExt;

use crate::domain::{
    DeclarativeResource,
    ReconcilableResource,
    ResourceDescriptorProvider,
    ResourceRepository,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ListReconcilableResourcesByKindHelper<R>
where
    R: ReconcilableResource + ResourceDescriptorProvider,
{
    _phantom: PhantomData<R>,
}

impl<R> ListReconcilableResourcesByKindHelper<R>
where
    R: DeclarativeResource + ReconcilableResource + ResourceDescriptorProvider,
{
    pub async fn execute(
        resource_repository: &dyn ResourceRepository,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<Vec<R::ResourceState>, internal_error::InternalError> {
        let mut resource_snapshots_stream = resource_repository.list_resource_snapshots_by_kind(
            account_id,
            R::DESCRIPTOR.resource_type,
            pagination,
        );

        let mut resource_states = Vec::new();
        while let Some(resource_snapshot) = resource_snapshots_stream.next().await {
            let resource_snapshot = resource_snapshot?;

            if resource_snapshot.kind != R::DESCRIPTOR.resource_type {
                internal_error::InternalError::bail(format!(
                    "Unexpected resource kind in resource repository row: expected='{}', \
                     actual='{}'",
                    R::DESCRIPTOR.resource_type,
                    resource_snapshot.kind
                ))?;
            }

            if resource_snapshot.api_version != R::DESCRIPTOR.api_version {
                internal_error::InternalError::bail(format!(
                    "Unexpected resource API version in resource repository row: expected='{}', \
                     actual='{}'",
                    R::DESCRIPTOR.api_version,
                    resource_snapshot.api_version
                ))?;
            }

            resource_states.push(R::decode_snapshot(resource_snapshot)?);
        }

        Ok(resource_states)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! declare_list_resources_by_kind_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::domain::ListResourcesByKindUseCase<$resource>)]
        pub struct $use_case {
            resource_repository: std::sync::Arc<dyn $crate::domain::ResourceRepository>,
        }

        #[async_trait::async_trait]
        impl $crate::domain::ListResourcesByKindUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                account_id: odf::AccountID,
                pagination: database_common::PaginationOpts,
            ) -> Result<
                Vec<<$resource as $crate::domain::DeclarativeResource>::ResourceState>,
                internal_error::InternalError,
            > {
                $crate::list::ListReconcilableResourcesByKindHelper::<$resource>::execute(
                    self.resource_repository.as_ref(),
                    account_id,
                    pagination,
                )
                .await
            }
        }
    };
}

pub(crate) use declare_list_resources_by_kind_use_case;
