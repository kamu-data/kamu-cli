// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_resource_deletion_dispatcher {
    (
        dispatcher = $dispatcher:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ResourceDeletionDispatcher)]
        #[dill::meta(kamu_resources::ResourceDispatcherMeta {
            descriptor: <$resource as kamu_resources::ResourceDescriptorProvider>::DESCRIPTOR,
        })]
        pub struct $dispatcher {
            delete_resources_use_case:
                std::sync::Arc<dyn kamu_resources::DeleteResourcesUseCase<$resource>>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::ResourceDeletionDispatcher for $dispatcher {
            async fn delete_resources(
                &self,
                account_id: &odf::AccountID,
                uids: Vec<kamu_resources::ResourceUID>,
            ) -> Result<(), internal_error::InternalError> {
                self.delete_resources_use_case
                    .execute(account_id.clone(), uids)
                    .await
                    .map_err(internal_error::ErrorIntoInternal::int_err)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
