// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod declare_resource_crud_dispatcher;
mod resource_crud_dispatcher_helpers;
mod resource_crud_dispatcher_registry;

pub use kamu_resources::{
    ApplyManifestPlan,
    ApplyResourceCrudDispatcherError,
    DeleteResourcesCrudDispatcherError,
    GetResourceCrudDispatcherError,
    ResourceCrudDispatcher,
    ResourceCrudDispatcherApplyRequest,
    ResourceCrudDispatcherDeleteRequest,
    ResourceCrudDispatcherGetRequest,
    ResourceCrudDispatcherListRequest,
    ResourceLifecycleValidationError,
    UnsupportedResourceDescriptorError,
};
pub use resource_crud_dispatcher_helpers::*;
pub use resource_crud_dispatcher_registry::*;
