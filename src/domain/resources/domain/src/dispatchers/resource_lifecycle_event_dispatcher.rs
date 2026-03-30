// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::Catalog;
use internal_error::InternalError;

use crate::{ResourceSnapshot, get_resource_dispatcher_from_catalog};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceLifecycleEventDispatcher: Send + Sync {
    async fn handle_applied(&self, resource: &ResourceSnapshot) -> Result<(), InternalError>;

    async fn handle_reconciliation_succeeded(
        &self,
        resource: &ResourceSnapshot,
    ) -> Result<(), InternalError>;

    async fn handle_reconciliation_failed(
        &self,
        resource: &ResourceSnapshot,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_resource_lifecycle_dispatcher_from_catalog(
    target_catalog: &Catalog,
    resource: &ResourceSnapshot,
) -> Result<Arc<dyn ResourceLifecycleEventDispatcher>, InternalError> {
    get_resource_dispatcher_from_catalog(target_catalog, resource, "resource lifecycle dispatcher")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
