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
use internal_error::{InternalError, ResultIntoInternal};

use crate::{ResourceDescriptor, ResourceSnapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceLifecycleEventDispatcher: Send + Sync {
    async fn handle_applied(&self, resource: &ResourceSnapshot) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_resource_lifecycle_dispatcher_from_catalog(
    target_catalog: &Catalog,
    resource: &ResourceSnapshot,
) -> Result<Arc<dyn ResourceLifecycleEventDispatcher>, InternalError> {
    let mut dispatchers = target_catalog
        .builders_for_with_meta::<dyn ResourceLifecycleEventDispatcher, _>(
            |meta: &ResourceLifecycleEventDispatcherMeta| {
                meta.descriptor.matches_snapshot(resource)
            },
        );

    dispatchers
        .next()
        .map(|builder| {
            if dispatchers.next().is_some() {
                return Err(InternalError::new(format!(
                    "Duplicate resource lifecycle dispatchers registered for resource_type='{}', \
                     api_version='{}'",
                    resource.kind, resource.api_version
                )));
            }

            builder.get(target_catalog).int_err()
        })
        .transpose()?
        .ok_or_else(|| {
            InternalError::new(format!(
                "No resource lifecycle dispatcher registered for resource_type='{}', \
                 api_version='{}'",
                resource.kind, resource.api_version
            ))
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceLifecycleEventDispatcherMeta {
    pub descriptor: ResourceDescriptor,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
