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
pub trait ResourceDeletionDispatcher: Send + Sync {
    async fn delete_resources(
        &self,
        account_id: &odf::AccountID,
        uids: Vec<crate::ResourceUID>,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_resource_deletion_dispatcher_from_catalog(
    target_catalog: &Catalog,
    resource: &ResourceSnapshot,
) -> Result<Arc<dyn ResourceDeletionDispatcher>, InternalError> {
    get_resource_dispatcher_from_catalog(target_catalog, resource, "resource deletion dispatcher")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
