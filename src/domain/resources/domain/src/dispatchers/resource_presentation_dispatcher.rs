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

use crate::{
    ResourceDescriptor,
    ResourcePresentationDefinition,
    ResourceSnapshot,
    get_resource_dispatcher_from_catalog,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourcePresentationDispatcher: Send + Sync {
    fn descriptor(&self) -> ResourceDescriptor;

    fn presentation(&self) -> ResourcePresentationDefinition;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_resource_presentation_dispatcher_from_catalog(
    target_catalog: &Catalog,
    resource: &ResourceSnapshot,
) -> Result<Arc<dyn ResourcePresentationDispatcher>, InternalError> {
    get_resource_dispatcher_from_catalog(
        target_catalog,
        resource,
        "resource presentation dispatcher",
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
