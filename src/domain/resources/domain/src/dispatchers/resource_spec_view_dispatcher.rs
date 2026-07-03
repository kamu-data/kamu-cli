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

use crate::{TypeUri, get_resource_dispatcher_from_catalog};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceSpecViewDispatcher: Send + Sync {
    fn schema(&self) -> &'static TypeUri;

    fn reveal_spec(&self, spec: serde_json::Value) -> Result<serde_json::Value, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Returns the spec view dispatcher for the given resource schema, or
/// `None` if none is registered. Absence is normal for resource types that have
/// no sensitive fields.
pub fn get_resource_spec_view_dispatcher_from_catalog(
    catalog: &Catalog,
    schema: &TypeUri,
) -> Option<Arc<dyn ResourceSpecViewDispatcher>> {
    get_resource_dispatcher_from_catalog(catalog, schema, "resource spec view dispatcher").ok()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
