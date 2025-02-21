// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use internal_error::InternalError;
use kamu_core::*;
use url::Url;

use super::object_store_with_tracing::ObjectStoreWithTracing;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn ObjectStoreBuilder)]
pub struct ObjectStoreBuilderLocalFs {}

impl ObjectStoreBuilderLocalFs {
    pub fn new() -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
impl ObjectStoreBuilder for ObjectStoreBuilderLocalFs {
    fn object_store_url(&self) -> Url {
        Url::parse("file://").unwrap()
    }

    #[tracing::instrument(level = "info", name = ObjectStoreBuilderLocalFs_build_object_store, skip_all)]
    fn build_object_store(&self) -> Result<Arc<dyn object_store::ObjectStore>, InternalError> {
        Ok(Arc::new(ObjectStoreWithTracing::new(
            object_store::local::LocalFileSystem::new(),
        )))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
