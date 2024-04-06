// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_core::ObjectStoreRegistry;

/////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub ObjectStoreRegistry {}
    impl ObjectStoreRegistry for ObjectStoreRegistry {
      fn as_datafusion_registry(self: Arc<Self>) -> Arc<dyn datafusion::datasource::object_store::ObjectStoreRegistry>;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl MockObjectStoreRegistry {
    pub fn with_datafusion_default() -> Self {
        MockObjectStoreRegistry::default()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
