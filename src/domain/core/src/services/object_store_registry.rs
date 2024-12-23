// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::datasource::object_store::ObjectStoreRegistry as ObjectStoreRegistryDatafusion;
use internal_error::InternalError;
use object_store::ObjectStore;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Holds a mapping between base URLs and the [`object_store::ObjectStore`]
/// instances that serve data from corresponding locations.
pub trait ObjectStoreRegistry: Send + Sync {
    fn as_datafusion_registry(self: Arc<Self>) -> Arc<dyn ObjectStoreRegistryDatafusion>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Allows lazily creating [`ObjectStore`] instances
pub trait ObjectStoreBuilder: Send + Sync {
    fn object_store_url(&self) -> Url;
    fn build_object_store(&self) -> Result<Arc<dyn ObjectStore>, InternalError>;
}
