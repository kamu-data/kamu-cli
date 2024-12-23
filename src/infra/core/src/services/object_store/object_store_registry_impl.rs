// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dashmap::DashMap;
use datafusion::error::DataFusionError;
use kamu_core::*;
use object_store::ObjectStore;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Replicates behavior of datafusion's `DefaultObjectStoreRegistry` but creates
/// stores lazily using [`ObjectStoreBuilder`]s registered in DI catalog.
///
/// This type is meant to be passed directly into `DataFusion`.
pub struct ObjectStoreRegistryImpl {
    builders: DashMap<String, Arc<dyn ObjectStoreBuilder>>,
    object_stores: DashMap<String, Arc<dyn ObjectStore>>,
}

#[dill::component(pub)]
#[dill::interface(dyn ObjectStoreRegistry)]
#[dill::scope(dill::Singleton)]
impl ObjectStoreRegistryImpl {
    pub fn new(builders: Vec<Arc<dyn ObjectStoreBuilder>>) -> Self {
        let builders = builders
            .into_iter()
            .map(|b| (Self::get_url_key(&b.object_store_url()), b))
            .collect();

        Self {
            builders,
            object_stores: DashMap::new(),
        }
    }

    fn get_url_key(url: &Url) -> String {
        format!(
            "{}://{}",
            url.scheme(),
            &url[url::Position::BeforeHost..url::Position::AfterPort],
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ObjectStoreRegistry for ObjectStoreRegistryImpl {
    fn as_datafusion_registry(
        self: Arc<Self>,
    ) -> Arc<dyn datafusion::datasource::object_store::ObjectStoreRegistry> {
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl datafusion::datasource::object_store::ObjectStoreRegistry for ObjectStoreRegistryImpl {
    fn register_store(
        &self,
        _url: &Url,
        _store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        panic!("ObjectStores should be registered via DI using ObjectStoreBuilder interface");
    }

    fn get_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let s = Self::get_url_key(url);

        // Try get existing store
        let store = self.object_stores.get(&s).map(|o| o.value().clone());

        if let Some(store) = store {
            return Ok(store);
        }

        // Try building
        let builder = self.builders.get(&s).map(|o| o.value().clone());
        if let Some(builder) = builder {
            let store = builder
                .build_object_store()
                .map_err(|e| DataFusionError::External(e.into()))?;

            self.object_stores.insert(s, store.clone());
            return Ok(store);
        }

        let keys: Vec<_> = self.builders.iter().map(|i| i.key().clone()).collect();
        Err(DataFusionError::Internal(format!(
            "No suitable object store found for {}, known stores are: {}",
            url,
            keys.join(", ")
        )))
    }
}

impl std::fmt::Debug for ObjectStoreRegistryImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreRegistryImpl").finish()
    }
}
