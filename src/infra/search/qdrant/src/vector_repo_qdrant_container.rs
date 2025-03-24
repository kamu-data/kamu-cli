// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::*;
use kamu_search::{FoundPoint, InsertError, NewPoint, SearchPointsOpts, VectorRepository};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct VectorRepositoryConfigQdrantContainer {
    pub image: String,
    pub dimensions: usize,
    pub start_timeout: std::time::Duration,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Lazily spawns a local Qdrant container that will be cleaned up on exit
pub struct VectorRepositoryQdrantContainer {
    runtime: Arc<container_runtime::ContainerRuntime>,
    config: Arc<VectorRepositoryConfigQdrantContainer>,
    state: tokio::sync::OnceCell<State>,
}

#[allow(dead_code)]
struct State {
    container: container_runtime::ContainerProcess,
    inner: VectorRepositoryQdrant,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn VectorRepository)]
impl VectorRepositoryQdrantContainer {
    pub fn new(
        runtime: Arc<container_runtime::ContainerRuntime>,
        config: Arc<VectorRepositoryConfigQdrantContainer>,
    ) -> Self {
        Self {
            runtime,
            config,
            state: tokio::sync::OnceCell::new(),
        }
    }

    async fn inner(&self) -> Result<&VectorRepositoryQdrant, InternalError> {
        let state = self
            .state
            .get_or_try_init(async || self.init_state().await)
            .await?;

        Ok(&state.inner)
    }

    async fn init_state(&self) -> Result<State, InternalError> {
        let container = self
            .runtime
            .run_attached(self.config.image.clone())
            .random_container_name_with_prefix("kamu-search-qdrant-")
            .expose_ports([6333, 6334])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .int_err()?;

        let grpc_port = container
            .wait_for_host_socket(6334, self.config.start_timeout)
            .await
            .int_err()?;

        let runtime_host = self.runtime.get_runtime_host_addr();

        let inner = VectorRepositoryQdrant::new(Arc::new(VectorRepositoryConfigQdrant {
            url: format!("http://{runtime_host}:{grpc_port}"),
            collection_name: "datasets".to_string(),
            dimensions: self.config.dimensions,
        }));

        Ok(State { container, inner })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl VectorRepository for VectorRepositoryQdrantContainer {
    async fn num_points(&self) -> Result<usize, InternalError> {
        self.inner().await?.num_points().await
    }

    async fn insert(&self, points: Vec<NewPoint>) -> Result<(), InsertError> {
        self.inner().await?.insert(points).await
    }

    async fn search_points(
        &self,
        vec: &[f32],
        opts: SearchPointsOpts,
    ) -> Result<Vec<FoundPoint>, InternalError> {
        self.inner().await?.search_points(vec, opts).await
    }

    async fn clear(&self) -> Result<(), InternalError> {
        self.inner().await?.clear().await
    }
}
