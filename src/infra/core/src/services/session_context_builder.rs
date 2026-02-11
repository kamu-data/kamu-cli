// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionContext;
use internal_error::InternalError;
use kamu_core::{ObjectStoreRegistry, QueryOptions};
use kamu_datasets::DatasetRegistry;

use crate::EngineConfigDatafusionEmbeddedBatchQuery;
use crate::services::query::{KamuCatalog, KamuSchema};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
pub struct SessionContextBuilder {
    datafusion_engine_config: Arc<EngineConfigDatafusionEmbeddedBatchQuery>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    dataset_registry: Arc<dyn DatasetRegistry>,
}

impl SessionContextBuilder {
    pub(crate) async fn session_context(
        &self,
        options: QueryOptions,
    ) -> Result<SessionContext, InternalError> {
        assert!(
            options.input_datasets.is_some(),
            "SessionContextBuilder should resolve all inputs"
        );
        for opts in options.input_datasets.as_ref().unwrap().values() {
            assert!(
                opts.hints.handle.is_some(),
                "SessionContextBuilder should pre-resolve handles"
            );
        }

        let config = self.datafusion_engine_config.0.clone();

        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_object_store_registry(
                    self.object_store_registry.clone().as_datafusion_registry(),
                )
                .build()
                .unwrap(),
        );

        #[allow(unused_mut)]
        let mut ctx = SessionContext::new_with_config_rt(config, runtime);

        let schema = KamuSchema::prepare(&ctx, self.dataset_registry.clone(), options).await?;

        ctx.register_catalog("kamu", Arc::new(KamuCatalog::new(Arc::new(schema))));

        cfg_if::cfg_if! {
            if #[cfg(feature = "query-extensions-json")] {
                datafusion_functions_json::register_all(&mut ctx).unwrap();
            }
        }
        cfg_if::cfg_if! {
            if #[cfg(feature = "query-extensions-nested-functions")] {
                // Register array functions (and some others)
                // https://datafusion.apache.org/user-guide/sql/scalar_functions.html#array-functions
                datafusion::functions_nested::register_all(&mut ctx).unwrap();
            }
        }

        Ok(ctx)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
