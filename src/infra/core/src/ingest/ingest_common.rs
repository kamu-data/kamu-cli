// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::prelude::*;
use kamu_core::engine::*;
use kamu_core::{ObjectStoreRegistry, *};
use opendatafabric::*;

use crate::engine::*;

///////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "info", skip_all)]
pub(crate) async fn preprocess(
    operation_id: &str,
    engine_provisioner: &dyn EngineProvisioner,
    ctx: &SessionContext,
    transform: &Transform,
    input_data: DataFrame,
    maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
) -> Result<Option<DataFrame>, EngineError> {
    let engine = match transform.engine().to_lowercase().as_str() {
        "datafusion" => Arc::new(EngineDatafusionInproc::new()),
        engine_id => engine_provisioner
            .provision_engine(engine_id, maybe_listener)
            .await
            .int_err()?,
    };

    let response = engine
        .execute_raw_query(RawQueryRequestExt {
            operation_id: operation_id.to_string(),
            ctx: ctx.clone(),
            input_data,
            transform: transform.clone(),
        })
        .await?;

    Ok(response.output_data)
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) fn new_session_context(
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
) -> SessionContext {
    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion::prelude::*;

    let config = SessionConfig::new().with_default_catalog_and_schema("kamu", "kamu");

    let runtime_config = RuntimeConfig {
        object_store_registry: object_store_registry.as_datafusion_registry(),
        ..RuntimeConfig::default()
    };

    let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());

    SessionContext::new_with_config_rt(config, runtime)
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) fn next_operation_id() -> String {
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    let mut name = String::with_capacity(16);
    name.extend(
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from),
    );

    name
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) fn get_random_cache_key(prefix: &str) -> String {
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    let mut name = String::with_capacity(10 + prefix.len());
    name.push_str(prefix);
    name.extend(
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from),
    );
    name
}
