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

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

pub fn new_session_context(object_store_registry: Arc<dyn ObjectStoreRegistry>) -> SessionContext {
    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion::prelude::*;

    let config = SessionConfig::new().with_default_catalog_and_schema("kamu", "kamu");

    let runtime_config = RuntimeConfig {
        object_store_registry: object_store_registry.as_datafusion_registry(),
        ..RuntimeConfig::default()
    };

    let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());

    let mut ctx = SessionContext::new_with_config_rt(config, runtime);

    // TODO: As part of the ODF spec we should let people opt-in into various
    // SQL extensions on per-transform basis
    datafusion_ethers::udf::register_all(&mut ctx).unwrap();
    datafusion_functions_json::register_all(&mut ctx).unwrap();

    ctx
}
