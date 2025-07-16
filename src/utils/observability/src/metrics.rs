// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use prometheus::Encoder as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Implementors of this trait will be aked during the startup to register
/// metrics that they provide.
pub trait MetricsProvider: Send + Sync {
    /// Called during startup to register the metrics
    ///
    /// IMPORTANT: Metrics that you register must be static or live in the
    /// [`dill::Singleton`] scope.
    fn register(&self, reg: &prometheus::Registry) -> prometheus::Result<()>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Uses catalog to extract all [`MetricsProvider`]s and register all provided
/// metrics in the [`prometheus::Registry`]
#[cfg(feature = "dill")]
pub fn register_all(catalog: &dill::Catalog) -> Arc<prometheus::Registry> {
    let registry: Arc<prometheus::Registry> = catalog
        .get_one()
        .expect("Prometheus registry is not in the DI catalog");

    for builder in catalog.builders_for::<dyn MetricsProvider>() {
        let metrics_set = builder.get(catalog).unwrap();
        metrics_set.register(&registry).unwrap();
    }

    registry
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "dill")]
pub async fn metrics_handler(axum::Extension(catalog): axum::Extension<dill::Catalog>) -> String {
    let reg = catalog.get_one::<prometheus::Registry>().unwrap();

    // Perf: registry has Arc inside and is cheap to clone
    metrics_handler_raw(axum::Extension(reg.as_ref().clone())).await
}

#[allow(clippy::unused_async)]
pub async fn metrics_handler_raw(
    axum::Extension(reg): axum::Extension<prometheus::Registry>,
) -> String {
    let mut buf = Vec::new();

    prometheus::TextEncoder::new()
        .encode(&reg.gather(), &mut buf)
        .unwrap();

    String::from_utf8(buf).unwrap()
}
