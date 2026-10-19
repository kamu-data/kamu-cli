// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::SubsecRound;

use crate::resources::{ResourceCtx, ResourceView, fixtures};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: generation counters as `get` reports them
//
// `generation` counts changes to headers and spec, `statusGeneration` counts
// status updates. Each reconciliation updates the status twice: when it starts
// and with its outcome.
//
// apply → spec update → labels and annotations update → unchanged re-apply
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_generations(ctx: ResourceCtx) {
    let name = "gen-vars";

    ctx.apply_variable_set(name, "one").await;
    let view = get(&ctx, name).await;
    assert_eq!(counters(&view), (1, 3, "Ready".to_string()));
    // Postgres keeps `updatedAt` in microseconds, the status timestamps keep
    // nanoseconds
    assert!(
        view.updated_at() >= view.reconciled_at().trunc_subsecs(6),
        "status updates must move updatedAt"
    );

    ctx.apply_variable_set(name, "two").await;
    assert_eq!(
        counters(&get(&ctx, name).await),
        (2, 5, "Ready".to_string())
    );

    // Same spec, headers gain labels and an annotation
    ctx.apply_variable_set_with_labels(name, "two").await;
    assert_eq!(
        counters(&get(&ctx, name).await),
        (3, 7, "Ready".to_string())
    );

    ctx.apply_variable_set_with_labels(name, "two").await;
    assert_eq!(
        counters(&get(&ctx, name).await),
        (3, 7, "Ready".to_string())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn get(ctx: &ResourceCtx, name: &str) -> ResourceView {
    let view = ctx.get_one(["get", "vs", name]).await;
    assert_eq!(view.ident(), (fixtures::VARIABLE_SET_SCHEMA, name));
    view
}

/// `(generation, statusGeneration, phase)`
fn counters(view: &ResourceView) -> (u64, u64, String) {
    (
        view.generation(),
        view.status_generation(),
        view.phase().to_string(),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
