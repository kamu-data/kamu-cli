// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::{ResourceCtx, fixtures};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: `context api-resources` Name/Aliases/Schema contract
//
// Pins the exact user-visible output acceptance testing flagged: `Name` is the
// canonical selector (the schema `TypeName`, e.g. `VariableSet`), not the old
// lowercase-plural selector, and the `Short Names` column is renamed to
// `Aliases`. Selector *resolution* (that every alias actually routes to the
// same type) is covered elsewhere — the facade contract test
// `test_selector_aliases_resolve_consistently` and the CLI selector e2e tests
// — this scenario only pins what `context api-resources` prints.
//
// This is deliberately coupled to the exact registered aliases/columns per
// type, so it will need updating whenever a resource type is added or its
// aliases change — an acceptable, intentional cost for pinning what the user
// actually sees.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_api_resources_command(ctx: ResourceCtx) {
    // -- Table output (`-o table`): exact rendering, baked from a real run -----
    let table_out = ctx
        .stdout(["context", "api-resources", "-o", "table"])
        .await;
    pretty_assertions::assert_eq!(
        table_out,
        indoc::indoc!(
            "
            ┌─────────────┬─────────────────┬────────────────────────────────────────────────────────────────┐
            │    Name     │     Aliases     │                             Schema                             │
            ├─────────────┼─────────────────┼────────────────────────────────────────────────────────────────┤
            │ SecretSet   │ secretsets,ss   │ https://opendatafabric.org/schemas/config/v1alpha1/SecretSet   │
            │ Storage     │ storages,st     │ https://opendatafabric.org/schemas/storage/v1alpha1/Storage    │
            │ VariableSet │ variablesets,vs │ https://opendatafabric.org/schemas/config/v1alpha1/VariableSet │
            └─────────────┴─────────────────┴────────────────────────────────────────────────────────────────┘
            "
        ),
        "`context api-resources -o table` should render this exact table, got:\n{table_out}"
    );

    // -- JSON output: exact array, baked from a real run ------------------------
    // Row order is not incidental:
    // `LocalResourceFacadeImpl::list_resource_type_descriptors`
    // sorts descriptors by canonical Name (alphabetically ascending), so
    // `SecretSet` < `Storage` < `VariableSet` deterministically.
    let json_out = ctx
        .stdout_json(["context", "api-resources", "-o", "json"])
        .await;
    pretty_assertions::assert_eq!(
        json_out,
        serde_json::json!([
            {
                "Name": "SecretSet",
                "Aliases": "secretsets,ss",
                "Schema": fixtures::SECRET_SET_SCHEMA,
            },
            {
                "Name": "Storage",
                "Aliases": "storages,st",
                "Schema": "https://opendatafabric.org/schemas/storage/v1alpha1/Storage",
            },
            {
                "Name": "VariableSet",
                "Aliases": "variablesets,vs",
                "Schema": fixtures::VARIABLE_SET_SCHEMA,
            },
        ]),
        "`context api-resources -o json` should render this exact array, got:\n{json_out:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
