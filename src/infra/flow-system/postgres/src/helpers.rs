// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::FlowScopeQuery;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn generate_scope_query_condition_clauses(
    flow_scope_query: &FlowScopeQuery,
    starting_parameter_index: usize,
) -> (String, usize) {
    let mut parameter_index = starting_parameter_index;

    let mut scope_clauses = Vec::new();
    for (key, values) in &flow_scope_query.attributes {
        if values.is_empty() {
            continue;
        }

        scope_clauses.push(format!("scope_data->>'{key}' = ANY(${parameter_index})"));
        parameter_index += 1;
    }

    (scope_clauses.join(" AND "), parameter_index)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn form_scope_query_condition_values(
    flow_scope_query: FlowScopeQuery,
) -> Vec<Vec<String>> {
    let mut scope_values = Vec::new();
    for (_, values) in flow_scope_query.attributes {
        if !values.is_empty() {
            scope_values.push(values);
        }
    }
    scope_values
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
