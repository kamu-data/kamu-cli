// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use database_common::sqlite_generate_placeholders_list;
use kamu_flow_system::FlowScopeQuery;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn generate_scope_query_condition_clauses(
    flow_scope_query: &FlowScopeQuery,
    starting_parameter_index: usize,
) -> (String, usize) {
    let mut parameter_index = starting_parameter_index;

    let mut scope_clauses = Vec::new();
    for (key, values) in &flow_scope_query.attributes {
        if values.len() == 1 {
            scope_clauses.push(format!(
                "json_extract(scope_data, '$.{key}') = ${parameter_index}",
            ));
            parameter_index += 1;
        } else if !values.is_empty() {
            scope_clauses.push(format!(
                "json_extract(scope_data, '$.{key}') IN ({})",
                sqlite_generate_placeholders_list(
                    values.len(),
                    NonZeroUsize::new(parameter_index).unwrap()
                )
            ));
            parameter_index += values.len();
        }
    }

    (scope_clauses.join(" AND "), parameter_index)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn form_scope_query_condition_values(flow_scope_query: FlowScopeQuery) -> Vec<String> {
    let mut scope_values = Vec::new();
    for (_, values) in flow_scope_query.attributes {
        for value in values {
            scope_values.push(value);
        }
    }
    scope_values
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
