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
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_flow_system::{EventID, FlowBinding, FlowScopeQuery};
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn generate_scope_query_condition_clauses(
    flow_scope_query: &FlowScopeQuery,
    starting_parameter_index: usize,
) -> (String, usize) {
    let mut parameter_index = starting_parameter_index;

    let mut scope_clauses = Vec::new();
    // keys are &'static str from code; safe to inline
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

    let text = if scope_clauses.is_empty() {
        "1".to_string()
    } else {
        scope_clauses.join(" AND ")
    };

    (text, parameter_index)
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

/// Serializes flow bindings into the JSON parameter of a `json_each($1)`
/// lookup, with scope data in the canonical form stored in `scope_data` and
/// the position of each binding under `idx`
pub(crate) fn flow_bindings_to_json(
    flow_bindings: &[FlowBinding],
) -> Result<String, InternalError> {
    let bindings = flow_bindings
        .iter()
        .enumerate()
        .map(|(idx, flow_binding)| {
            let scope_json = serde_json::to_value(&flow_binding.scope).int_err()?;
            Ok(json!({
                "idx": idx,
                "flow_type": flow_binding.flow_type,
                "scope_data": canonical_json::to_string(&scope_json).int_err()?,
            }))
        })
        .collect::<Result<Vec<_>, InternalError>>()?;

    serde_json::to_string(&bindings).int_err()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Computes the last event ID of every item of a multi-save from the global
/// event counter, which triggers advance by one per inserted event in insertion
/// order
pub(crate) fn global_counter_event_ids_per_item(
    counter_before: i64,
    counter_after: i64,
    item_event_counts: &[i64],
) -> Result<Vec<EventID>, InternalError> {
    let num_total_events: i64 = item_event_counts.iter().sum();
    if counter_after != counter_before + num_total_events {
        return Err(format!(
            "Global event counter advanced by {}, expected {num_total_events}",
            counter_after - counter_before
        )
        .int_err());
    }

    Ok(item_event_counts
        .iter()
        .scan(counter_before, |last_event_id, item_event_count| {
            *last_event_id += item_event_count;
            Some(EventID::new(*last_event_id))
        })
        .collect())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
