// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Helper utilities for preparing event data for the merged event stream
pub(crate) struct FlowEventDataHelper;

impl FlowEventDataHelper {
    /// Generic method to prepare merge event data for any event type.
    /// Takes events and an extractor function to get the event time.
    pub(crate) fn prepare_merge_event_data<T, F>(
        events: &[T],
        event_time_extractor: F,
    ) -> Vec<(DateTime<Utc>, serde_json::Value)>
    where
        F: Fn(&T) -> DateTime<Utc>,
        T: serde::Serialize,
    {
        events
            .iter()
            .map(|event| {
                (
                    event_time_extractor(event),
                    serde_json::to_value(event).unwrap(),
                )
            })
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
