// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use chrono::{DateTime, Utc};
use kamu_core::InternalError;
use thiserror::Error;

use crate::AnyFlowID;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub(crate) struct FlowTimeWheel {
    flow_heap: BinaryHeap<Reverse<FlowRecord>>,
    flow_activation_times_by_id: HashMap<AnyFlowID, DateTime<Utc>>,
}

// TODO: assign a score, and use it as an ordering criteria for the tasks within
// the same activation time
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct FlowRecord {
    pub activation_time: DateTime<Utc>,
    pub any_flow_id: AnyFlowID,
}

impl FlowRecord {
    fn new(activation_time: DateTime<Utc>, any_flow_id: AnyFlowID) -> Self {
        Self {
            activation_time,
            any_flow_id,
        }
    }
}

impl FlowTimeWheel {
    pub fn nearest_activation_moment(&self) -> Option<DateTime<Utc>> {
        self.flow_heap.peek().map(|ar| ar.0.activation_time)
    }

    pub fn take_nearest_planned_flows(&mut self) -> Vec<AnyFlowID> {
        if self.flow_heap.is_empty() {
            vec![]
        } else {
            let activation_moment = self.flow_heap.peek().unwrap().0.activation_time;

            let mut res: Vec<_> = Vec::new();
            while let Some(ar) = self.flow_heap.peek() {
                if ar.0.activation_time > activation_moment {
                    break;
                }

                if self.is_flow_activation_planned(&ar.0.any_flow_id) {
                    res.push(ar.0.any_flow_id);
                }

                self.flow_heap.pop();
            }

            self.clean_top_cancellations();

            res
        }
    }

    // TODO: maybe round activation time by a reasonable interval, like a minute, so
    // that scoring logic might be inserted
    pub fn activate_at(
        &mut self,
        activation_time: DateTime<Utc>,
        any_flow_id: AnyFlowID,
    ) -> Result<(), InternalError> {
        match self.flow_activation_times_by_id.get(&any_flow_id) {
            Some(earlier_activation_time) => {
                if activation_time < *earlier_activation_time {
                    self.unplan_flow(&any_flow_id);
                    self.plan_flow(FlowRecord::new(activation_time, any_flow_id));
                }
                Ok(())
            }
            None => {
                self.plan_flow(FlowRecord::new(activation_time, any_flow_id));
                Ok(())
            }
        }
    }

    pub fn is_flow_activation_planned(&self, any_flow_id: &AnyFlowID) -> bool {
        self.flow_activation_times_by_id.contains_key(&any_flow_id)
    }

    pub fn cancel_flow_activation(
        &mut self,
        any_flow_id: AnyFlowID,
    ) -> Result<(), TimeWheelCancelActivationError> {
        if self.flow_activation_times_by_id.contains_key(&any_flow_id) {
            self.unplan_flow(&any_flow_id);
            Ok(())
        } else {
            Err(TimeWheelCancelActivationError::FlowNotPlanned(
                TimeWheelFlowNotPlannedError { any_flow_id },
            ))
        }
    }

    fn plan_flow(&mut self, flow_record: FlowRecord) {
        self.flow_activation_times_by_id
            .insert(flow_record.any_flow_id, flow_record.activation_time);

        self.flow_heap.push(Reverse(flow_record));
    }

    fn unplan_flow(&mut self, any_flow_id: &AnyFlowID) {
        self.flow_activation_times_by_id.remove(any_flow_id);
        self.clean_top_cancellations();
    }

    fn clean_top_cancellations(&mut self) {
        while let Some(ar) = self.flow_heap.peek() {
            if self.is_flow_activation_planned(&ar.0.any_flow_id) {
                break;
            }

            self.flow_heap.pop();
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub(crate) enum TimeWheelCancelActivationError {
    #[error(transparent)]
    FlowNotPlanned(TimeWheelFlowNotPlannedError),
}

#[derive(Error, Debug)]
#[error("Flow {:?} not found planned in the time wheel", any_flow_id)]
pub(crate) struct TimeWheelFlowNotPlannedError {
    any_flow_id: AnyFlowID,
}

/////////////////////////////////////////////////////////////////////////////////////////
