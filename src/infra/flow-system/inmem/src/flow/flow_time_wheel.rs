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
use kamu_flow_system::FlowID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub(crate) struct FlowTimeWheel {
    flow_heap: BinaryHeap<Reverse<FlowRecord>>,
    flow_activation_times_by_id: HashMap<FlowID, DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: assign a score, and use it as an ordering criteria for the tasks within
// the same activation time
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct FlowRecord {
    pub activation_time: DateTime<Utc>,
    pub flow_id: FlowID,
}

impl FlowRecord {
    fn new(activation_time: DateTime<Utc>, flow_id: FlowID) -> Self {
        Self {
            activation_time,
            flow_id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowTimeWheel {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn nearest_activation_moment(&self) -> Option<DateTime<Utc>> {
        self.flow_heap.peek().map(|ar| ar.0.activation_time)
    }

    pub(crate) fn take_nearest_planned_flows(&mut self) -> Vec<FlowID> {
        if self.flow_heap.is_empty() {
            vec![]
        } else {
            let activation_moment = self.flow_heap.peek().unwrap().0.activation_time;

            let mut res: Vec<_> = Vec::new();
            while let Some(ar) = self.flow_heap.peek() {
                if ar.0.activation_time > activation_moment {
                    break;
                }

                if self.is_flow_activation_planned_at(ar.0.flow_id, activation_moment) {
                    res.push(ar.0.flow_id);
                }

                self.flow_heap.pop();
            }

            self.clean_top_cancellations();

            res
        }
    }

    pub(crate) fn activate_at(&mut self, activation_time: DateTime<Utc>, flow_id: FlowID) {
        match self.flow_activation_times_by_id.get(&flow_id) {
            Some(earlier_activation_time) => {
                if activation_time < *earlier_activation_time {
                    self.unplan_flow(flow_id);
                    self.plan_flow(FlowRecord::new(activation_time, flow_id));
                }
            }
            None => {
                self.plan_flow(FlowRecord::new(activation_time, flow_id));
            }
        }
    }

    pub(crate) fn cancel_flow_activation(&mut self, flow_id: FlowID) {
        if self.flow_activation_times_by_id.contains_key(&flow_id) {
            self.unplan_flow(flow_id);
        }
    }

    fn is_flow_activation_planned_at(
        &self,
        flow_id: FlowID,
        activation_moment: DateTime<Utc>,
    ) -> bool {
        self.flow_activation_times_by_id
            .get(&flow_id)
            .is_some_and(|flow_activation_time| *flow_activation_time == activation_moment)
    }

    fn clean_top_cancellations(&mut self) {
        while let Some(ar) = self.flow_heap.peek() {
            if self.is_flow_activation_planned_at(ar.0.flow_id, ar.0.activation_time) {
                break;
            }

            self.flow_heap.pop();
        }
    }

    fn plan_flow(&mut self, flow_record: FlowRecord) {
        self.flow_activation_times_by_id
            .insert(flow_record.flow_id, flow_record.activation_time);

        self.flow_heap.push(Reverse(flow_record));
    }

    fn unplan_flow(&mut self, flow_id: FlowID) {
        self.flow_activation_times_by_id.remove(&flow_id);
        self.clean_top_cancellations();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;

    const FLOW_ID_1: u64 = 115;
    const FLOW_ID_2: u64 = 116;
    const FLOW_ID_3: u64 = 117;
    const FLOW_ID_4: u64 = 118;
    const FLOW_ID_5: u64 = 119;

    #[test]
    fn test_sequential_scheduling() {
        let mut timewheel = FlowTimeWheel::new();
        assert!(timewheel.nearest_activation_moment().is_none());

        let now = Utc::now();
        let moment_1 = now + Duration::try_seconds(10).unwrap();
        let moment_2 = now + Duration::try_seconds(20).unwrap();
        let moment_3 = now + Duration::try_seconds(30).unwrap();

        schedule_flow(&mut timewheel, moment_1, FLOW_ID_1);
        schedule_flow(&mut timewheel, moment_1, FLOW_ID_2);
        schedule_flow(&mut timewheel, moment_2, FLOW_ID_3);
        schedule_flow(&mut timewheel, moment_3, FLOW_ID_4);
        schedule_flow(&mut timewheel, moment_3, FLOW_ID_5);

        check_next_time_slot(&mut timewheel, moment_1, &[FLOW_ID_1, FLOW_ID_2]);
        check_next_time_slot(&mut timewheel, moment_2, &[FLOW_ID_3]);
        check_next_time_slot(&mut timewheel, moment_3, &[FLOW_ID_4, FLOW_ID_5]);
    }

    #[test]
    fn test_random_order_scheduling() {
        let mut timewheel = FlowTimeWheel::new();
        assert!(timewheel.nearest_activation_moment().is_none());

        let now = Utc::now();
        let moment_1 = now + Duration::try_seconds(10).unwrap();
        let moment_2 = now + Duration::try_seconds(20).unwrap();
        let moment_3 = now + Duration::try_seconds(30).unwrap();

        schedule_flow(&mut timewheel, moment_2, FLOW_ID_3);
        schedule_flow(&mut timewheel, moment_3, FLOW_ID_5);
        schedule_flow(&mut timewheel, moment_1, FLOW_ID_1);
        schedule_flow(&mut timewheel, moment_3, FLOW_ID_4);
        schedule_flow(&mut timewheel, moment_1, FLOW_ID_2);

        check_next_time_slot(&mut timewheel, moment_1, &[FLOW_ID_1, FLOW_ID_2]);
        check_next_time_slot(&mut timewheel, moment_2, &[FLOW_ID_3]);
        check_next_time_slot(&mut timewheel, moment_3, &[FLOW_ID_4, FLOW_ID_5]);
    }

    #[test]
    fn test_cancellations() {
        let mut timewheel = FlowTimeWheel::new();
        assert!(timewheel.nearest_activation_moment().is_none());

        let now = Utc::now();
        let moment_1 = now + Duration::try_seconds(10).unwrap();
        let moment_2 = now + Duration::try_seconds(20).unwrap();
        let moment_3 = now + Duration::try_seconds(30).unwrap();

        schedule_flow(&mut timewheel, moment_1, FLOW_ID_1);
        schedule_flow(&mut timewheel, moment_1, FLOW_ID_2);
        schedule_flow(&mut timewheel, moment_2, FLOW_ID_3);
        schedule_flow(&mut timewheel, moment_3, FLOW_ID_4);
        schedule_flow(&mut timewheel, moment_3, FLOW_ID_5);

        timewheel.cancel_flow_activation(FlowID::new(FLOW_ID_1));
        timewheel.cancel_flow_activation(FlowID::new(FLOW_ID_3));
        timewheel.cancel_flow_activation(FlowID::new(FLOW_ID_5));

        check_next_time_slot(&mut timewheel, moment_1, &[FLOW_ID_2]);
        check_next_time_slot(&mut timewheel, moment_3, &[FLOW_ID_4]);
        assert!(timewheel.nearest_activation_moment().is_none());
    }

    fn schedule_flow(timewheel: &mut FlowTimeWheel, moment: DateTime<Utc>, flow_id: u64) {
        timewheel.activate_at(moment, FlowID::new(flow_id));
    }

    fn check_next_time_slot(
        timewheel: &mut FlowTimeWheel,
        moment: DateTime<Utc>,
        flow_ids: &[u64],
    ) {
        assert_eq!(timewheel.nearest_activation_moment().unwrap(), moment);
        assert_eq!(
            timewheel.take_nearest_planned_flows(),
            flow_ids
                .iter()
                .map(|id| FlowID::new(*id))
                .collect::<Vec<_>>()
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
