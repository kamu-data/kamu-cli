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

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ActivityTimeWheel {
    activity_heap: BinaryHeap<Reverse<ActivityRecord>>,
    activity_times_by_id: HashMap<u64, DateTime<Utc>>,
}

// TODO: assign a score, and use it as an ordering criteria for the tasks within
// the same activation time
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ActivityRecord {
    pub activation_time: DateTime<Utc>,
    pub activity_id: u64,
}

impl ActivityRecord {
    fn new(activation_time: DateTime<Utc>, activity_id: u64) -> Self {
        Self {
            activation_time,
            activity_id,
        }
    }
}

impl ActivityTimeWheel {
    pub fn new() -> Self {
        Self {
            activity_heap: BinaryHeap::new(),
            activity_times_by_id: HashMap::new(),
        }
    }

    pub fn nearest_activation_moment(&self) -> Option<DateTime<Utc>> {
        self.activity_heap.peek().map(|ar| ar.0.activation_time)
    }

    pub fn take_nearest_planned_activities(&mut self) -> Vec<u64> {
        if self.activity_heap.is_empty() {
            vec![]
        } else {
            let activation_moment = self.activity_heap.peek().unwrap().0.activation_time;

            let mut res: Vec<_> = Vec::new();
            while let Some(ar) = self.activity_heap.peek() {
                if ar.0.activation_time > activation_moment {
                    break;
                }

                if self.is_activation_planned(ar.0.activity_id) {
                    res.push(ar.0.activity_id);
                }

                self.activity_heap.pop();
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
        activity_id: u64,
    ) -> Result<(), InternalError> {
        match self.activity_times_by_id.get(&activity_id) {
            Some(earlier_activation_time) => {
                if activation_time < *earlier_activation_time {
                    self.unplan_activity(activity_id);
                    self.plan_activity(ActivityRecord::new(activation_time, activity_id));
                }
                Ok(())
            }
            None => {
                self.plan_activity(ActivityRecord::new(activation_time, activity_id));
                Ok(())
            }
        }
    }

    pub fn is_activation_planned(&self, activity_id: u64) -> bool {
        self.activity_times_by_id.contains_key(&activity_id)
    }

    pub fn cancel_activation(
        &mut self,
        activity_id: u64,
    ) -> Result<(), TimeWheelCancelActivationError> {
        if self.activity_times_by_id.contains_key(&activity_id) {
            self.unplan_activity(activity_id);
            Ok(())
        } else {
            Err(TimeWheelCancelActivationError::UpdateNotPlanned(
                TimeWheelUpdateNotPlannedError { activity_id },
            ))
        }
    }

    fn plan_activity(&mut self, activity_record: ActivityRecord) {
        self.activity_times_by_id
            .insert(activity_record.activity_id, activity_record.activation_time);

        self.activity_heap.push(Reverse(activity_record));
    }

    fn unplan_activity(&mut self, activity_id: u64) {
        self.activity_times_by_id.remove(&activity_id);
        self.clean_top_cancellations();
    }

    fn clean_top_cancellations(&mut self) {
        while let Some(ar) = self.activity_heap.peek() {
            if self.is_activation_planned(ar.0.activity_id) {
                break;
            }

            self.activity_heap.pop();
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub(crate) enum TimeWheelCancelActivationError {
    #[error(transparent)]
    UpdateNotPlanned(TimeWheelUpdateNotPlannedError),
}

#[derive(Error, Debug)]
#[error("Activity {activity_id} not found planned in the time wheel")]
pub(crate) struct TimeWheelUpdateNotPlannedError {
    activity_id: u64,
}

/////////////////////////////////////////////////////////////////////////////////////////
