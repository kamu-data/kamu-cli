// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use kamu_core::{InternalError, ResultIntoInternal};
use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ActivityTimeWheel {
    time_slots: VecDeque<TimeSlot>,
    activity_times: HashMap<u64, DateTime<Utc>>,
}

struct TimeSlot {
    pub activation_time: DateTime<Utc>,
    pub planned_activity_ids: HashSet<u64>,
}

impl TimeSlot {
    fn new(activation_time: DateTime<Utc>) -> Self {
        Self {
            activation_time,
            planned_activity_ids: HashSet::new(),
        }
    }
}

impl ActivityTimeWheel {
    pub fn new() -> Self {
        Self {
            time_slots: VecDeque::new(),
            activity_times: HashMap::new(),
        }
    }

    pub fn nearest_activation_moment(&self) -> Option<DateTime<Utc>> {
        self.time_slots.front().map(|ts| ts.activation_time)
    }

    pub fn nearest_planned_activities(&self) -> Box<dyn Iterator<Item = u64>> {
        match self.time_slots.front() {
            Some(ts) => Box::new(ts.planned_activity_ids.clone().into_iter()),
            None => Box::new(std::iter::empty()),
        }
    }

    pub fn activate_at(
        &mut self,
        activation_time: DateTime<Utc>,
        activity_id: u64,
    ) -> Result<(), InternalError> {
        match self.activity_times.get(&activity_id) {
            Some(earlier_activation_time) => {
                if &activation_time < earlier_activation_time {
                    self.unplan_activity_for(*earlier_activation_time, activity_id)
                        .int_err()?;
                    self.plan_activity_for(activation_time, activity_id);
                }
                Ok(())
            }
            None => {
                self.plan_activity_for(activation_time, activity_id);
                Ok(())
            }
        }
    }

    pub fn is_activation_planned(&self, activity_id: u64) -> bool {
        self.activity_times.get(&activity_id).is_some()
    }

    pub fn cancel_activation(
        &mut self,
        activity_id: u64,
    ) -> Result<(), TimeWheelCancelActivationError> {
        match self.activity_times.get(&activity_id) {
            Some(update_time) => {
                self.unplan_activity_for(update_time.to_owned(), activity_id)
                    .map_err(|e| TimeWheelCancelActivationError::SlotNotPlanned(e))?;
                Ok(())
            }
            None => Err(TimeWheelCancelActivationError::UpdateNotPlanned(
                TimeWheelUpdateNotPlannedError { activity_id },
            )),
        }
    }

    pub fn spin(&mut self) {
        if !self.time_slots.is_empty() {
            self.time_slots.pop_front();
        }
    }

    fn plan_activity_for(&mut self, activation_time: DateTime<Utc>, activity_id: u64) {
        match self.find_time_slot(&activation_time) {
            Some(ts) => {
                ts.planned_activity_ids.insert(activity_id);
            }
            None => {
                let mut ts = TimeSlot::new(activation_time);
                ts.planned_activity_ids.insert(activity_id);
                self.insert_time_slot(ts);
            }
        }

        self.activity_times.insert(activity_id, activation_time);
    }

    fn unplan_activity_for(
        &mut self,
        activation_time: DateTime<Utc>,
        activity_id: u64,
    ) -> Result<(), TimeWheelSlotNotPlannedError> {
        let maybe_remove_ts_at = match self.find_time_slot(&activation_time) {
            Some(ts) => {
                ts.planned_activity_ids.remove(&activity_id);
                if ts.planned_activity_ids.is_empty() {
                    Some(ts.activation_time)
                } else {
                    None
                }
            }
            None => {
                return Err(TimeWheelSlotNotPlannedError {
                    activation_time: activation_time.to_owned(),
                })
            }
        };

        if let Some(remove_ts_at) = maybe_remove_ts_at {
            self.remove_time_slot_at(remove_ts_at)
        }

        Ok(())
    }

    fn find_time_slot(&mut self, activation_time: &DateTime<Utc>) -> Option<&mut TimeSlot> {
        self.time_slots
            .binary_search_by(|ts| ts.activation_time.cmp(activation_time))
            .ok()
            .map(|idx| self.time_slots.get_mut(idx).unwrap())
    }

    fn insert_time_slot(&mut self, new_ts: TimeSlot) {
        let insert_idx = self
            .time_slots
            .binary_search_by(|ts| ts.activation_time.cmp(&new_ts.activation_time))
            .expect_err("Existing timeslot unexpected");

        self.time_slots.insert(insert_idx, new_ts);
    }

    fn remove_time_slot_at(&mut self, activation_time: DateTime<Utc>) {
        let idx = self
            .time_slots
            .binary_search_by(|ts| ts.activation_time.cmp(&activation_time))
            .unwrap();

        self.time_slots.remove(idx);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub(crate) enum TimeWheelCancelActivationError {
    #[error(transparent)]
    UpdateNotPlanned(TimeWheelUpdateNotPlannedError),
    #[error(transparent)]
    SlotNotPlanned(TimeWheelSlotNotPlannedError),
}

#[derive(Error, Debug)]
#[error("Activity {activity_id} not found planned in the time wheel")]
pub(crate) struct TimeWheelUpdateNotPlannedError {
    activity_id: u64,
}

#[derive(Error, Debug)]
#[error("Timeslot for {activation_time} not found in the time wheel")]
pub(crate) struct TimeWheelSlotNotPlannedError {
    activation_time: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////
