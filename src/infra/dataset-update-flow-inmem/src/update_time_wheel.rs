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
use kamu_dataset_update_flow::UpdateID;
use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct UpdateTimeWheel {
    time_slots: VecDeque<TimeSlot>,
    update_times: HashMap<UpdateID, DateTime<Utc>>,
}

struct TimeSlot {
    pub activation_time: DateTime<Utc>,
    pub planned_updates: HashSet<UpdateID>,
}

impl TimeSlot {
    fn new(activation_time: DateTime<Utc>) -> Self {
        Self {
            activation_time,
            planned_updates: HashSet::new(),
        }
    }
}

impl UpdateTimeWheel {
    pub fn new() -> Self {
        Self {
            time_slots: VecDeque::new(),
            update_times: HashMap::new(),
        }
    }

    pub fn nearest_activation_moment(&self) -> Option<DateTime<Utc>> {
        self.time_slots.front().map(|ts| ts.activation_time)
    }

    pub fn nearest_planned_updates(&self) -> Box<dyn Iterator<Item = UpdateID>> {
        match self.time_slots.front() {
            Some(ts) => Box::new(ts.planned_updates.clone().into_iter()),
            None => Box::new(std::iter::empty()),
        }
    }

    pub fn activate_update_at(
        &mut self,
        activation_time: DateTime<Utc>,
        update_id: UpdateID,
    ) -> Result<(), InternalError> {
        match self.update_times.get(&update_id) {
            Some(earlier_activation_time) => {
                if &activation_time < earlier_activation_time {
                    self.unplan_update_for_time(*earlier_activation_time, update_id)
                        .int_err()?;
                    self.plan_update_for_time(activation_time, update_id);
                }
                Ok(())
            }
            None => {
                self.plan_update_for_time(activation_time, update_id);
                Ok(())
            }
        }
    }

    pub fn cancel_activation(
        &mut self,
        update_id: UpdateID,
    ) -> Result<(), TimeWheelCancelActivationError> {
        match self.update_times.get(&update_id) {
            Some(update_time) => {
                self.unplan_update_for_time(update_time.to_owned(), update_id)
                    .map_err(|e| TimeWheelCancelActivationError::SlotNotPlanned(e))?;
                Ok(())
            }
            None => Err(TimeWheelCancelActivationError::UpdateNotPlanned(
                TimeWheelUpdateNotPlannedError { update_id },
            )),
        }
    }

    pub fn spin(&mut self) {
        if !self.time_slots.is_empty() {
            self.time_slots.pop_front();
        }
    }

    fn plan_update_for_time(&mut self, activation_time: DateTime<Utc>, update_id: UpdateID) {
        match self.find_time_slot(&activation_time) {
            Some(ts) => {
                ts.planned_updates.insert(update_id);
            }
            None => {
                let mut ts = TimeSlot::new(activation_time);
                ts.planned_updates.insert(update_id);
                self.insert_time_slot(ts);
            }
        }

        self.update_times.insert(update_id, activation_time);
    }

    fn unplan_update_for_time(
        &mut self,
        activation_time: DateTime<Utc>,
        update_id: UpdateID,
    ) -> Result<(), TimeWheelSlotNotPlannedError> {
        let maybe_remove_ts_at = match self.find_time_slot(&activation_time) {
            Some(ts) => {
                ts.planned_updates.remove(&update_id);
                if ts.planned_updates.is_empty() {
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
#[error("Update {update_id} not found planned in the time wheel")]
pub(crate) struct TimeWheelUpdateNotPlannedError {
    update_id: UpdateID,
}

#[derive(Error, Debug)]
#[error("Timeslot for {activation_time} not found in the time wheel")]
pub(crate) struct TimeWheelSlotNotPlannedError {
    activation_time: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////
