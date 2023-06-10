// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use chrono::{DateTime, Utc};
use enum_variants::*;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEventInstance {
    pub event_id: TaskEventID,
    pub event_time: DateTime<Utc>,
    pub event: TaskEvent,
}

// TODO: Replace the below with helper macros
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEventInstanceTRef<'a, T>
where
    T: VariantOf<TaskEvent>,
    T: Clone,
{
    pub event_id: TaskEventID,
    pub event_time: DateTime<Utc>,
    pub event: Cow<'a, T>,
}

pub type TaskEventInstanceT<T> = TaskEventInstanceTRef<'static, T>;

impl<'a, T> Into<TaskEventInstance> for TaskEventInstanceTRef<'a, T>
where
    T: VariantOf<TaskEvent>,
    T: Clone,
{
    fn into(self) -> TaskEventInstance {
        let TaskEventInstanceTRef {
            event_id,
            event_time,
            event,
        } = self;
        TaskEventInstance {
            event_id,
            event_time,
            event: event.into_owned().into(),
        }
    }
}

impl TaskEventInstance {
    pub fn as_typed<'a, T>(&'a self) -> Option<TaskEventInstanceTRef<'a, T>>
    where
        T: VariantOf<TaskEvent>,
        T: Clone,
    {
        match self.event.as_variant() {
            None => None,
            Some(event) => Some(TaskEventInstanceTRef {
                event_id: self.event_id,
                event_time: self.event_time,
                event: Cow::Borrowed(event),
            }),
        }
    }

    pub fn into_typed<T>(self) -> Option<TaskEventInstanceT<T>>
    where
        T: VariantOf<TaskEvent>,
        T: Clone,
    {
        match self.event.into_variant() {
            None => None,
            Some(event) => Some(TaskEventInstanceT {
                event_id: self.event_id,
                event_time: self.event_time,
                event: Cow::Owned(event),
            }),
        }
    }
}
