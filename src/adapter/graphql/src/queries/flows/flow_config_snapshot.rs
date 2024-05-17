// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{FlowConfigSnapshot, Schedule};

use crate::prelude::*;

#[derive(Union)]
pub enum FlowConfigurationSnapshot {
    Batching(FlowConfigurationBatching),
    Schedule(FlowConfigurationScheduleRule),
    Compacting(FlowConfigurationCompacting),
}

#[derive(SimpleObject)]
pub struct FlowConfigurationScheduleRule {
    schedule_rule: FlowConfigurationSchedule,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<FlowConfigSnapshot> for FlowConfigurationSnapshot {
    fn from(value: FlowConfigSnapshot) -> Self {
        match value {
            FlowConfigSnapshot::Batching(batching_rule) => Self::Batching(batching_rule.into()),
            FlowConfigSnapshot::Schedule(schedule) => {
                Self::Schedule(FlowConfigurationScheduleRule {
                    schedule_rule: match schedule {
                        Schedule::TimeDelta(time_delta) => {
                            FlowConfigurationSchedule::TimeDelta(time_delta.every.into())
                        }
                        Schedule::Cron(cron) => {
                            FlowConfigurationSchedule::Cron(cron.clone().into())
                        }
                    },
                })
            }
            FlowConfigSnapshot::Compacting(compacting_rule) => {
                Self::Compacting(compacting_rule.into())
            }
        }
    }
}
