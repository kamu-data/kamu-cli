// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{ConfigSnapshot, Schedule};

use crate::prelude::*;

#[derive(SimpleObject)]
pub struct FlowConfigurationSnapshot {
    batching_rule: Option<FlowConfigurationBatching>,
    schedule: Option<FlowConfigurationSchedule>,
    compacting_rule: Option<FlowConfigurationCompacting>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<ConfigSnapshot> for FlowConfigurationSnapshot {
    fn from(value: ConfigSnapshot) -> Self {
        Self {
            batching_rule: value.batching_rule.map(Into::into),
            schedule: value.schedule.map(|schedule| match schedule {
                Schedule::TimeDelta(time_delta) => {
                    FlowConfigurationSchedule::TimeDelta(time_delta.every.into())
                }
                Schedule::Cron(cron) => FlowConfigurationSchedule::Cron(cron.clone().into()),
            }),
            compacting_rule: value.compacting_rule.map(Into::into),
        }
    }
}
