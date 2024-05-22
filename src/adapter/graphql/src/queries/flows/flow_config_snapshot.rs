// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system as fs;

use crate::prelude::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub enum FlowConfigurationSnapshot {
    Batching(FlowConfigurationBatching),
    Schedule(FlowConfigurationScheduleRule),
    Compacting(FlowConfigurationCompactingRule),
}

#[derive(SimpleObject)]
pub struct FlowConfigurationScheduleRule {
    schedule_rule: FlowConfigurationSchedule,
}

#[derive(SimpleObject)]
pub struct FlowConfigurationCompactingRule {
    compacting_rule: FlowConfigurationCompacting,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<fs::FlowConfigurationSnapshot> for FlowConfigurationSnapshot {
    fn from(value: fs::FlowConfigurationSnapshot) -> Self {
        match value {
            fs::FlowConfigurationSnapshot::Batching(batching_rule) => {
                Self::Batching(batching_rule.into())
            }
            fs::FlowConfigurationSnapshot::Schedule(schedule) => {
                Self::Schedule(FlowConfigurationScheduleRule {
                    schedule_rule: match schedule {
                        fs::Schedule::TimeDelta(time_delta) => {
                            FlowConfigurationSchedule::TimeDelta(time_delta.every.into())
                        }
                        fs::Schedule::Cron(cron) => {
                            FlowConfigurationSchedule::Cron(cron.clone().into())
                        }
                    },
                })
            }
            fs::FlowConfigurationSnapshot::Compacting(compacting_rule) => {
                Self::Compacting(FlowConfigurationCompactingRule {
                    compacting_rule: match compacting_rule {
                        fs::CompactingRule::Full(compacting_full_rule) => {
                            FlowConfigurationCompacting::Full(compacting_full_rule.into())
                        }
                        fs::CompactingRule::MetadataOnly(compacting_metadata_only_rule) => {
                            FlowConfigurationCompacting::MetadataOnly(
                                compacting_metadata_only_rule.into(),
                            )
                        }
                    },
                })
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
