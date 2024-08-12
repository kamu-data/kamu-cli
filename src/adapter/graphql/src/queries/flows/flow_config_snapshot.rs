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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub enum FlowConfigurationSnapshot {
    Batching(FlowConfigurationBatching),
    Schedule(FlowConfigurationScheduleRule),
    Compaction(FlowConfigurationCompactionRule),
    Reset(FlowConfigurationReset),
}

#[derive(SimpleObject)]
pub struct FlowConfigurationScheduleRule {
    schedule_rule: FlowConfigurationSchedule,
}

#[derive(SimpleObject)]
pub struct FlowConfigurationCompactionRule {
    compaction_rule: FlowConfigurationCompaction,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<fs::FlowConfigurationSnapshot> for FlowConfigurationSnapshot {
    fn from(value: fs::FlowConfigurationSnapshot) -> Self {
        match value {
            fs::FlowConfigurationSnapshot::Batching(batching_rule) => {
                Self::Batching(batching_rule.into())
            }
            fs::FlowConfigurationSnapshot::Reset(reset_rule) => Self::Reset(reset_rule.into()),
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
            fs::FlowConfigurationSnapshot::Compaction(compaction_rule) => {
                Self::Compaction(FlowConfigurationCompactionRule {
                    compaction_rule: match compaction_rule {
                        fs::CompactionRule::Full(compaction_full_rule) => {
                            FlowConfigurationCompaction::Full(compaction_full_rule.into())
                        }
                        fs::CompactionRule::MetadataOnly(compaction_metadata_only_rule) => {
                            FlowConfigurationCompaction::MetadataOnly(
                                compaction_metadata_only_rule.into(),
                            )
                        }
                    },
                })
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
