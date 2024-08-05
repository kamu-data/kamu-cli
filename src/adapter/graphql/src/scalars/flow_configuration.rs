// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{
    BatchingRule,
    CompactionRule,
    CompactionRuleFull,
    CompactionRuleMetadataOnly,
    FlowConfigurationRule,
    FlowConfigurationSnapshot,
    IngestRule,
    Schedule,
    ScheduleCron,
    ScheduleCronError,
    ScheduleTimeDelta,
};

use crate::mutations::FlowInvalidRunConfigurations;
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowConfiguration {
    pub paused: bool,
    pub ingest: Option<FlowConfigurationIngest>,
    pub batching: Option<FlowConfigurationBatching>,
    pub compaction: Option<FlowConfigurationCompaction>,
}

impl From<kamu_flow_system::FlowConfigurationState> for FlowConfiguration {
    fn from(value: kamu_flow_system::FlowConfigurationState) -> Self {
        Self {
            paused: !value.is_active(),
            batching: if let FlowConfigurationRule::BatchingRule(condition) = &value.rule {
                Some((*condition).into())
            } else {
                None
            },
            ingest: if let FlowConfigurationRule::IngestRule(ingest_rule) = &value.rule {
                Some(ingest_rule.clone().into())
            } else {
                None
            },
            compaction: if let FlowConfigurationRule::CompactionRule(compaction_args) = &value.rule
            {
                match compaction_args {
                    CompactionRule::Full(compaction_rule) => {
                        Some(FlowConfigurationCompaction::Full((*compaction_rule).into()))
                    }
                    CompactionRule::MetadataOnly(compaction_rule) => Some(
                        FlowConfigurationCompaction::MetadataOnly((*compaction_rule).into()),
                    ),
                }
            } else {
                None
            },
        }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowConfigurationIngest {
    pub schedule: FlowConfigurationSchedule,
    pub fetch_uncacheable: bool,
}

impl From<IngestRule> for FlowConfigurationIngest {
    fn from(value: IngestRule) -> Self {
        Self {
            fetch_uncacheable: value.fetch_uncacheable,
            schedule: match value.schedule_condition {
                Schedule::TimeDelta(time_delta) => {
                    FlowConfigurationSchedule::TimeDelta(time_delta.every.into())
                }
                Schedule::Cron(cron) => FlowConfigurationSchedule::Cron(cron.clone().into()),
            },
        }
    }
}

#[derive(Union, Clone, PartialEq, Eq)]
pub enum FlowConfigurationSchedule {
    TimeDelta(TimeDelta),
    Cron(Cron5ComponentExpression),
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowConfigurationBatching {
    pub min_records_to_await: u64,
    pub max_batching_interval: TimeDelta,
}

impl From<BatchingRule> for FlowConfigurationBatching {
    fn from(value: BatchingRule) -> Self {
        Self {
            min_records_to_await: value.min_records_to_await(),
            max_batching_interval: (*value.max_batching_interval()).into(),
        }
    }
}

#[derive(Union, Clone, PartialEq, Eq)]
pub enum FlowConfigurationCompaction {
    Full(CompactionFull),
    MetadataOnly(CompactionMetadataOnly),
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct CompactionFull {
    pub max_slice_size: u64,
    pub max_slice_records: u64,
    pub recursive: bool,
}

impl From<CompactionRuleFull> for CompactionFull {
    fn from(value: CompactionRuleFull) -> Self {
        Self {
            max_slice_records: value.max_slice_records(),
            max_slice_size: value.max_slice_size(),
            recursive: value.recursive(),
        }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct CompactionMetadataOnly {
    pub recursive: bool,
}

impl From<CompactionRuleMetadataOnly> for CompactionMetadataOnly {
    fn from(value: CompactionRuleMetadataOnly) -> Self {
        Self {
            recursive: value.recursive,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct Cron5ComponentExpression {
    pub cron_5component_expression: String,
}

impl From<ScheduleCron> for Cron5ComponentExpression {
    fn from(value: ScheduleCron) -> Self {
        Self {
            cron_5component_expression: value.source_5component_cron_expression,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct TimeDelta {
    pub every: i64,
    pub unit: TimeUnit,
}

#[derive(Enum, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Minutes,
    Hours,
    Days,
    Weeks,
}

impl From<chrono::Duration> for TimeDelta {
    fn from(value: chrono::Duration) -> Self {
        assert!(
            value.num_seconds() > 0,
            "Positive interval expected, but received [{value}]"
        );

        let num_weeks = value.num_weeks();
        if (value - chrono::Duration::try_weeks(num_weeks).unwrap()).is_zero() {
            return Self {
                every: num_weeks,
                unit: TimeUnit::Weeks,
            };
        }

        let num_days = value.num_days();
        if (value - chrono::Duration::try_days(num_days).unwrap()).is_zero() {
            return Self {
                every: num_days,
                unit: TimeUnit::Days,
            };
        }

        let num_hours = value.num_hours();
        if (value - chrono::Duration::try_hours(num_hours).unwrap()).is_zero() {
            return Self {
                every: num_hours,
                unit: TimeUnit::Hours,
            };
        }

        let num_minutes = value.num_minutes();
        if (value - chrono::Duration::try_minutes(num_minutes).unwrap()).is_zero() {
            return Self {
                every: num_minutes,
                unit: TimeUnit::Minutes,
            };
        }

        panic!(
            "Expecting intervals not smaller than 1 minute that are clearly dividable by unit, \
             but received [{value}]"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject)]
pub enum FlowRunConfiguration {
    Schedule(ScheduleInput),
    Batching(BatchingConditionInput),
    Compaction(CompactionConditionInput),
    Ingest(IngestConditionInput),
}

#[derive(OneofObject, Clone)]
pub enum ScheduleInput {
    TimeDelta(TimeDeltaInput),
    /// Supported CRON syntax: min hour dayOfMonth month dayOfWeek
    Cron5ComponentExpression(String),
}

#[derive(InputObject, Clone)]
pub struct TimeDeltaInput {
    pub every: u32,
    pub unit: TimeUnit,
}

impl From<TimeDeltaInput> for chrono::Duration {
    fn from(value: TimeDeltaInput) -> Self {
        let every = i64::from(value.every);
        match value.unit {
            TimeUnit::Weeks => chrono::Duration::try_weeks(every).unwrap(),
            TimeUnit::Days => chrono::Duration::try_days(every).unwrap(),
            TimeUnit::Hours => chrono::Duration::try_hours(every).unwrap(),
            TimeUnit::Minutes => chrono::Duration::try_minutes(every).unwrap(),
        }
    }
}

impl From<&TimeDeltaInput> for chrono::Duration {
    fn from(value: &TimeDeltaInput) -> Self {
        let every = i64::from(value.every);
        match value.unit {
            TimeUnit::Weeks => chrono::Duration::try_weeks(every).unwrap(),
            TimeUnit::Days => chrono::Duration::try_days(every).unwrap(),
            TimeUnit::Hours => chrono::Duration::try_hours(every).unwrap(),
            TimeUnit::Minutes => chrono::Duration::try_minutes(every).unwrap(),
        }
    }
}

#[derive(InputObject)]
pub struct BatchingConditionInput {
    pub min_records_to_await: u64,
    pub max_batching_interval: TimeDeltaInput,
}

#[derive(OneofObject)]
pub enum CompactionConditionInput {
    Full(CompactionConditionFull),
    MetadataOnly(CompactionConditionMetadataOnly),
}

#[derive(InputObject, Clone)]
pub struct CompactionConditionFull {
    pub max_slice_size: u64,
    pub max_slice_records: u64,
    pub recursive: bool,
}

#[derive(InputObject)]
pub struct CompactionConditionMetadataOnly {
    pub recursive: bool,
}

#[derive(InputObject, Clone)]
pub struct IngestConditionInput {
    pub fetch_uncacheable: bool,
    pub schedule: ScheduleInput,
}

impl TryFrom<IngestConditionInput> for IngestRule {
    type Error = ScheduleCronError;

    fn try_from(value: IngestConditionInput) -> std::result::Result<Self, Self::Error> {
        let schedule = match value.schedule {
            ScheduleInput::TimeDelta(td) => {
                Schedule::TimeDelta(ScheduleTimeDelta { every: td.into() })
            }
            ScheduleInput::Cron5ComponentExpression(cron_5component_expression) => {
                Schedule::try_from_5component_cron_expression(&cron_5component_expression)?
            }
        };
        Ok(Self {
            fetch_uncacheable: value.fetch_uncacheable,
            schedule_condition: schedule,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowRunConfiguration {
    pub fn try_into_snapshot(
        &self,
        dataset_flow_type: DatasetFlowType,
    ) -> Result<FlowConfigurationSnapshot, FlowInvalidRunConfigurations> {
        Ok(match self {
            Self::Batching(batching_input) => {
                if dataset_flow_type != DatasetFlowType::ExecuteTransform {
                    return Err(FlowInvalidRunConfigurations {
                        error: "Incompatible flow run configuration and dataset flow type"
                            .to_string(),
                    });
                };
                FlowConfigurationSnapshot::Batching(
                    BatchingRule::new_checked(
                        batching_input.min_records_to_await,
                        batching_input.max_batching_interval.clone().into(),
                    )
                    .map_err(|_| FlowInvalidRunConfigurations {
                        error: "Invalid batching flow run configuration".to_string(),
                    })?,
                )
            }
            Self::Ingest(ingest_input) => {
                if dataset_flow_type != DatasetFlowType::Ingest {
                    return Err(FlowInvalidRunConfigurations {
                        error: "Incompatible flow run configuration and dataset flow type"
                            .to_string(),
                    });
                };
                FlowConfigurationSnapshot::Ingest(ingest_input.clone().try_into().map_err(
                    |_| FlowInvalidRunConfigurations {
                        error: "Invalid ingest flow run configuration".to_string(),
                    },
                )?)
            }
            Self::Compaction(compaction_input) => {
                if dataset_flow_type != DatasetFlowType::HardCompaction {
                    return Err(FlowInvalidRunConfigurations {
                        error: "Incompatible flow run configuration and dataset flow type"
                            .to_string(),
                    });
                };
                FlowConfigurationSnapshot::Compaction(match compaction_input {
                    CompactionConditionInput::Full(compaction_input) => CompactionRule::Full(
                        CompactionRuleFull::new_checked(
                            compaction_input.max_slice_size,
                            compaction_input.max_slice_records,
                            compaction_input.recursive,
                        )
                        .map_err(|_| FlowInvalidRunConfigurations {
                            error: "Invalid compaction flow run configuration".to_string(),
                        })?,
                    ),
                    CompactionConditionInput::MetadataOnly(compaction_input) => {
                        CompactionRule::MetadataOnly(CompactionRuleMetadataOnly {
                            recursive: compaction_input.recursive,
                        })
                    }
                })
            }
            Self::Schedule(schedule_input) => {
                if dataset_flow_type != DatasetFlowType::Ingest {
                    return Err(FlowInvalidRunConfigurations {
                        error: "Incompatible flow run configuration and dataset flow type"
                            .to_string(),
                    });
                };
                FlowConfigurationSnapshot::Schedule(match schedule_input {
                    ScheduleInput::TimeDelta(td) => {
                        Schedule::TimeDelta(ScheduleTimeDelta { every: td.into() })
                    }
                    ScheduleInput::Cron5ComponentExpression(cron_5component_expression) => {
                        Schedule::try_from_5component_cron_expression(cron_5component_expression)
                            .map_err(|_| FlowInvalidRunConfigurations {
                                error: "Invalid schedule flow run configuration".to_string(),
                            })?
                    }
                })
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
