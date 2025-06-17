// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_flow_dataset::{FlowConfigRuleCompact, FlowConfigRuleIngest, FlowConfigRuleReset};
use kamu_flow_system as fs;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub enum FlowConfigurationSnapshot {
    Compaction(FlowConfigurationCompactionRule),
    Ingest(FlowConfigurationIngest),
    Reset(FlowConfigurationReset),
}

#[derive(SimpleObject)]
pub struct FlowConfigurationCompactionRule {
    compaction_rule: FlowConfigurationCompaction,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<fs::FlowConfigurationRule> for FlowConfigurationSnapshot {
    fn from(value: fs::FlowConfigurationRule) -> Self {
        match value.rule_type.as_str() {
            FlowConfigRuleIngest::TYPE_ID => {
                let ingest_rule = FlowConfigRuleIngest::from_flow_config(&value).unwrap();
                Self::Ingest(ingest_rule.into())
            }

            FlowConfigRuleReset::TYPE_ID => {
                let reset_rule = FlowConfigRuleReset::from_flow_config(&value).unwrap();
                Self::Reset(reset_rule.into())
            }

            FlowConfigRuleCompact::TYPE_ID => {
                let compaction_rule = FlowConfigRuleCompact::from_flow_config(&value).unwrap();
                Self::Compaction(match compaction_rule {
                    FlowConfigRuleCompact::Full(full_rule) => FlowConfigurationCompactionRule {
                        compaction_rule: FlowConfigurationCompaction::Full(full_rule.into()),
                    },
                    FlowConfigRuleCompact::MetadataOnly { recursive } => {
                        FlowConfigurationCompactionRule {
                            compaction_rule: FlowConfigurationCompaction::MetadataOnly(
                                CompactionMetadataOnly { recursive },
                            ),
                        }
                    }
                })
            }

            _ => panic!(
                "Unsupported flow configuration rule type: {}",
                value.rule_type
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
