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
        match value {
            fs::FlowConfigurationRule::IngestRule(ingest_rule) => Self::Ingest(ingest_rule.into()),
            fs::FlowConfigurationRule::ResetRule(reset_rule) => Self::Reset(reset_rule.into()),
            fs::FlowConfigurationRule::CompactionRule(compaction_rule) => {
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
