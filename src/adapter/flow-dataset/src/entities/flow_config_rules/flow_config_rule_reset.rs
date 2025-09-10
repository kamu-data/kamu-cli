// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_flow_system::flow_config_struct! {
    pub struct FlowConfigRuleReset {
        pub new_head_hash: Option<odf::Multihash>,
        pub old_head_hash: Option<odf::Multihash>,
    }
    => "ResetRule"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
