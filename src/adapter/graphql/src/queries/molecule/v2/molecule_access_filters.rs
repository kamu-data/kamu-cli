// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Clone)]
pub struct MoleculeAccessLevelRuleInput {
    pub ipnft_uid: Option<String>,
    pub access_levels: Vec<String>,
}

impl From<MoleculeAccessLevelRuleInput> for kamu_molecule_domain::MoleculeAccessLevelRule {
    fn from(value: MoleculeAccessLevelRuleInput) -> Self {
        Self {
            ipnft_uid: value.ipnft_uid,
            access_levels: value.access_levels,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
