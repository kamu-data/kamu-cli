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
#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetPermissions {
    pub collaboration: DatasetCollaborationPermissions,
    pub env_vars: DatasetEnvVarsPermissions,
    pub flows: DatasetFlowsPermissions,
    pub general: DatasetGeneralPermissions,
    pub metadata: DatasetMetadataPermissions,
}

#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetCollaborationPermissions {
    pub can_view: bool,
    pub can_update: bool,
}

#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetEnvVarsPermissions {
    pub can_view: bool,
    pub can_update: bool,
}

#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetFlowsPermissions {
    pub can_view: bool,
    pub can_run: bool,
}

#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetGeneralPermissions {
    pub can_rename: bool,
    pub can_set_visibility: bool,
    pub can_delete: bool,
}

#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetMetadataPermissions {
    pub can_commit: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
