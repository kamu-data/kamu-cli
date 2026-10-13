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

/// Actions the current account is allowed to perform on a dataset
#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetPermissions {
    /// Permissions for managing access of other accounts
    pub collaboration: DatasetCollaborationPermissions,
    /// Permissions for dataset environment variables
    pub env_vars: DatasetEnvVarsPermissions,
    /// Permissions for dataset flows
    pub flows: DatasetFlowsPermissions,
    /// Permissions for dataset webhooks
    pub webhooks: DatasetWebhooksPermissions,
    /// Permissions for general dataset management
    pub general: DatasetGeneralPermissions,
    /// Permissions for dataset metadata
    pub metadata: DatasetMetadataPermissions,
}

/// Permissions for managing access of other accounts to a dataset
#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetCollaborationPermissions {
    /// Whether the account can see who has access to the dataset
    pub can_view: bool,
    /// Whether the account can grant and revoke access to the dataset
    pub can_update: bool,
}

/// Permissions for environment variables of a dataset
#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetEnvVarsPermissions {
    /// Whether the account can see the environment variables
    pub can_view: bool,
    /// Whether the account can change the environment variables
    pub can_update: bool,
}

/// Permissions for flows of a dataset
#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetFlowsPermissions {
    /// Whether the account can see the flows and their history
    pub can_view: bool,
    /// Whether the account can trigger and configure the flows
    pub can_run: bool,
}

/// Permissions for webhooks of a dataset
#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetWebhooksPermissions {
    /// Whether the account can see the webhook subscriptions
    pub can_view: bool,
    /// Whether the account can change the webhook subscriptions
    pub can_update: bool,
}

/// Permissions for general management of a dataset
#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetGeneralPermissions {
    /// Whether the account can rename the dataset
    pub can_rename: bool,
    /// Whether the account can change the dataset visibility
    pub can_set_visibility: bool,
    /// Whether the account can delete the dataset
    pub can_delete: bool,
}

/// Permissions for metadata of a dataset
#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetMetadataPermissions {
    /// Whether the account can commit new metadata events
    pub can_commit: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
