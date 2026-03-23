// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::{ReconcilableResourceEvent, SecretSetReconcileSuccess, SecretSetSpec, SecretSetStats};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type SecretSetEvent =
    ReconcilableResourceEvent<SecretSetSpec, SecretSetReconcileSuccess, SecretSetFailureDetails>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretSetFailureDetails {
    pub stats: SecretSetStats,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
