// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ReconcilableResourceState, ReconcilableStateModel};

use crate::{
    SecretSetFailureDetails,
    SecretSetReconcileSuccess,
    SecretSetSpec,
    SecretSetStatus,
    SecretSetStatusProjector,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type SecretSetState = ReconcilableResourceState<SecretSetStateModel>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SecretSetStateModel {}

impl ReconcilableStateModel for SecretSetStateModel {
    type Spec = SecretSetSpec;
    type Status = SecretSetStatus;
    type Success = SecretSetReconcileSuccess;
    type FailureDetails = SecretSetFailureDetails;
    type State = SecretSetState;
    type StatusProjector = SecretSetStatusProjector;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
