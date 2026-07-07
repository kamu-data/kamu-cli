// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ReconcilableStatusProjector, ResourceStatus};

use crate::{VariableSetFailureDetails, VariableSetReconcileSuccess, VariableSetSpec};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type VariableSetStatus = ResourceStatus;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VariableSetStatusProjector;

impl
    ReconcilableStatusProjector<
        VariableSetSpec,
        VariableSetReconcileSuccess,
        VariableSetFailureDetails,
    > for VariableSetStatusProjector
{
    type Status = VariableSetStatus;

    fn on_reconciliation_succeeded(
        _status: &mut Self::Status,
        _success: VariableSetReconcileSuccess,
    ) {
    }

    fn on_reconciliation_failed(_status: &mut Self::Status, _details: VariableSetFailureDetails) {}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
