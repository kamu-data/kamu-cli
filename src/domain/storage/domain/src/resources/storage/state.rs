// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ReconcilableResourceState, ReconcilableStateModel};

use crate::{StorageFailureDetails, StorageReconcileSuccess, StorageSpec};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type StorageState = ReconcilableResourceState<StorageStateModel>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct StorageStateModel {}

impl ReconcilableStateModel for StorageStateModel {
    type Spec = StorageSpec;
    type SpecInput = StorageSpec;
    type Success = StorageReconcileSuccess;
    type FailureDetails = StorageFailureDetails;
    type State = StorageState;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
