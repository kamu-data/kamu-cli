// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{EventSourcedResourceState, ResourceStateModel};

use crate::{VariableSetSpec, VariableSetSpecInput};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type VariableSetState = EventSourcedResourceState<VariableSetStateModel>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VariableSetStateModel {}

impl ResourceStateModel for VariableSetStateModel {
    type Spec = VariableSetSpec;
    type SpecInput = VariableSetSpecInput;
    type State = VariableSetState;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
