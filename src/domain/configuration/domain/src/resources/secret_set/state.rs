// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{EventSourcedResourceState, ResourceStateModel};

use crate::{SecretSetSpec, SecretSetSpecInput};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type SecretSetState = EventSourcedResourceState<SecretSetStateModel>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SecretSetStateModel {}

impl ResourceStateModel for SecretSetStateModel {
    type Spec = SecretSetSpec;
    type SpecInput = SecretSetSpecInput;
    type State = SecretSetState;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
