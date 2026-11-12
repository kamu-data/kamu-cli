// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::{VariableSetResource, VariableSetStateModel};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Persisted in the controller bookkeeping and used as a metric label: changing
/// it orphans the bookkeeping of every resource of the type
pub const VARIABLE_SET_CONTROLLER_NAME: &str =
    "dev.kamu.domain.configuration.VariableSetController";

kamu_resources_services::declare_resource_service_layer!(
    domain = kamu_configuration,
    name = VariableSet,
    resource = VariableSetResource,
    state_model = VariableSetStateModel,
    controller_name = VARIABLE_SET_CONTROLLER_NAME
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
