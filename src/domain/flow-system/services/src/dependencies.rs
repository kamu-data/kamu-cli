// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;
use kamu_flow_system::FlowSystemEventProjectorDummy;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_dependencies(catalog_builder: &mut CatalogBuilder) {
    catalog_builder.add::<FlowConfigurationServiceImpl>();
    catalog_builder.add::<FlowTriggerServiceImpl>();
    catalog_builder.add::<FlowQueryServiceImpl>();
    catalog_builder.add::<FlowRunServiceImpl>();
    catalog_builder.add::<FlowSensorDispatcherImpl>();

    catalog_builder.add::<FlowAbortHelper>();
    catalog_builder.add::<FlowSchedulingHelper>();

    catalog_builder.add::<FlowProcessStateProjector>();
    catalog_builder.add::<FlowProcessStateIndexer>();

    catalog_builder.add::<FlowSystemEventProjectorDummy>();

    catalog_builder.add::<FlowAgentImpl>();
    catalog_builder.add::<FlowSystemEventAgentImpl>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
