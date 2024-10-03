// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{CatalogBuilder, Component};
use kamu_task_system::TaskLogicalPlanRunner;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_dependencies(catalog_builder: &mut CatalogBuilder, in_multi_tenant_mode: bool) {
    catalog_builder.add::<TaskExecutorImpl>();
    catalog_builder.add::<TaskSchedulerImpl>();

    catalog_builder.add_builder(
        TaskLogicalPlanRunnerImpl::builder().with_in_multi_tenant_mode(in_multi_tenant_mode),
    );
    catalog_builder.bind::<dyn TaskLogicalPlanRunner, TaskLogicalPlanRunnerImpl>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
