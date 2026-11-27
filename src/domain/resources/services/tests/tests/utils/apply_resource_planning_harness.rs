// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::CatalogBuilder;
use kamu_resources::{ResourceAggregateLoader, TypedResourceQueryService};
use kamu_resources_services::ApplyResourcePlanner;
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
    TestResource,
    TestResourceReconciler,
    register_test_resource_resource_service_layer,
};

use crate::tests::utils::ScriptedNameSuffixGenerator;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The test resource type with an [`ApplyResourcePlanner`] built over it, for
/// suites that drive the planning steps of an apply directly
#[oop::extend(BaseResourceServiceHarness, base)]
pub struct ApplyResourcePlanningHarness {
    base: BaseResourceServiceHarness,
    catalog: dill::Catalog,
    typed_query_svc: Arc<dyn TypedResourceQueryService<TestResource>>,
    aggregate_loader: Arc<dyn ResourceAggregateLoader<TestResource>>,
    name_suffix_generator: ScriptedNameSuffixGenerator,
}

impl ApplyResourcePlanningHarness {
    pub fn new(
        opts: BaseResourceServiceHarnessOpts,
        name_suffix_generator: ScriptedNameSuffixGenerator,
    ) -> Self {
        let base = BaseResourceServiceHarness::new_with_opts(opts);

        let mut b = CatalogBuilder::new_chained(base.catalog());
        register_test_resource_resource_service_layer(&mut b);
        b.add::<TestResourceReconciler>();

        let catalog = b.build();

        let typed_query_svc = catalog.get_one().unwrap();
        let aggregate_loader = catalog.get_one().unwrap();

        Self {
            base,
            catalog,
            typed_query_svc,
            aggregate_loader,
            name_suffix_generator,
        }
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    pub fn aggregate_loader(&self) -> &dyn ResourceAggregateLoader<TestResource> {
        self.aggregate_loader.as_ref()
    }

    pub fn name_suffix_generator(&self) -> &ScriptedNameSuffixGenerator {
        &self.name_suffix_generator
    }

    pub fn make_planner(&self) -> ApplyResourcePlanner<'_, TestResource> {
        ApplyResourcePlanner::new(
            self.generic_query_svc(),
            self.typed_query_svc.as_ref(),
            self.aggregate_loader.as_ref(),
            &self.name_suffix_generator,
            self.time_source(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
