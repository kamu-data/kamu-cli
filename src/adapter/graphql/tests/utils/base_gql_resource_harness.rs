// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration_inmem::{
    InMemoryDatasetSecretSetBindingRepository,
    InMemoryDatasetVariableSetBindingRepository,
    InMemorySecretSetProjectionRepository,
    InMemoryVariableSetProjectionRepository,
};
use kamu_core::TenancyConfig;
use kamu_resources::{MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE, ResourceLifecycleMessage};
use kamu_resources_inmem::{InMemoryRawResourceEventStore, InMemoryResourceRepository};
use messaging_outbox::{OutboxProvider, register_message_dispatcher};

use crate::utils::BaseGQLDatasetHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
pub struct BaseGQLResourceHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    pub catalog_base: dill::Catalog,
}

impl BaseGQLResourceHarness {
    pub fn new(base_gql_harness: BaseGQLDatasetHarness, catalog: dill::Catalog) -> Self {
        Self {
            base_gql_harness,
            catalog_base: catalog,
        }
    }

    pub fn make_base_gql_resource_catalog(base_catalog: &dill::Catalog) -> dill::Catalog {
        let mut b = dill::CatalogBuilder::new_chained(base_catalog);

        b.add::<InMemoryResourceRepository>()
            .add::<InMemoryRawResourceEventStore>()
            .add::<InMemoryDatasetVariableSetBindingRepository>()
            .add::<InMemoryDatasetSecretSetBindingRepository>()
            .add::<InMemoryVariableSetProjectionRepository>()
            .add::<InMemorySecretSetProjectionRepository>();

        kamu_resources_services::register_dependencies(&mut b);
        kamu_configuration_services::register_dependencies(&mut b);

        register_message_dispatcher::<ResourceLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
        );

        b.build()
    }

    pub fn new_with_config(tenancy_config: TenancyConfig, outbox_provider: OutboxProvider) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(tenancy_config)
            .outbox_provider(outbox_provider)
            .build();

        let catalog_base = Self::make_base_gql_resource_catalog(base_gql_harness.catalog());

        Self::new(base_gql_harness, catalog_base)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
