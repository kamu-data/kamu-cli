// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use database_common::NoOpDatabasePlugin;
use dill::CatalogBuilder;
use kamu_resources::{
    GenericResourceQueryService,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceLifecycleMessage,
    ResourceUID,
};
use kamu_resources_inmem::{InMemoryRawResourceEventStore, InMemoryResourceRepository};
use messaging_outbox::{ConsumerFilter, Outbox, OutboxImmediateImpl, register_message_dispatcher};
use time_source::{SystemTimeSource, SystemTimeSourceStub};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Minimal harness providing the resource service infrastructure layer with
/// in-memory storage. Contains no concrete resource types — suitable as a base
/// for any harness that exercises the resource service layer.
pub struct BaseResourceServiceHarness {
    catalog: dill::Catalog,
}

impl BaseResourceServiceHarness {
    pub fn new() -> Self {
        let mut b = CatalogBuilder::new();

        b.add_value(SystemTimeSourceStub::new_set(Utc::now()))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>();

        b.add_builder(OutboxImmediateImpl::builder(ConsumerFilter::AllConsumers))
            .bind::<dyn Outbox, OutboxImmediateImpl>();

        b.add::<InMemoryResourceRepository>()
            .add::<InMemoryRawResourceEventStore>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        crate::register_dependencies(&mut b);

        register_message_dispatcher::<ResourceLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
        );

        Self { catalog: b.build() }
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    pub async fn allocate_resource_uid(&self) -> ResourceUID {
        self.catalog
            .get_one::<dyn GenericResourceQueryService>()
            .unwrap()
            .allocate_uid()
            .await
            .unwrap()
    }

    pub async fn resource_uid_by_name(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        name: &str,
    ) -> Option<ResourceUID> {
        use kamu_resources::GenericResourceQueryService;
        self.catalog
            .get_one::<dyn GenericResourceQueryService>()
            .unwrap()
            .find_resource_uid_by_name(account_id, kind, &name.to_string())
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
