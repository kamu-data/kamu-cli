// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;
use database_common::NoOpDatabasePlugin;
use dill::CatalogBuilder;
use kamu_resources::{
    GenericResourceQueryService,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceLifecycleMessage,
    ResourceMetadataInput,
    ResourceRepository,
    ResourceUID,
};
use kamu_resources_inmem::{InMemoryRawResourceEventStore, InMemoryResourceRepository};
use messaging_outbox::{Outbox, OutboxProvider, register_message_dispatcher};
use time_source::{SystemTimeSource, SystemTimeSourceStub};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Minimal harness providing the resource service infrastructure layer with
/// in-memory storage. Contains no concrete resource types — suitable as a base
/// for any harness that exercises the resource service layer.
pub struct BaseResourceServiceHarness {
    catalog: dill::Catalog,
    generic_query_svc: Arc<dyn GenericResourceQueryService>,
    resource_repo: Arc<dyn ResourceRepository>,
    outbox: Arc<dyn Outbox>,
    time_source: Arc<dyn SystemTimeSource>,
}

pub struct BaseResourceServiceHarnessOpts {
    pub outbox_provider: OutboxProvider,
}

impl Default for BaseResourceServiceHarnessOpts {
    fn default() -> Self {
        Self {
            outbox_provider: OutboxProvider::Immediate {
                force_immediate: true,
            },
        }
    }
}

impl BaseResourceServiceHarness {
    pub fn new() -> Self {
        Self::new_with_opts(BaseResourceServiceHarnessOpts::default())
    }

    pub fn new_with_opts(opts: BaseResourceServiceHarnessOpts) -> Self {
        let mut b = CatalogBuilder::new();

        b.add_value(SystemTimeSourceStub::new_set(Utc::now()))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>();

        opts.outbox_provider.embed_into_catalog(&mut b);

        b.add::<InMemoryResourceRepository>()
            .add::<InMemoryRawResourceEventStore>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        crate::register_dependencies(&mut b);

        register_message_dispatcher::<ResourceLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
        );

        let catalog = b.build();

        let generic_query_svc = catalog.get_one().unwrap();
        let resource_repo = catalog.get_one().unwrap();

        let outbox = catalog.get_one().unwrap();
        let time_source = catalog.get_one().unwrap();

        Self {
            catalog,
            generic_query_svc,
            resource_repo,
            outbox,
            time_source,
        }
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    pub fn generic_query_svc(&self) -> &dyn GenericResourceQueryService {
        self.generic_query_svc.as_ref()
    }

    pub fn outbox(&self) -> &dyn Outbox {
        self.outbox.as_ref()
    }

    pub fn time_source(&self) -> &dyn SystemTimeSource {
        self.time_source.as_ref()
    }

    pub fn resource_repo(&self) -> &dyn ResourceRepository {
        self.resource_repo.as_ref()
    }

    pub async fn allocate_resource_uid(&self) -> ResourceUID {
        self.generic_query_svc.allocate_uid().await.unwrap()
    }

    pub fn make_metadata_input(account_id: odf::AccountID, name: &str) -> ResourceMetadataInput {
        ResourceMetadataInput {
            account: account_id,
            name: name.to_string(),
            description: None,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        }
    }

    pub async fn resource_uid_by_name(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        name: &str,
    ) -> Option<ResourceUID> {
        self.generic_query_svc
            .find_resource_uid_by_name(account_id, kind, &name.to_string())
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
