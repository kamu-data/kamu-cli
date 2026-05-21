// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_configuration::{SecretSetResource, VariableSetResource};
use kamu_configuration_inmem::{
    InMemoryDatasetSecretSetBindingRepository,
    InMemoryDatasetVariableSetBindingRepository,
    InMemorySecretSetProjectionRepository,
    InMemoryVariableSetProjectionRepository,
};
use kamu_core::TenancyConfig;
use kamu_resources::{
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceLifecycleMessage,
    ResourceMetadata,
    ResourceRepository,
    ResourceSnapshot,
    ResourceUID,
};
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

    pub async fn create_resource(
        catalog: &dill::Catalog,
        account_id: &odf::AccountID,
        name: &str,
        kind: &str,
        api_version: &str,
        spec: serde_json::Value,
    ) -> ResourceUID {
        let resource_repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
        let uid = ResourceUID::new(uuid::Uuid::new_v4());

        resource_repo
            .create_resource(&ResourceSnapshot {
                uid,
                kind: kind.to_string(),
                api_version: api_version.to_string(),
                metadata: ResourceMetadata::simple(Utc::now(), account_id.clone(), name),
                spec,
                status: None,
                last_reconciled_at: None,
                last_event_id: None,
            })
            .await
            .unwrap();

        uid
    }

    pub fn dummy_variable_set_spec() -> serde_json::Value {
        serde_json::to_value(kamu_configuration::VariableSetSpec {
            variables: [(
                "PLACEHOLDER".to_string(),
                kamu_configuration::VariableSpec::Value(kamu_configuration::VariableValueSpec {
                    value: "placeholder".to_string(),
                }),
            )]
            .into(),
        })
        .unwrap()
    }

    pub fn dummy_secret_set_spec() -> serde_json::Value {
        serde_json::to_value(kamu_configuration::SecretSetSpec {
            secrets: [(
                "PLACEHOLDER".to_string(),
                kamu_configuration::SecretSpec::Value(kamu_configuration::SecretValueSpec {
                    value: "placeholder".to_string(),
                }),
            )]
            .into(),
        })
        .unwrap()
    }

    pub async fn create_variable_set_resource(
        catalog: &dill::Catalog,
        account_id: &odf::AccountID,
        name: &str,
        spec: serde_json::Value,
    ) -> ResourceUID {
        Self::create_resource(
            catalog,
            account_id,
            name,
            VariableSetResource::RESOURCE_TYPE,
            VariableSetResource::API_VERSION,
            spec,
        )
        .await
    }

    pub async fn create_secret_set_resource(
        catalog: &dill::Catalog,
        account_id: &odf::AccountID,
        name: &str,
        spec: serde_json::Value,
    ) -> ResourceUID {
        Self::create_resource(
            catalog,
            account_id,
            name,
            SecretSetResource::RESOURCE_TYPE,
            SecretSetResource::API_VERSION,
            spec,
        )
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
