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
use kamu_configuration::{
    DatasetConfigurationSetBinding,
    DatasetSecretSetBindingRepository,
    DatasetVariableSetBindingRepository,
    SecretSetProjectionRepository,
    SecretSetResource,
    VariableSetProjectionRepository,
};
use kamu_configuration_inmem::{
    InMemoryDatasetSecretSetBindingRepository,
    InMemoryDatasetVariableSetBindingRepository,
    InMemorySecretSetProjectionRepository,
    InMemoryVariableSetProjectionRepository,
};
use kamu_datasets::SecretsEncryptionConfig;
use kamu_resources::{ApplyResourceUseCase, ResourceUID};
use kamu_resources_services::testing::BaseResourceServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Harness for configuration domain service tests. Chains configuration
/// repositories and services on top of `BaseResourceServiceHarness`.
/// Reusable by any crate that needs the resource + configuration service layer
/// without a GQL adapter.
#[oop::extend(BaseResourceServiceHarness, base)]
pub struct BaseConfigurationServiceHarness {
    base: BaseResourceServiceHarness,
    catalog: dill::Catalog,
}

impl BaseConfigurationServiceHarness {
    pub fn new() -> Self {
        let base = BaseResourceServiceHarness::new();

        let mut b = CatalogBuilder::new_chained(base.catalog());

        b.add_value(SecretsEncryptionConfig::sample())
            .add::<InMemoryVariableSetProjectionRepository>()
            .add::<InMemorySecretSetProjectionRepository>()
            .add::<InMemoryDatasetVariableSetBindingRepository>()
            .add::<InMemoryDatasetSecretSetBindingRepository>();

        crate::register_dependencies(&mut b);

        let catalog = b.build();

        Self { base, catalog }
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    pub fn variable_set_binding_repo(&self) -> Arc<dyn DatasetVariableSetBindingRepository> {
        self.catalog.get_one().unwrap()
    }

    pub fn secret_set_binding_repo(&self) -> Arc<dyn DatasetSecretSetBindingRepository> {
        self.catalog.get_one().unwrap()
    }

    pub fn variable_set_projection_repo(&self) -> Arc<dyn VariableSetProjectionRepository> {
        self.catalog.get_one().unwrap()
    }

    pub fn secret_set_projection_repo(&self) -> Arc<dyn SecretSetProjectionRepository> {
        self.catalog.get_one().unwrap()
    }

    pub fn apply_secret_use_case(&self) -> Arc<dyn ApplyResourceUseCase<SecretSetResource>> {
        self.catalog.get_one().unwrap()
    }

    pub async fn allocate_resource_uid(&self) -> ResourceUID {
        self.base.allocate_resource_uid().await
    }

    pub async fn variable_bindings(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Vec<DatasetConfigurationSetBinding> {
        self.variable_set_binding_repo()
            .list_bindings(dataset_id)
            .await
            .unwrap()
    }

    pub async fn secret_bindings(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Vec<DatasetConfigurationSetBinding> {
        self.secret_set_binding_repo()
            .list_bindings(dataset_id)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
