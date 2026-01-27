// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu_accounts::CurrentAccountSubject;
use kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl;
use kamu_core::{MockDidGenerator, TenancyConfig};
use kamu_datasets::DatasetActionAuthorizer;
use kamu_datasets_services::testing::MockDatasetActionAuthorizer;
use messaging_outbox::{MockOutbox, Outbox};
use time_source::SystemTimeSourceStub;

use crate::testing::BaseRepoHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct BaseUseCaseHarnessOptions {
    tenancy_config: TenancyConfig,
    maybe_mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
    mock_outbox: Option<MockOutbox>,
    maybe_mock_did_generator: Option<MockDidGenerator>,
    maybe_system_time_source_stub: Option<SystemTimeSourceStub>,
    current_account_subject: CurrentAccountSubject,
}

impl BaseUseCaseHarnessOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_tenancy_config(mut self, tenancy_config: TenancyConfig) -> Self {
        self.tenancy_config = tenancy_config;
        self
    }

    pub fn with_maybe_authorizer(
        mut self,
        mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
    ) -> Self {
        self.maybe_mock_dataset_action_authorizer = mock_dataset_action_authorizer;
        self
    }

    pub fn with_outbox(mut self, mock_outbox: MockOutbox) -> Self {
        self.mock_outbox = Some(mock_outbox);
        self
    }

    pub fn without_outbox(mut self) -> Self {
        self.mock_outbox = None;
        self
    }

    pub fn with_maybe_mock_did_generator(
        mut self,
        mock_did_generator: Option<MockDidGenerator>,
    ) -> Self {
        self.maybe_mock_did_generator = mock_did_generator;
        self
    }

    pub fn with_current_account_subject(
        mut self,
        current_account_subject: CurrentAccountSubject,
    ) -> Self {
        self.current_account_subject = current_account_subject;
        self
    }

    pub fn with_system_time_source_stub(
        mut self,
        system_time_source_stub: SystemTimeSourceStub,
    ) -> Self {
        self.maybe_system_time_source_stub = Some(system_time_source_stub);
        self
    }
}

impl Default for BaseUseCaseHarnessOptions {
    fn default() -> Self {
        Self {
            tenancy_config: TenancyConfig::SingleTenant,
            maybe_mock_dataset_action_authorizer: None,
            mock_outbox: Some(MockOutbox::new()),
            maybe_mock_did_generator: None,
            maybe_system_time_source_stub: None,
            current_account_subject: CurrentAccountSubject::new_test(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
pub struct BaseUseCaseHarness {
    base_repo_harness: BaseRepoHarness,
    catalog: Catalog,
}

impl BaseUseCaseHarness {
    pub fn new(options: BaseUseCaseHarnessOptions) -> Self {
        let base_repo_harness_builder = BaseRepoHarness::builder()
            .tenancy_config(options.tenancy_config)
            .maybe_mock_did_generator(options.maybe_mock_did_generator)
            .current_account_subject(options.current_account_subject)
            .maybe_system_time_source_stub(options.maybe_system_time_source_stub);

        let base_repo_harness = base_repo_harness_builder.build();

        let catalog = {
            let mut b = dill::CatalogBuilder::new_chained(base_repo_harness.catalog());

            b.add::<RebacDatasetRegistryFacadeImpl>();

            if let Some(mock_outbox) = options.mock_outbox {
                b.add_value(mock_outbox).bind::<dyn Outbox, MockOutbox>();
            }

            if let Some(mock_dataset_action_authorizer) =
                options.maybe_mock_dataset_action_authorizer
            {
                b.add_value(mock_dataset_action_authorizer)
                    .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>();
            }

            b.build()
        };

        Self {
            base_repo_harness,
            catalog,
        }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
