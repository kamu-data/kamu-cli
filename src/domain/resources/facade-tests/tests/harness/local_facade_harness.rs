// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use database_common::NoOpDatabasePlugin;
use dill::CatalogBuilder;
use kamu_accounts::{
    AccountConfig,
    CurrentAccountSubject,
    DidSecretEncryptionConfig,
    JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
    PredefinedAccountsConfig,
};
use kamu_accounts_inmem::{
    InMemoryAccountQuotaEventStore,
    InMemoryAccountRepository,
    InMemoryDidSecretKeyRepository,
};
use kamu_accounts_services::{
    AccountQuotaServiceImpl,
    AccountServiceImpl,
    CreateAccountUseCaseImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
    UpdateAccountUseCaseImpl,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    DeleteDatasetRebacPropertiesUseCaseImpl,
    RebacServiceImpl,
    SetDatasetRebacPropertiesUseCaseImpl,
};
use kamu_configuration_inmem::{
    InMemoryDatasetSecretSetBindingRepository,
    InMemoryDatasetVariableSetBindingRepository,
    InMemorySecretSetProjectionRepository,
    InMemoryVariableSetProjectionRepository,
};
use kamu_datasets::SecretsEncryptionConfig;
use kamu_resources::{MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE, ResourceLifecycleMessage};
use kamu_resources_facade::ResourceFacade;
use kamu_resources_inmem::{InMemoryRawResourceEventStore, InMemoryResourceRepository};
use messaging_outbox::{OutboxAgent, OutboxProvider, register_message_dispatcher};
use strum::IntoEnumIterator;
use time_source::SystemTimeSourceProvider;

use super::facade_harness_trait::{FacadeContractHarness, TestAccount};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct LocalFacadeHarnessOpts {
    pub outbox_provider: OutboxProvider,
}

impl Default for LocalFacadeHarnessOpts {
    fn default() -> Self {
        Self {
            outbox_provider: OutboxProvider::Immediate {
                force_immediate: true,
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct LocalFacadeHarness {
    base_catalog: dill::Catalog,
    alice_account_id: odf::AccountID,
    bob_account_id: odf::AccountID,
    outbox_agent: Option<Arc<dyn OutboxAgent>>,
}

impl LocalFacadeHarness {
    pub async fn new() -> Self {
        Self::new_with_opts(LocalFacadeHarnessOpts::default()).await
    }

    pub async fn new_with_opts(opts: LocalFacadeHarnessOpts) -> Self {
        let subjects: Vec<(TestAccount, CurrentAccountSubject)> = TestAccount::iter()
            .map(|a| {
                let subject = CurrentAccountSubject::new_test_with(&a);
                (a, subject)
            })
            .collect();

        let alice_account_id = subjects
            .iter()
            .find(|(a, _)| *a == TestAccount::Alice)
            .unwrap()
            .1
            .account_id()
            .clone();
        let bob_account_id = subjects
            .iter()
            .find(|(a, _)| *a == TestAccount::Bob)
            .unwrap()
            .1
            .account_id()
            .clone();

        let is_dispatching = matches!(opts.outbox_provider, OutboxProvider::Dispatching);
        let base_catalog = Self::build_base_catalog(&subjects, opts.outbox_provider).await;

        let outbox_agent = if is_dispatching {
            let agent = base_catalog.get_one::<dyn OutboxAgent>().unwrap();
            agent.run_initialization().await.unwrap();
            Some(agent)
        } else {
            None
        };

        Self {
            base_catalog,
            alice_account_id,
            bob_account_id,
            outbox_agent,
        }
    }

    async fn build_base_catalog(
        subjects: &[(TestAccount, CurrentAccountSubject)],
        outbox_provider: OutboxProvider,
    ) -> dill::Catalog {
        let mut predefined_accounts_config = PredefinedAccountsConfig::new();
        for (_, subject) in subjects {
            let CurrentAccountSubject::Logged(logged) = subject else {
                unreachable!();
            };
            predefined_accounts_config
                .predefined
                .push(AccountConfig::test_config_from_subject(logged.clone()));
        }

        let mut b = CatalogBuilder::new();

        // Time source
        SystemTimeSourceProvider::default().embed_into_catalog(&mut b);

        // Database noop
        NoOpDatabasePlugin::init_database_components(&mut b);

        // Outbox
        let needs_bridge = matches!(outbox_provider, OutboxProvider::Dispatching);
        outbox_provider.embed_into_catalog(&mut b);
        if needs_bridge {
            b.add::<kamu_messaging_outbox_inmem::InMemoryOutboxMessageBridge>();
        }

        // Accounts
        b.add::<InMemoryAccountRepository>()
            .add::<InMemoryAccountQuotaEventStore>()
            .add::<InMemoryDidSecretKeyRepository>()
            .add::<LoginPasswordAuthProvider>()
            .add::<PredefinedAccountsRegistrator>()
            .add::<AccountServiceImpl>()
            .add::<AccountQuotaServiceImpl>()
            .add::<CreateAccountUseCaseImpl>()
            .add::<UpdateAccountUseCaseImpl>()
            .add_value(DidSecretEncryptionConfig::sample())
            .add_value(predefined_accounts_config);

        // ReBac
        b.add::<InMemoryRebacRepository>()
            .add::<RebacServiceImpl>()
            .add::<SetDatasetRebacPropertiesUseCaseImpl>()
            .add::<DeleteDatasetRebacPropertiesUseCaseImpl>()
            .add_value(DefaultAccountProperties::default())
            .add_value(DefaultDatasetProperties::default());

        // Resources
        b.add::<InMemoryResourceRepository>()
            .add::<InMemoryRawResourceEventStore>();

        // Configuration
        b.add::<InMemoryVariableSetProjectionRepository>()
            .add::<InMemorySecretSetProjectionRepository>()
            .add::<InMemoryDatasetVariableSetBindingRepository>()
            .add::<InMemoryDatasetSecretSetBindingRepository>()
            .add_value(SecretsEncryptionConfig::sample());

        kamu_resources_services::register_dependencies(&mut b);
        kamu_configuration_services::register_dependencies(&mut b);

        register_message_dispatcher::<ResourceLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
        );

        let catalog = b.build();

        // Run startup jobs to register predefined accounts
        init_on_startup::run_startup_jobs_ex(
            &catalog,
            init_on_startup::RunStartupJobsOptions {
                job_selector: Some(init_on_startup::JobSelector::AllOf(HashSet::from([
                    JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
                ]))),
            },
        )
        .await
        .unwrap();

        catalog
    }

    pub fn base_catalog(&self) -> &dill::Catalog {
        &self.base_catalog
    }

    pub async fn flush_outbox(&self) {
        self.outbox_agent
            .as_ref()
            .expect("flush_outbox requires Dispatching outbox mode")
            .run_while_has_tasks()
            .await
            .unwrap();
    }

    fn catalog_for_account(&self, account: TestAccount) -> dill::Catalog {
        let account_id = self.account_id(account);
        let subject = CurrentAccountSubject::logged(
            account_id,
            odf::AccountName::new_unchecked(account.name()),
        );

        dill::CatalogBuilder::new_chained(&self.base_catalog)
            .add_value(subject)
            .build()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FacadeContractHarness for LocalFacadeHarness {
    fn facade_for(&self, account: TestAccount) -> Arc<dyn ResourceFacade> {
        let catalog = self.catalog_for_account(account);
        let mut b = dill::CatalogBuilder::new_chained(&catalog);
        kamu_resources_facade::register_dependencies(&mut b);
        let catalog = b.build();
        catalog.get_one::<dyn ResourceFacade>().unwrap()
    }

    fn account_id(&self, account: TestAccount) -> odf::AccountID {
        match account {
            TestAccount::Alice => self.alice_account_id.clone(),
            TestAccount::Bob => self.bob_account_id.clone(),
        }
    }

    fn account_name(&self, account: TestAccount) -> odf::AccountName {
        odf::AccountName::new_unchecked(account.name())
    }

    async fn flush_outbox(&self) {
        LocalFacadeHarness::flush_outbox(self).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
