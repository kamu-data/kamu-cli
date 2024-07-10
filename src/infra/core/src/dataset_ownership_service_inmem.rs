// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dill::*;
use kamu_accounts::{AuthenticationService, CurrentAccountSubject};
use kamu_core::messages::{DatasetCreatedMessage, DatasetDeletedMessage};
use kamu_core::*;
use messaging_outbox::MessageConsumerT;
use opendatafabric::{AccountID, AccountName, DatasetID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetOwnershipServiceInMemory {
    state: Arc<tokio::sync::RwLock<State>>,
}

#[derive(Default)]
struct State {
    dataset_ids_by_account_id: HashMap<AccountID, HashSet<DatasetID>>,
    account_ids_by_dataset_id: HashMap<DatasetID, Vec<AccountID>>,
    initially_scanned: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetOwnershipService)]
#[interface(dyn MessageConsumerT<DatasetCreatedMessage>)]
#[interface(dyn MessageConsumerT<DatasetDeletedMessage>)]
#[scope(Singleton)]
impl DatasetOwnershipServiceInMemory {
    pub fn new() -> Self {
        Self {
            state: Default::default(),
        }
    }

    fn insert_dataset_record(
        &self,
        state: &mut State,
        dataset_id: &DatasetID,
        owner_account_id: &AccountID,
    ) {
        state
            .account_ids_by_dataset_id
            .insert(dataset_id.clone(), vec![owner_account_id.clone()]);

        state
            .dataset_ids_by_account_id
            .entry(owner_account_id.clone())
            .and_modify(|e| {
                e.insert(dataset_id.clone());
            })
            .or_insert_with(|| {
                let mut dataset_ids = HashSet::new();
                dataset_ids.insert(dataset_id.clone());
                dataset_ids
            });
    }

    async fn check_has_initialized(&self) -> Result<(), InternalError> {
        let has_initially_scanned = self.state.read().await.initially_scanned;

        if has_initially_scanned {
            Ok(())
        } else {
            InternalError::bail("The service was not previously initialized!")
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetOwnershipService for DatasetOwnershipServiceInMemory {
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id))]
    async fn get_dataset_owners(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Vec<AccountID>, InternalError> {
        self.check_has_initialized().await?;

        let guard = self.state.read().await;
        let maybe_account_ids = guard.account_ids_by_dataset_id.get(dataset_id);
        if let Some(account_ids) = maybe_account_ids {
            Ok(account_ids.clone())
        } else {
            Ok(vec![])
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%account_id))]
    async fn get_owned_datasets(
        &self,
        account_id: &AccountID,
    ) -> Result<Vec<DatasetID>, InternalError> {
        self.check_has_initialized().await?;

        let guard = self.state.read().await;
        let maybe_dataset_ids = guard.dataset_ids_by_account_id.get(account_id);
        if let Some(dataset_ids) = maybe_dataset_ids {
            Ok(dataset_ids.iter().cloned().collect::<Vec<_>>())
        } else {
            Ok(vec![])
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id, %account_id))]
    async fn is_dataset_owned_by(
        &self,
        dataset_id: &DatasetID,
        account_id: &AccountID,
    ) -> Result<bool, InternalError> {
        self.check_has_initialized().await?;

        let guard = self.state.read().await;

        let maybe_account_ids = guard.account_ids_by_dataset_id.get(dataset_id);
        if let Some(account_ids) = maybe_account_ids {
            Ok(account_ids.contains(account_id))
        } else {
            Ok(false)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetCreatedMessage> for DatasetOwnershipServiceInMemory {
    #[tracing::instrument(level = "debug", skip_all, fields(?message))]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: DatasetCreatedMessage,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.write().await;
        self.insert_dataset_record(&mut guard, &message.dataset_id, &message.owner_account_id);
        Ok(())
    }

    fn consumer_name(&self) -> &'static str {
        MESSAGE_CONSUMER_KAMU_CORE_DATASET_OWNERSHIP_SERVICE
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetDeletedMessage> for DatasetOwnershipServiceInMemory {
    #[tracing::instrument(level = "debug", skip_all, fields(?message))]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: DatasetDeletedMessage,
    ) -> Result<(), InternalError> {
        let account_ids = self.get_dataset_owners(&message.dataset_id).await?;
        if !account_ids.is_empty() {
            let mut guard = self.state.write().await;
            for account_id in account_ids {
                if let Some(dataset_ids) = guard.dataset_ids_by_account_id.get_mut(&account_id) {
                    dataset_ids.remove(&message.dataset_id);
                }
            }
            guard.account_ids_by_dataset_id.remove(&message.dataset_id);
        }
        Ok(())
    }

    fn consumer_name(&self) -> &'static str {
        MESSAGE_CONSUMER_KAMU_CORE_DATASET_OWNERSHIP_SERVICE
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Initializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetOwnershipServiceInMemoryStateInitializer {
    current_account_subject: Arc<CurrentAccountSubject>,
    dataset_repo: Arc<dyn DatasetRepository>,
    authentication_service: Arc<dyn AuthenticationService>,
    dataset_ownership_service: Arc<DatasetOwnershipServiceInMemory>,
}

#[component(pub)]
impl DatasetOwnershipServiceInMemoryStateInitializer {
    pub fn new(
        current_account_subject: Arc<CurrentAccountSubject>,
        dataset_repo: Arc<dyn DatasetRepository>,
        authentication_service: Arc<dyn AuthenticationService>,
        dataset_ownership_service: Arc<DatasetOwnershipServiceInMemory>,
    ) -> Self {
        Self {
            current_account_subject,
            dataset_repo,
            authentication_service,
            dataset_ownership_service,
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn eager_initialization(&self) -> Result<(), InternalError> {
        let mut guard = self.dataset_ownership_service.state.write().await;
        if guard.initially_scanned {
            tracing::warn!("The service has already initialized");

            return Ok(());
        }

        use futures::StreamExt;

        tracing::debug!("Initializing dataset ownership data started");

        let mut account_ids_by_name: HashMap<AccountName, AccountID> = HashMap::new();

        let mut datasets_stream = self.dataset_repo.get_all_datasets();
        while let Some(Ok(dataset_handle)) = datasets_stream.next().await {
            let account_name = match dataset_handle.alias.account_name {
                Some(account_name) => account_name,
                None => match self.current_account_subject.as_ref() {
                    CurrentAccountSubject::Anonymous(_) => {
                        panic!("Initializing dataset ownership without authorization")
                    }
                    CurrentAccountSubject::Logged(l) => l.account_name.clone(),
                },
            };

            let maybe_account_id = if let Some(account_id) = account_ids_by_name.get(&account_name)
            {
                Some(account_id.clone())
            } else {
                let maybe_account_id = self
                    .authentication_service
                    .find_account_id_by_name(&account_name)
                    .await?;
                if let Some(account_id) = maybe_account_id {
                    account_ids_by_name.insert(account_name.clone(), account_id.clone());
                    Some(account_id)
                } else {
                    None
                }
            };

            if let Some(account_id) = maybe_account_id {
                self.dataset_ownership_service.insert_dataset_record(
                    &mut guard,
                    &dataset_handle.id,
                    &account_id,
                );
            }
        }

        guard.initially_scanned = true;

        tracing::debug!("Finished initializing dataset ownership data",);

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
