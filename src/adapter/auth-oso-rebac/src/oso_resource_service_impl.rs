// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use database_common::{EntityPageListing, EntityPageStreamer};
use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::{AccountNotFoundByIdError, AccountService, GetAccountByIdError};
use kamu_auth_rebac::{AuthorizedAccount, RebacService};
use kamu_datasets::{
    DatasetEntriesResolution,
    DatasetEntryNotFoundError,
    DatasetEntryService,
    GetDatasetEntryError,
};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::{DatasetResource, UserActor};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type EntityId = String;

#[derive(Debug, Default)]
pub struct State {
    user_actor_cache_map: HashMap<EntityId, UserActor>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OsoResourceServiceImplStateHolder {
    pub state: RwLock<State>,
}

#[component(pub)]
#[scope(Singleton)]
impl OsoResourceServiceImplStateHolder {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(State::default()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OsoDatasetResourceServiceImpl {
    dataset_entry_svc: Arc<dyn DatasetEntryService>,
    rebac_service: Arc<dyn RebacService>,
}

#[component(pub)]
impl OsoDatasetResourceServiceImpl {
    pub fn new(
        dataset_entry_svc: Arc<dyn DatasetEntryService>,
        rebac_service: Arc<dyn RebacService>,
    ) -> Self {
        Self {
            dataset_entry_svc,
            rebac_service,
        }
    }

    pub async fn dataset_resource(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetResource, GetDatasetResourceError> {
        let dataset_entry = match self.dataset_entry_svc.get_entry(dataset_id).await {
            Ok(found_dataset_entry) => found_dataset_entry,
            Err(e) => return Err(e.into()),
        };
        let dataset_properties = self
            .rebac_service
            .get_dataset_properties(&dataset_entry.id)
            .await
            .int_err()?;
        let authorized_accounts = self
            .rebac_service
            .get_authorized_accounts(dataset_id)
            .await
            .int_err()?;

        let mut dataset_resource = DatasetResource::new(
            &dataset_entry.owner_id,
            dataset_properties.allows_public_read,
        );
        for AuthorizedAccount { account_id, role } in authorized_accounts {
            dataset_resource.authorize_account(&account_id, role);
        }

        Ok(dataset_resource)
    }

    pub async fn get_multiple_dataset_resources(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<DatasetResourcesResolution, GetMultipleDatasetResourcesError> {
        let DatasetEntriesResolution {
            resolved_entries,
            unresolved_entries,
        } = self
            .dataset_entry_svc
            .get_multiple_entries(dataset_ids)
            .await
            .int_err()?;

        let dataset_resources_stream = EntityPageStreamer::default().into_stream(
            || async { Ok(Arc::new(resolved_entries)) },
            |dataset_entries, pagination| {
                let dataset_entries_page = dataset_entries
                    .iter()
                    .skip(pagination.offset)
                    .take(pagination.safe_limit(dataset_entries.len()))
                    .collect::<Vec<_>>();

                let dataset_id_owner_id_mapping =
                    dataset_entries_page
                        .iter()
                        .fold(HashMap::new(), |mut acc, dataset_entry| {
                            acc.insert(dataset_entry.id.clone(), dataset_entry.owner_id.clone());
                            acc
                        });
                let dataset_ids = dataset_entries_page
                    .iter()
                    .map(|dataset_entry| dataset_entry.id.clone())
                    .collect::<Vec<_>>();

                async move {
                    let dataset_properties_map = self
                        .rebac_service
                        .get_dataset_properties_by_ids(&dataset_ids)
                        .await
                        .int_err()?;
                    let mut dataset_accounts_relation_map = self
                        .rebac_service
                        .get_authorized_accounts_by_ids(&dataset_ids)
                        .await
                        .int_err()?;

                    let mut dataset_resources = Vec::with_capacity(dataset_properties_map.len());

                    for (dataset_id, dataset_properties) in dataset_properties_map {
                        let owner_id =
                            dataset_id_owner_id_mapping
                                .get(&dataset_id)
                                .ok_or_else(|| {
                                    format!("Unexpectedly, owner_id not found: {dataset_id}")
                                        .int_err()
                                })?;

                        let mut dataset_resource =
                            DatasetResource::new(owner_id, dataset_properties.allows_public_read);

                        if let Some(authorized_accounts) =
                            dataset_accounts_relation_map.remove(&dataset_id)
                        {
                            for AuthorizedAccount { account_id, role } in authorized_accounts {
                                dataset_resource.authorize_account(&account_id, role);
                            }
                        }

                        dataset_resources.push((dataset_id, dataset_resource));
                    }

                    Ok(EntityPageListing {
                        list: dataset_resources,
                        total_count: dataset_entries.len(),
                    })
                }
            },
        );

        use futures::TryStreamExt;

        Ok(DatasetResourcesResolution {
            resolved_resources: dataset_resources_stream.try_collect().await?,
            unresolved_resources: unresolved_entries,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OsoAccountResourceServiceImpl {
    state_holder: Arc<OsoResourceServiceImplStateHolder>,
    rebac_service: Arc<dyn RebacService>,
    account_service: Arc<dyn AccountService>,
}

#[component(pub)]
impl OsoAccountResourceServiceImpl {
    pub fn new(
        state_holder: Arc<OsoResourceServiceImplStateHolder>,
        rebac_service: Arc<dyn RebacService>,
        account_service: Arc<dyn AccountService>,
    ) -> Self {
        Self {
            state_holder,
            rebac_service,
            account_service,
        }
    }

    pub async fn user_actor(
        &self,
        maybe_account_id: Option<&odf::AccountID>,
    ) -> Result<UserActor, GetUserActorError> {
        let Some(account_id) = maybe_account_id else {
            return Ok(UserActor::anonymous());
        };

        // First, an attempt to get from the cache
        {
            let readable_state = self.state_holder.state.read().await;

            let account_id_stack = account_id.as_did_str().to_stack_string();
            let maybe_cached_user_actor = readable_state
                .user_actor_cache_map
                .get(account_id_stack.as_str())
                .cloned();

            if let Some(cached_user_actor) = maybe_cached_user_actor {
                return Ok(cached_user_actor);
            }
        }

        // The second attempt is from the database
        let user_actor = {
            let account = match self.account_service.get_account_by_id(account_id).await {
                Ok(found_account) => found_account,
                Err(e) => return Err(e.into()),
            };

            let account_properties = self
                .rebac_service
                .get_account_properties(&account.id)
                .await
                .int_err()?;

            UserActor::logged(
                &account.id,
                account_properties.is_admin,
                account_properties.can_provision_accounts,
            )
        };

        // Lastly, caching
        let mut writable_state = self.state_holder.state.write().await;

        writable_state
            .user_actor_cache_map
            .insert(user_actor.account_id.clone(), user_actor.clone());

        Ok(user_actor)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DatasetResourcesResolution {
    pub resolved_resources: Vec<(odf::DatasetID, DatasetResource)>,
    pub unresolved_resources: Vec<odf::DatasetID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetUserActorError {
    #[error(transparent)]
    NotFound(#[from] AccountNotFoundByIdError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<GetAccountByIdError> for GetUserActorError {
    fn from(err: GetAccountByIdError) -> Self {
        match err {
            GetAccountByIdError::NotFound(e) => Self::NotFound(e),
            e @ GetAccountByIdError::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetResourceError {
    #[error(transparent)]
    NotFound(#[from] DatasetEntryNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<GetDatasetEntryError> for GetDatasetResourceError {
    fn from(err: GetDatasetEntryError) -> Self {
        match err {
            GetDatasetEntryError::NotFound(e) => Self::NotFound(e),
            GetDatasetEntryError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetMultipleDatasetResourcesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
